package com.vault.node.service;

import com.vault.node.model.Node;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

/**
 * Shared streaming peer object fetcher that bypasses WebClient's 256 KB
 * in-memory buffer limit by streaming chunks directly into a staged temp file
 * and validating SHA-256 and expected size before exposing the bytes.
 */
@Component
@Slf4j
public class PeerObjectClient {

    public enum FetchStatus {
        VERIFIED,
        CORRUPT,
        UNREACHABLE_OR_ERROR
    }

    public record FetchResult(FetchStatus status, Path tempFile, long byteCount) {}

    private final WebClient internalWebClient;
    private final String nodeSecret;
    private final long maxObjectSizeBytes;

    public PeerObjectClient(
            WebClient internalWebClient,
            @Value("${vault.cluster.node-secret}") String nodeSecret,
            @Value("${vault.storage.max-object-size-bytes:134217728}") long maxObjectSizeBytes) {
        this.internalWebClient = internalWebClient;
        this.nodeSecret = nodeSecret;
        this.maxObjectSizeBytes = maxObjectSizeBytes;
    }

    /**
     * Streams an object replica from {@code sourceNode} into a temp file and verifies SHA-256 and size.
     * Caller owns deleting {@code result.tempFile()} when {@code status == VERIFIED} (e.g., via
     * {@link #openDeleteOnCloseStream(Path)}). On non-verified outcomes, no temp file is left behind.
     */
    public FetchResult fetchVerifiedToTempFile(
            Node sourceNode,
            String objectId,
            long version,
            String expectedChecksum,
            long expectedSizeBytes) {
        return fetchAndVerify(sourceNode, objectId, version, expectedChecksum, expectedSizeBytes, true);
    }

    /**
     * Verifies a peer's copy checksum without keeping a temp file afterwards (used for STRONG read quorum checks).
     */
    public FetchStatus verifyRemoteChecksum(
            Node sourceNode,
            String objectId,
            long version,
            String expectedChecksum,
            long expectedSizeBytes) {
        return fetchAndVerify(sourceNode, objectId, version, expectedChecksum, expectedSizeBytes, false).status();
    }

    private FetchResult fetchAndVerify(
            Node sourceNode,
            String objectId,
            long version,
            String expectedChecksum,
            long expectedSizeBytes,
            boolean keepTempFile) {
        if (sourceNode == null || sourceNode.getAddress() == null || sourceNode.getAddress().isBlank()) {
            return new FetchResult(FetchStatus.UNREACHABLE_OR_ERROR, null, -1L);
        }

        URI uri = UriComponentsBuilder.fromHttpUrl("http://" + sourceNode.getAddress() + "/internal/objects")
                .queryParam("key", objectId)
                .queryParam("version", version)
                .build()
                .encode()
                .toUri();

        Path stagedFile = null;
        try {
            if (keepTempFile) {
                stagedFile = Files.createTempFile("vault-peer-fetch-", ".tmp");
            }

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            long totalBytes = 0L;

            Flux<DataBuffer> bodyFlux = internalWebClient.get()
                    .uri(uri)
                    .header("X-Vault-Node-Secret", nodeSecret)
                    .exchangeToFlux(response -> {
                        if (!response.statusCode().is2xxSuccessful()) {
                            return response.releaseBody()
                                    .thenMany(Flux.error(new IllegalStateException(
                                            "Peer returned HTTP " + response.statusCode())));
                        }
                        return response.bodyToFlux(DataBuffer.class);
                    });

            try (FileChannel channel = keepTempFile
                    ? FileChannel.open(stagedFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
                    : null) {
                for (DataBuffer buffer : bodyFlux.toIterable(1)) {
                    try {
                        int count = buffer.readableByteCount();
                        if (count > 0) {
                            totalBytes += count;
                            if (totalBytes > maxObjectSizeBytes) {
                                throw new IllegalStateException(
                                        "Peer response exceeds max object size limit (" + maxObjectSizeBytes + " bytes)");
                            }
                            byte[] chunk = new byte[count];
                            buffer.read(chunk);
                            md.update(chunk);
                            if (channel != null) {
                                ByteBuffer bb = ByteBuffer.wrap(chunk);
                                while (bb.hasRemaining()) {
                                    channel.write(bb);
                                }
                            }
                        }
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                }
                if (channel != null) {
                    channel.force(true);
                }
            }

            if (expectedSizeBytes >= 0 && totalBytes != expectedSizeBytes) {
                log.error("Peer {} size mismatch for {} v{}: expected {}, got {}",
                        sourceNode.getAddress(), objectId, version, expectedSizeBytes, totalBytes);
                deleteQuietly(stagedFile);
                return new FetchResult(FetchStatus.CORRUPT, null, totalBytes);
            }

            StringBuilder sb = new StringBuilder(64);
            for (byte b : md.digest()) {
                sb.append(String.format("%02x", b));
            }
            String actualChecksum = sb.toString();

            if (!actualChecksum.equals(expectedChecksum)) {
                log.error("Peer {} checksum mismatch for {} v{}: expected {}, got {}",
                        sourceNode.getAddress(), objectId, version, expectedChecksum, actualChecksum);
                deleteQuietly(stagedFile);
                return new FetchResult(FetchStatus.CORRUPT, null, totalBytes);
            }

            return new FetchResult(FetchStatus.VERIFIED, stagedFile, totalBytes);
        } catch (Exception e) {
            deleteQuietly(stagedFile);
            log.warn("Failed streaming verified payload from peer {} for {} v{}: {}",
                    sourceNode.getAddress(), objectId, version, e.getMessage());
            return new FetchResult(FetchStatus.UNREACHABLE_OR_ERROR, null, -1L);
        }
    }

    /**
     * Opens an InputStream on {@code tempFile} that deletes {@code tempFile} when closed.
     */
    public InputStream openDeleteOnCloseStream(Path tempFile) throws IOException {
        InputStream raw = Files.newInputStream(tempFile);
        return new FilterInputStream(raw) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    deleteQuietly(tempFile);
                }
            }
        };
    }

    public static void deleteQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {}
        }
    }
}
