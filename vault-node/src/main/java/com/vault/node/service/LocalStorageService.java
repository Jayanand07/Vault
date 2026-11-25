package com.vault.node.service;

import com.vault.common.dto.WriteResult;
import com.vault.common.util.ObjectKey;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

@Service
@Slf4j
public class LocalStorageService {

    @Value("${vault.node.data-dir}")
    private String dataDirStr;

    private Path objectsDir;
    private Path tmpDir;
    private Path tombstonesDir;

    @PostConstruct
    public void init() throws IOException {
        Path baseDir = Paths.get(dataDirStr);
        objectsDir = baseDir.resolve("objects");
        tmpDir = baseDir.resolve("tmp");
        tombstonesDir = baseDir.resolve("tombstones");

        Files.createDirectories(objectsDir);
        Files.createDirectories(tmpDir);
        Files.createDirectories(tombstonesDir);

        cleanTmpDir();
    }

    public void cleanTmpDir() {
        try {
            Files.list(tmpDir).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    log.warn("Failed to delete stale tmp file: {}", path);
                }
            });
            log.info("Cleaned temporary directory on startup.");
        } catch (IOException e) {
            log.error("Failed to clean tmp directory", e);
        }
    }

    public WriteResult store(String objectId, long version, InputStream data) throws IOException, NoSuchAlgorithmException {
        String keyHash = ObjectKey.keyHash(objectId);
        Path tempPath = tmpDir.resolve(keyHash + ".v" + version + "." + UUID.randomUUID() + ".partial");

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long sizeBytes = 0;

        // 1. Write to temp file, compute SHA-256 in one pass, and fsync before publish
        try {
            try (DigestInputStream dis = new DigestInputStream(data, digest);
                 FileChannel channel = FileChannel.open(tempPath,
                         StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = dis.read(buffer)) != -1) {
                    ByteBuffer bb = ByteBuffer.wrap(buffer, 0, bytesRead);
                    while (bb.hasRemaining()) {
                        channel.write(bb);
                    }
                    sizeBytes += bytesRead;
                }
                channel.force(true);
            }
        } catch (Exception e) {
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException ignored) {}
            throw e;
        }

        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        String checksum = sb.toString();

        // 2. Determine collision-free final path sharded by SHA-256(objectId)
        Path finalPath = getFilePath(objectId, version);
        Files.createDirectories(finalPath.getParent());

        // 3. Atomic rename (StandardCopyOption.ATOMIC_MOVE)
        try {
            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
        }

        log.info("Stored file locally: {} (v{}), path: {}, size: {} bytes, sha256: {}",
                objectId, version, finalPath.getFileName(), sizeBytes, checksum);
        return new WriteResult(checksum, sizeBytes);
    }

    public Path getFilePath(String objectId, long version) {
        String keyHash = ObjectKey.keyHash(objectId);
        return objectsDir.resolve(keyHash.substring(0, 2))
                .resolve(keyHash.substring(2, 4))
                .resolve(keyHash + ".v" + version + ".dat");
    }

    public Path getFilePath(String objectId, long version, String checksum) {
        return getFilePath(objectId, version);
    }

    public String getRelativePath(String objectId, long version) {
        return ObjectKey.relativeStoragePath(objectId, version);
    }

    public InputStream read(String objectId, long version, String checksum) throws IOException {
        Path path = getFilePath(objectId, version, checksum);
        if (!Files.exists(path)) {
            return null;
        }
        return Files.newInputStream(path);
    }
    
    public byte[] readFullyVerified(String objectId, long version, String expectedChecksum) throws IOException, NoSuchAlgorithmException {
        Path path = getFilePath(objectId, version, expectedChecksum);
        if (!Files.exists(path)) {
            return null;
        }
        
        byte[] data = Files.readAllBytes(path);
        String actualChecksum = com.vault.common.util.ChecksumUtil.sha256(data);
        
        if (!actualChecksum.equals(expectedChecksum)) {
            log.error("CORRUPTION DETECTED on read! Object {} v{}. Expected: {}, Actual: {}", objectId, version, expectedChecksum, actualChecksum);
            throw new RuntimeException("CORRUPT_FILE");
        }
        
        return data;
    }
    
    /**
     * Opens a verified stream for the replica: validates existence, expected size (when >= 0),
     * and full-file SHA-256 checksum BEFORE returning any bytes, then rewinds the pinned channel.
     *
     * @return verified {@link InputStream} backed by a pinned channel at offset 0, or {@code null} if missing
     * @throws CorruptReplicaException if size or SHA-256 mismatches
     */
    public InputStream openVerifiedStream(
            String objectId,
            long version,
            String expectedChecksum,
            long expectedSizeBytes) throws IOException, NoSuchAlgorithmException {
        Path path = getFilePath(objectId, version);
        if (!Files.exists(path)) {
            return null;
        }

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long actualSize = channel.size();
            if (expectedSizeBytes >= 0 && actualSize != expectedSizeBytes) {
                log.error("SIZE MISMATCH on read! Object {} v{}. Expected: {}, Actual: {}",
                        objectId, version, expectedSizeBytes, actualSize);
                throw new CorruptReplicaException("CORRUPT_FILE");
            }

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            ByteBuffer buffer = ByteBuffer.allocate(8192);
            while (channel.read(buffer) != -1) {
                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            StringBuilder sb = new StringBuilder(64);
            for (byte b : digest.digest()) {
                sb.append(String.format("%02x", b));
            }
            String actualChecksum = sb.toString();

            if (!actualChecksum.equals(expectedChecksum)) {
                log.error("CORRUPTION DETECTED before serving! Object {} v{}. Expected: {}, Actual: {}",
                        objectId, version, expectedChecksum, actualChecksum);
                throw new CorruptReplicaException("CORRUPT_FILE");
            }

            channel.position(0);
            return Channels.newInputStream(channel);
        } catch (Throwable t) {
            try {
                channel.close();
            } catch (IOException ignored) {}
            throw t;
        }
    }

    /**
     * Atomically installs a staged, pre-verified temp file into the final object path.
     */
    public void installVerifiedTempFile(String objectId, long version, Path verifiedTempFile) throws IOException {
        Path finalPath = getFilePath(objectId, version);
        Files.createDirectories(finalPath.getParent());
        try {
            Files.move(verifiedTempFile, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(verifiedTempFile, finalPath, StandardCopyOption.REPLACE_EXISTING);
        }
        log.info("Installed pre-verified file for {} (v{}) at {}", objectId, version, finalPath.getFileName());
    }

    /**
     * Pre-verifies full SHA-256 before writing any bytes to the destination stream.
     */
    public void streamVerified(String objectId, long version, String expectedChecksum, java.io.OutputStream out) throws IOException, NoSuchAlgorithmException {
        try (InputStream verified = openVerifiedStream(objectId, version, expectedChecksum, -1L)) {
            if (verified == null) {
                throw new java.io.FileNotFoundException("File not found: " + getFilePath(objectId, version));
            }
            verified.transferTo(out);
        }
    }

    public static class CorruptReplicaException extends RuntimeException {
        public CorruptReplicaException(String message) {
            super(message);
        }
    }
    
    public long getFileSize(String objectId, long version, String checksum) {
        Path path = getFilePath(objectId, version, checksum);
        try {
            return Files.exists(path) ? Files.size(path) : -1;
        } catch (IOException e) {
            return -1;
        }
    }

    public java.util.List<Path> listAllDataFiles() {
        try (java.util.stream.Stream<Path> stream = Files.walk(objectsDir)) {
            return stream.filter(Files::isRegularFile)
                         .filter(p -> p.toString().endsWith(".dat"))
                         .toList();
        } catch (IOException e) {
            log.error("Failed to list data files", e);
            return java.util.Collections.emptyList();
        }
    }
    
    /**
     * Deletes both the version payload (.dat) and any tombstone marker for {@code (objectId, version)}.
     */
    public void deleteVersionFiles(String objectId, long version) {
        try {
            Files.deleteIfExists(getFilePath(objectId, version));
            Files.deleteIfExists(tombstonesDir.resolve(ObjectKey.tombstoneFileName(objectId, version)));
            log.info("Deleted local version files for {} (v{})", objectId, version);
        } catch (Exception e) {
            log.error("Failed to delete local version files for {} v{}", objectId, version, e);
        }
    }

    /**
     * Writes the local tombstone marker for {@code (objectId, version)} without removing prior version files.
     */
    public void writeTombstoneMarker(String objectId, long version) {
        try {
            Path marker = tombstonesDir.resolve(ObjectKey.tombstoneFileName(objectId, version));
            Files.writeString(marker, "TOMBSTONE");
            log.info("Wrote tombstone marker for {} (v{})", objectId, version);
        } catch (Exception e) {
            log.error("Failed to write tombstone marker for {} v{}", objectId, version, e);
        }
    }

    public void delete(String objectId, long version) {
        deleteVersionFiles(objectId, version);
        writeTombstoneMarker(objectId, version);
    }
}
