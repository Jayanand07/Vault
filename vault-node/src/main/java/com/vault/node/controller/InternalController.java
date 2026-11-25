package com.vault.node.controller;

import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.ElectionService;
import com.vault.node.model.Replica;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.ObjectIdValidator;
import com.vault.node.service.LocalStorageService;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import jakarta.servlet.http.HttpServletRequest;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

@RestController
@RequestMapping("/internal")
@RequiredArgsConstructor
@Slf4j
public class InternalController {

    private final LocalStorageService localStorageService;
    private final ReplicaRepository replicaRepository;
    private final NodeRegistryService nodeRegistryService;
    private final ElectionService electionService;
    private final com.vault.node.election.NodeState nodeState;
    private final ObjectIdValidator objectIdValidator;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Value("${vault.storage.max-object-size-bytes:134217728}")
    private long maxObjectSizeBytes = 134217728L;

    private boolean isAuthorized(String secret) {
        if (secret == null || nodeSecret == null) return false;
        return MessageDigest.isEqual(
                nodeSecret.getBytes(StandardCharsets.UTF_8),
                secret.getBytes(StandardCharsets.UTF_8)
        );
    }

    @PostMapping("/heartbeat")
    public ResponseEntity<?> receiveHeartbeat(
            @RequestParam long term,
            @RequestParam java.util.UUID leaderId,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!isAuthorized(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        electionService.recordHeartbeat(term, leaderId);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/replicate")
    public ResponseEntity<?> replicate(
            @RequestHeader("X-Vault-Node-Secret") String secret,
            @RequestHeader("X-Vault-Checksum") String expectedChecksum,
            @RequestHeader("X-Vault-Version") long version,
            @RequestHeader("X-Vault-Term") long term,
            @RequestHeader("X-Vault-Object-Id") String objectId,
            HttpServletRequest request) {

        if (!isAuthorized(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            objectIdValidator.validateOrThrow(objectId);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        // Fencing: Reject writes from old leaders
        if (term < nodeState.getCurrentTerm()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Stale term");
        }

        long declaredLength = request.getContentLengthLong();
        if (declaredLength > maxObjectSizeBytes) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body("Payload exceeds size limit");
        }

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("vault-internal-replicate-", ".tmp");
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            long totalBytes = 0L;
            byte[] buffer = new byte[8192];
            try (InputStream is = request.getInputStream();
                 java.security.DigestInputStream dis = new java.security.DigestInputStream(is, md);
                 var os = Files.newOutputStream(tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                int read;
                while ((read = dis.read(buffer)) != -1) {
                    totalBytes += read;
                    if (totalBytes > maxObjectSizeBytes) {
                        return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body("Payload exceeds size limit");
                    }
                    os.write(buffer, 0, read);
                }
                os.flush();
            }

            // Verify checksum
            byte[] digest = md.digest();
            String actualChecksum = com.google.common.io.BaseEncoding.base16().lowerCase().encode(digest);
            if (!actualChecksum.equals(expectedChecksum)) {
                log.error("Checksum mismatch for {}. Expected: {}, Actual: {}", objectId, expectedChecksum, actualChecksum);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Checksum mismatch");
            }

            // Store locally
            try (InputStream fileIs = Files.newInputStream(tempFile)) {
                localStorageService.store(objectId, version, fileIs);
            }

            // Record replica in DB
            Replica replica = new Replica();
            replica.setObjectId(objectId);
            replica.setVersion(version);
            replica.setNodeId(nodeRegistryService.getSelfNodeId());
            replica.setChecksumSha256(actualChecksum);
            replicaRepository.save(replica);

            return ResponseEntity.ok().build();

        } catch (Exception e) {
            log.error("Internal replication failed for {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (Exception ignore) {}
            }
        }
    }

    @GetMapping("/objects")
    public ResponseEntity<InputStreamResource> getInternalObject(
            @RequestParam("key") String objectId,
            @RequestParam long version,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!isAuthorized(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            objectIdValidator.validateOrThrow(objectId);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        }

        try {
            return replicaRepository.findById(new com.vault.node.model.ReplicaId(objectId, version, nodeRegistryService.getSelfNodeId()))
                    .map(replica -> {
                        try {
                            InputStream verified = localStorageService.openVerifiedStream(
                                    objectId, version, replica.getChecksumSha256(), -1L);
                            if (verified == null) {
                                return ResponseEntity.notFound().<InputStreamResource>build();
                            }
                            return ResponseEntity.ok(new InputStreamResource(verified));
                        } catch (LocalStorageService.CorruptReplicaException e) {
                            log.error("Refusing to serve corrupt internal replica {} v{}", objectId, version);
                            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).<InputStreamResource>build();
                        } catch (Exception e) {
                            log.error("Failed to verify internal object {} v{}", objectId, version, e);
                            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).<InputStreamResource>build();
                        }
                    })
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DeleteMapping("/replicate")
    public ResponseEntity<?> replicateTombstone(
            @RequestParam("key") String objectId,
            @RequestParam long version,
            @RequestParam long term,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!isAuthorized(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            objectIdValidator.validateOrThrow(objectId);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        // Fencing: Reject writes from old leaders
        if (term < nodeState.getCurrentTerm()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Stale term");
        }

        try {
            localStorageService.writeTombstoneMarker(objectId, version);

            // Record tombstone replica
            Replica replica = new Replica();
            replica.setObjectId(objectId);
            replica.setVersion(version);
            replica.setNodeId(nodeRegistryService.getSelfNodeId());
            replica.setChecksumSha256("TOMBSTONE");
            replicaRepository.save(replica);

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Internal tombstone replication failed for {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DeleteMapping("/gc")
    public ResponseEntity<?> garbageCollectReplica(
            @RequestParam("key") String objectId,
            @RequestParam long version,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!isAuthorized(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            objectIdValidator.validateOrThrow(objectId);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(e.getMessage());
        }

        try {
            // Invariant: clear the replica DB row BEFORE deleting the physical file
            com.vault.node.model.ReplicaId repId = new com.vault.node.model.ReplicaId(objectId, version, nodeRegistryService.getSelfNodeId());
            replicaRepository.deleteById(repId);
            localStorageService.deleteVersionFiles(objectId, version);

            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Internal GC deletion failed for {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
