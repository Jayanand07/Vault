package com.vault.node.controller;

import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.ElectionService;
import com.vault.node.model.Replica;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.service.LocalStorageService;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import jakarta.servlet.http.HttpServletRequest;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

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

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @PostMapping("/heartbeat")
    public ResponseEntity<?> receiveHeartbeat(
            @RequestParam long term,
            @RequestParam java.util.UUID leaderId,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!nodeSecret.equals(secret)) {
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

        if (!nodeSecret.equals(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // Fencing: Reject writes from old leaders
        if (term < nodeState.getCurrentTerm()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Stale term");
        }

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("vault-internal-replicate-", ".tmp");
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            try (InputStream is = request.getInputStream();
                 java.security.DigestInputStream dis = new java.security.DigestInputStream(is, md)) {
                Files.copy(dis, tempFile, StandardCopyOption.REPLACE_EXISTING);
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

    @GetMapping("/objects/{objectId}")
    public ResponseEntity<StreamingResponseBody> getInternalObject(
            @PathVariable String objectId,
            @RequestParam long version,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!nodeSecret.equals(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            // Find replica in DB to get checksum
            return replicaRepository.findById(new com.vault.node.model.ReplicaId(objectId, version, nodeRegistryService.getSelfNodeId()))
                    .map(replica -> {
                        try {
                            InputStream is = localStorageService.read(objectId, version, replica.getChecksumSha256());
                            if (is != null) {
                                StreamingResponseBody responseBody = outputStream -> {
                                    try (is) {
                                        byte[] buffer = new byte[8192];
                                        int bytesRead;
                                        while ((bytesRead = is.read(buffer)) != -1) {
                                            outputStream.write(buffer, 0, bytesRead);
                                        }
                                    }
                                };
                                return ResponseEntity.ok(responseBody);
                            }
                        } catch (Exception e) {
                            log.error("Failed to read internal object", e);
                        }
                        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).<StreamingResponseBody>build();
                    })
                    .orElseGet(() -> ResponseEntity.notFound().build());
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @DeleteMapping("/replicate/{objectId}")
    public ResponseEntity<?> replicateTombstone(
            @PathVariable String objectId,
            @RequestParam long version,
            @RequestParam long term,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!nodeSecret.equals(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // Fencing: Reject writes from old leaders
        if (term < nodeState.getCurrentTerm()) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body("Stale term");
        }

        try {
            localStorageService.delete(objectId, version);

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
    @DeleteMapping("/gc/{objectId}")
    public ResponseEntity<?> garbageCollectReplica(
            @PathVariable String objectId,
            @RequestParam long version,
            @RequestHeader("X-Vault-Node-Secret") String secret) {

        if (!nodeSecret.equals(secret)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        try {
            localStorageService.delete(objectId, version);
            
            com.vault.node.model.ReplicaId repId = new com.vault.node.model.ReplicaId(objectId, version, nodeRegistryService.getSelfNodeId());
            replicaRepository.deleteById(repId);
            
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            log.error("Internal GC deletion failed for {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
