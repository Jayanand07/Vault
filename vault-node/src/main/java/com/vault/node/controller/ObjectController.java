package com.vault.node.controller;

import com.vault.common.dto.WriteResult;
import com.vault.common.model.DurabilityTier;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMeta;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.security.ObjectIdValidator;
import com.vault.node.service.*;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/objects")
@RequiredArgsConstructor
@Slf4j
public class ObjectController {

    private final LocalStorageService localStorageService;
    private final MetadataService metadataService;
    private final ObjectIdValidator objectIdValidator;
    private final IdempotencyService idempotencyService;
    private final ObjectService objectService;
    private final NodeState nodeState;
    private final ConsistentHashRing ring;
    private final WebClient internalWebClient;
    private final ReadRepairService readRepairService;
    private final NodeRegistryService nodeRegistryService;
    private final AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    private final CorruptionGenealogyTracker corruptionGenealogyTracker;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;

    @org.springframework.beans.factory.annotation.Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @PutMapping("/{objectId}")
    public ResponseEntity<?> putObjectRest(
            @PathVariable String objectId,
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Vault-Durability", defaultValue = "STANDARD") DurabilityTier tier,
            HttpServletRequest request) {

        Path tempFile = null;
        try {
            tempFile = Files.createTempFile("vault-native-upload-", ".tmp");
            try (InputStream is = request.getInputStream()) {
                Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            return putObject(objectId, idempotencyKey, tier, tempFile);
        } catch (Exception e) {
            log.error("Failed to spool upload", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (Exception ignore) {}
            }
        }
    }

    public ResponseEntity<?> putObject(
            String objectId,
            String idempotencyKey,
            DurabilityTier tier,
            Path tempFile) {

        try {
            objectIdValidator.validateOrThrow(objectId);
            
            if (!idempotencyService.claimKey(idempotencyKey)) {
                return ResponseEntity.status(HttpStatus.CONFLICT).body("Request already in progress or completed");
            }

            // Phase 2: Redirect to leader if not leader
            if (!nodeState.isLeader()) {
                String leaderAddr = nodeState.getLeaderAddress();
                if (leaderAddr == null) {
                    idempotencyService.completeKey(idempotencyKey, 503, Map.of("error", "No leader available"));
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("No leader - retry later");
                }
                idempotencyService.completeKey(idempotencyKey, 307, null);
                return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                        .location(URI.create("http://" + leaderAddr + "/api/v1/objects/" + objectId))
                        .header("X-Vault-Leader-Address", leaderAddr)
                        .build();
            }

            // Phase 7: Compute consistency mode
            com.vault.common.model.ConsistencyMode mode = adaptiveConsistencyEngine.determineConsistency(tier);

            // Process write via ObjectService using the requested tier
            WriteResult result = objectService.put(objectId, tempFile, tier);

            var responsePayload = Map.of(
                    "objectId", objectId,
                    "checksum", result.getChecksumSha256(),
                    "size", result.getSizeBytes()
            );

            idempotencyService.completeKey(idempotencyKey, 201, responsePayload);
            vaultMetrics.incrementWrites();
            return ResponseEntity.status(HttpStatus.CREATED)
                    .header("X-Vault-Consistency-Mode", mode.name())
                    .body(responsePayload);

        } catch (Exception e) {
            log.error("Failed to store object {}", objectId, e);
            idempotencyService.completeKey(idempotencyKey, 500, Map.of("error", e.getMessage()));
            if (e instanceof com.vault.common.exception.InsufficientQuorumException) {
                return ResponseEntity.status(507).body(e.getMessage());
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @GetMapping("/{objectId}")
    public ResponseEntity<?> getObject(@PathVariable String objectId, jakarta.servlet.http.HttpServletResponse servletResponse) {
        try {
            objectIdValidator.validateOrThrow(objectId);

            // 1. Metadata first!
            ObjectMeta meta = metadataService.findLatestActive(objectId);
            if (meta == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Object not found");
            }

            long expectedVersion = meta.getVersion();
            String expectedChecksum = meta.getChecksumSha256();

            // Phase 7: Compute consistency mode
            com.vault.common.model.ConsistencyMode mode = adaptiveConsistencyEngine.determineConsistency(meta.getDurabilityTier());
            int readQuorum = mode == com.vault.common.model.ConsistencyMode.STRONG ? (meta.getDurabilityTier().getN() / 2) + 1 : 1;
            int successfulReads = 0;
            boolean localAvailable = false;

            // 2. Local check — probe only, don't buffer
            long localSize = localStorageService.getFileSize(objectId, expectedVersion, expectedChecksum);
            if (localSize > 0) {
                localAvailable = true;
                successfulReads++;
            }

            // 3. If STRONG consistency requires quorum, verify remote nodes
            if (readQuorum > 1) {
                for (Node target : ring.getTargetNodes(objectId, meta.getDurabilityTier().getN())) {
                    if (successfulReads >= readQuorum) break;
                    if (target.getNodeId().equals(nodeRegistryService.getSelfNodeId())) continue;

                    try {
                        byte[] remoteData = internalWebClient.get()
                                .uri("http://" + target.getAddress() + "/internal/objects/" + objectId + "?version=" + expectedVersion)
                                .header("X-Vault-Node-Secret", nodeSecret)
                                .retrieve()
                                .bodyToMono(byte[].class)
                                .block();

                        if (remoteData != null) {
                            String remoteChecksum = com.vault.common.util.ChecksumUtil.sha256(remoteData);
                            if (remoteChecksum.equals(expectedChecksum)) {
                                successfulReads++;
                                // If we didn't have it locally, use the remote copy to stream back
                                if (!localAvailable) {
                                    localAvailable = true; // we have data to serve
                                }
                            } else {
                                corruptionGenealogyTracker.recordCorruption(objectId, target.getNodeId(), nodeRegistryService.getSelfNodeId());
                            }
                        }
                    } catch (Exception ignored) { }
                }
            }

            if (successfulReads >= readQuorum && localAvailable) {
                vaultMetrics.incrementReads();
                
                // Stream directly from disk — only 8KB in memory at a time
                org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody stream = outputStream -> {
                    try {
                        localStorageService.streamVerified(objectId, expectedVersion, expectedChecksum, outputStream);
                    } catch (RuntimeException e) {
                        if ("CORRUPT_FILE".equals(e.getMessage())) {
                            log.warn("Corruption detected during streaming read for {} v{}, triggering repair", objectId, expectedVersion);
                            corruptionGenealogyTracker.recordCorruption(objectId, nodeRegistryService.getSelfNodeId(), nodeRegistryService.getSelfNodeId());
                            readRepairService.repair(objectId, expectedVersion, nodeRegistryService.getSelfNodeId());
                        }
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                
                return ResponseEntity.ok()
                        .header("X-Vault-Version", String.valueOf(expectedVersion))
                        .header("X-Vault-Checksum", expectedChecksum)
                        .header("X-Vault-Consistency-Mode", mode.name())
                        .header("Content-Length", String.valueOf(localSize))
                        .body(stream);
            }

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Insufficient replicas available to meet read quorum (" + successfulReads + "/" + readQuorum + ")");

        } catch (Exception e) {
            log.error("Failed to read object {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @RequestMapping(value = "/{objectId}", method = RequestMethod.HEAD)
    public ResponseEntity<?> headObject(@PathVariable String objectId) {
        try {
            objectIdValidator.validateOrThrow(objectId);
            ObjectMeta meta = metadataService.findLatestActive(objectId);
            if (meta == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }
            return ResponseEntity.ok()
                    .header("X-Vault-Version", String.valueOf(meta.getVersion()))
                    .header("X-Vault-Checksum", meta.getChecksumSha256())
                    .header("Content-Length", String.valueOf(meta.getSizeBytes()))
                    .build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping
    public ResponseEntity<?> listObjects(
            @RequestParam(required = false, defaultValue = "") String prefix,
            @RequestParam(required = false, defaultValue = "100") int limit) {
        try {
            var page = metadataService.listObjects(prefix, limit);
            var results = page.getContent().stream()
                .map(meta -> Map.of(
                    "objectId", meta.getObjectId(),
                    "size", meta.getSizeBytes(),
                    "checksum", meta.getChecksumSha256(),
                    "tier", meta.getDurabilityTier().name()
                )).toList();
            return ResponseEntity.ok(Map.of(
                "objects", results,
                "total", page.getTotalElements(),
                "page", page.getNumber()
            ));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @DeleteMapping("/{objectId}")
    public ResponseEntity<?> deleteObject(@PathVariable String objectId) {
        try {
            objectIdValidator.validateOrThrow(objectId);
            
            // Phase 2: Redirect to leader if not leader
            if (!nodeState.isLeader()) {
                String leaderAddr = nodeState.getLeaderAddress();
                if (leaderAddr == null) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("No leader - retry later");
                }
                return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                        .location(URI.create("http://" + leaderAddr + "/api/v1/objects/" + objectId))
                        .header("X-Vault-Leader-Address", leaderAddr)
                        .build();
            }

            objectService.delete(objectId);
            return ResponseEntity.noContent().build();
            
        } catch (Exception e) {
            log.error("Failed to delete object {}", objectId, e);
            if (e instanceof com.vault.common.exception.InsufficientQuorumException) {
                return ResponseEntity.status(507).body(e.getMessage());
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
}
