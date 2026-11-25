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

import java.nio.file.Files;
import java.nio.file.Path;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.InputStream;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

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
    private final UploadSpooler uploadSpooler;
    private final PeerObjectClient peerObjectClient;
    private final NodeState nodeState;
    private final ConsistentHashRing ring;
    private final WebClient internalWebClient;
    private final ReadRepairService readRepairService;
    private final NodeRegistryService nodeRegistryService;
    private final AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    private final CorruptionGenealogyTracker corruptionGenealogyTracker;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;
    private final com.vault.node.repository.ReplicaRepository replicaRepository;

    @org.springframework.beans.factory.annotation.Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    private String normalizePathKey(String objectPath) {
        if (objectPath == null) return "";
        return objectPath.startsWith("/") ? objectPath.substring(1) : objectPath;
    }

    @PutMapping(params = "key")
    public ResponseEntity<?> putObjectByQuery(
            @RequestParam("key") String objectId,
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Vault-Durability", defaultValue = "STANDARD") DurabilityTier tier,
            HttpServletRequest request) {
        return handlePutRaw(objectId, idempotencyKey, tier, request);
    }

    @PutMapping("/{*objectPath}")
    public ResponseEntity<?> putObjectRest(
            @PathVariable String objectPath,
            @RequestHeader(value = "X-Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "X-Vault-Durability", defaultValue = "STANDARD") DurabilityTier tier,
            HttpServletRequest request) {
        return handlePutRaw(normalizePathKey(objectPath), idempotencyKey, tier, request);
    }

    private ResponseEntity<?> handlePutRaw(
            String objectId,
            String idempotencyKey,
            DurabilityTier tier,
            HttpServletRequest request) {

        Path tempFile = null;
        try {
            objectIdValidator.validateOrThrow(objectId);
            tempFile = uploadSpooler.spoolBounded(request, "vault-native-upload-", true);
            return putObject(objectId, idempotencyKey, tier, tempFile);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (UploadSpooler.UnsupportedUploadMediaTypeException e) {
            return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).body(Map.of("error", e.getMessage()));
        } catch (UploadSpooler.PayloadTooLargeException e) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(Map.of("error", e.getMessage()));
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

        boolean idempotencyClaimed = false;
        try {
            objectIdValidator.validateOrThrow(objectId);

            // Phase 2: Redirect to leader BEFORE claiming idempotency so 307/503 never consume the key
            if (!nodeState.isLeader()) {
                String leaderAddr = nodeState.getLeaderAddress();
                if (leaderAddr == null) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("No leader - retry later");
                }
                URI redirectUri = UriComponentsBuilder.fromHttpUrl("http://" + leaderAddr + "/api/v1/objects")
                        .queryParam("key", objectId)
                        .build()
                        .encode()
                        .toUri();
                return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                        .location(redirectUri)
                        .header("X-Vault-Leader-Address", leaderAddr)
                        .build();
            }

            IdempotencyService.ClaimResult claim = idempotencyService.claimOrReplay(idempotencyKey);
            if (claim.status() == IdempotencyService.ClaimStatus.REPLAY) {
                int cachedStatus = claim.responseCode() != null ? claim.responseCode() : HttpStatus.CREATED.value();
                return ResponseEntity.status(cachedStatus)
                        .header("X-Idempotency-Replayed", "true")
                        .body(idempotencyService.deserializePayload(claim.responsePayload()));
            }
            if (claim.status() == IdempotencyService.ClaimStatus.IN_PROGRESS) {
                return ResponseEntity.status(HttpStatus.CONFLICT)
                        .body(Map.of("error", "Request already in progress"));
            }
            idempotencyClaimed = (idempotencyKey != null && !idempotencyKey.isBlank());

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

        } catch (IllegalArgumentException e) {
            if (idempotencyClaimed) {
                idempotencyService.releaseKey(idempotencyKey);
            }
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (com.vault.common.exception.StaleLeaderException e) {
            if (idempotencyClaimed) {
                idempotencyService.releaseKey(idempotencyKey);
            }
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        } catch (com.vault.common.exception.InsufficientQuorumException e) {
            if (idempotencyClaimed) {
                idempotencyService.releaseKey(idempotencyKey);
            }
            return ResponseEntity.status(507).body(e.getMessage());
        } catch (Exception e) {
            log.error("Failed to store object {}", objectId, e);
            if (idempotencyClaimed) {
                idempotencyService.releaseKey(idempotencyKey);
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to store object");
        }
    }

    @GetMapping(params = "key")
    public ResponseEntity<?> getObjectByQuery(@RequestParam("key") String objectId, jakarta.servlet.http.HttpServletResponse servletResponse) {
        return getObject(objectId, servletResponse);
    }

    @GetMapping("/inspect")
    public ResponseEntity<?> inspectObjectByQuery(@RequestParam("key") String objectId) {
        return inspectObject(objectId);
    }

    @GetMapping("/{*objectPath}")
    public ResponseEntity<?> getOrInspectByPath(@PathVariable String objectPath, jakarta.servlet.http.HttpServletResponse servletResponse) {
        String key = normalizePathKey(objectPath);
        if ("inspect".equals(key)) {
            return ResponseEntity.badRequest().body(Map.of("error", "Query parameter 'key' is required for /inspect"));
        }
        if (key.endsWith("/inspect") && key.length() > "/inspect".length()) {
            return inspectObject(key.substring(0, key.length() - "/inspect".length()));
        }
        return getObject(key, servletResponse);
    }

    public ResponseEntity<?> getObject(String objectId, jakarta.servlet.http.HttpServletResponse servletResponse) {
        InputStream localVerifiedStream = null;
        Path stagedFallbackFile = null;
        boolean responseOwnsStream = false;

        try {
            objectIdValidator.validateOrThrow(objectId);

            // 1. Metadata first!
            ObjectMeta meta = metadataService.findLatestActive(objectId);
            if (meta == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Object not found");
            }

            long expectedVersion = meta.getVersion();
            String expectedChecksum = meta.getChecksumSha256();
            long expectedSize = meta.getSizeBytes() != null ? meta.getSizeBytes() : -1L;
            UUID selfId = nodeRegistryService.getSelfNodeId();

            // Compute read quorum for this tier
            com.vault.common.model.ConsistencyMode mode = adaptiveConsistencyEngine.determineConsistency(meta.getDurabilityTier());
            int readQuorum = mode == com.vault.common.model.ConsistencyMode.STRONG ? (meta.getDurabilityTier().getN() / 2) + 1 : 1;
            int successfulReads = 0;

            // 2. Verify local replica BEFORE returning 200 or committing any bytes
            try {
                localVerifiedStream = localStorageService.openVerifiedStream(
                        objectId, expectedVersion, expectedChecksum, expectedSize);
                if (localVerifiedStream != null) {
                    successfulReads++;
                }
            } catch (LocalStorageService.CorruptReplicaException e) {
                log.warn("Corruption detected BEFORE serving {} v{}, falling back to peers and scheduling repair",
                        objectId, expectedVersion);
                corruptionGenealogyTracker.recordCorruption(objectId, selfId, selfId);
                readRepairService.repair(objectId, expectedVersion, selfId);
            }

            // 3. Peer fallback / read quorum verification:
            //    Run whenever we lack a verified local payload OR need more quorum confirmations.
            boolean needPayload = (localVerifiedStream == null);
            if (needPayload || successfulReads < readQuorum) {
                Map<UUID, Node> candidates = new LinkedHashMap<>();
                for (var replica : replicaRepository.findByObjectIdAndVersion(objectId, expectedVersion)) {
                    if (replica.getStatus() == com.vault.common.model.ReplicaStatus.HEALTHY
                            && !replica.getNodeId().equals(selfId)) {
                        nodeRegistryService.findNode(replica.getNodeId())
                                .ifPresent(n -> candidates.put(n.getNodeId(), n));
                    }
                }
                for (Node ringTarget : ring.getTargetNodes(objectId, meta.getDurabilityTier().getN())) {
                    if (!ringTarget.getNodeId().equals(selfId)) {
                        candidates.putIfAbsent(ringTarget.getNodeId(), ringTarget);
                    }
                }

                for (Node peer : candidates.values()) {
                    if (!needPayload && successfulReads >= readQuorum) {
                        break;
                    }
                    if (needPayload) {
                        PeerObjectClient.FetchResult fetch = peerObjectClient.fetchVerifiedToTempFile(
                                peer, objectId, expectedVersion, expectedChecksum, expectedSize);
                        if (fetch.status() == PeerObjectClient.FetchStatus.VERIFIED && fetch.tempFile() != null) {
                            stagedFallbackFile = fetch.tempFile();
                            needPayload = false;
                            successfulReads++;
                        } else if (fetch.status() == PeerObjectClient.FetchStatus.CORRUPT) {
                            corruptionGenealogyTracker.recordCorruption(objectId, peer.getNodeId(), selfId);
                        }
                    } else {
                        PeerObjectClient.FetchStatus verifyStatus = peerObjectClient.verifyRemoteChecksum(
                                peer, objectId, expectedVersion, expectedChecksum, expectedSize);
                        if (verifyStatus == PeerObjectClient.FetchStatus.VERIFIED) {
                            successfulReads++;
                        } else if (verifyStatus == PeerObjectClient.FetchStatus.CORRUPT) {
                            corruptionGenealogyTracker.recordCorruption(objectId, peer.getNodeId(), selfId);
                        }
                    }
                }
            }

            if (successfulReads >= readQuorum && (localVerifiedStream != null || stagedFallbackFile != null)) {
                vaultMetrics.incrementReads();
                InputStream bodyStream = localVerifiedStream != null
                        ? localVerifiedStream
                        : peerObjectClient.openDeleteOnCloseStream(stagedFallbackFile);
                responseOwnsStream = true;

                var builder = ResponseEntity.ok()
                        .header("X-Vault-Version", String.valueOf(expectedVersion))
                        .header("X-Vault-Checksum", expectedChecksum)
                        .header("X-Vault-Consistency-Mode", mode.name());
                if (expectedSize >= 0) {
                    builder.header("Content-Length", String.valueOf(expectedSize));
                }
                return builder.body(new InputStreamResource(bodyStream));
            }

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Insufficient verified replicas available to meet read quorum (" + successfulReads + "/" + readQuorum + ")");

        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to read object {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        } finally {
            if (!responseOwnsStream) {
                if (localVerifiedStream != null) {
                    try {
                        localVerifiedStream.close();
                    } catch (Exception ignored) {}
                }
                PeerObjectClient.deleteQuietly(stagedFallbackFile);
            }
        }
    }

    @RequestMapping(method = RequestMethod.HEAD, params = "key")
    public ResponseEntity<?> headObjectByQuery(@RequestParam("key") String objectId) {
        return headObject(objectId);
    }

    @RequestMapping(path = "/{*objectPath}", method = RequestMethod.HEAD)
    public ResponseEntity<?> headObjectByPath(@PathVariable String objectPath) {
        return headObject(normalizePathKey(objectPath));
    }

    public ResponseEntity<?> headObject(String objectId) {
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
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    public ResponseEntity<?> inspectObject(String objectId) {
        try {
            objectIdValidator.validateOrThrow(objectId);
            ObjectMeta meta = metadataService.findLatestActive(objectId);
            if (meta == null) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "Object not found or tombstoned"));
            }

            var replicas = replicaRepository.findByObjectIdAndVersion(objectId, meta.getVersion());
            int expected = switch (meta.getDurabilityTier()) {
                case CRITICAL -> 5;
                case STANDARD -> 3;
                case TEMPORARY -> 1;
            };

            long healthyReplicas = replicas.stream()
                    .filter(r -> r.getStatus() == com.vault.common.model.ReplicaStatus.HEALTHY)
                    .count();

            var locations = replicas.stream().map(r -> Map.of(
                "nodeId", r.getNodeId().toString(),
                "checksum", r.getChecksumSha256(),
                "status", r.getStatus().name(),
                "verified", r.getStatus() == com.vault.common.model.ReplicaStatus.HEALTHY
            )).toList();

            return ResponseEntity.ok(Map.ofEntries(
                Map.entry("objectId", meta.getObjectId()),
                Map.entry("version", meta.getVersion()),
                Map.entry("sizeBytes", meta.getSizeBytes()),
                Map.entry("checksumSha256", meta.getChecksumSha256()),
                Map.entry("durabilityTier", meta.getDurabilityTier().name()),
                Map.entry("expectedReplicas", expected),
                Map.entry("actualReplicas", replicas.size()),
                Map.entry("healthyReplicas", healthyReplicas),
                Map.entry("degradedPlacement", healthyReplicas < expected),
                Map.entry("replicaLocations", locations),
                Map.entry("storagePath", localStorageService.getRelativePath(objectId, meta.getVersion())),
                Map.entry("status", meta.getStatus().name()),
                Map.entry("createdAt", meta.getCreatedAt() != null ? meta.getCreatedAt().toString() : "")
            ));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to inspect object {}", objectId, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping
    public ResponseEntity<?> listObjects(
            @RequestParam(required = false, defaultValue = "") String prefix,
            @RequestParam(required = false, defaultValue = "100") int limit) {
        try {
            int effectiveLimit = Math.min(Math.max(limit, 1), 1000);
            var page = metadataService.listObjects(prefix, effectiveLimit);
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
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to list objects");
        }
    }

    @DeleteMapping(params = "key")
    public ResponseEntity<?> deleteObjectByQuery(@RequestParam("key") String objectId) {
        return deleteObject(objectId);
    }

    @DeleteMapping("/{*objectPath}")
    public ResponseEntity<?> deleteObjectByPath(@PathVariable String objectPath) {
        return deleteObject(normalizePathKey(objectPath));
    }

    public ResponseEntity<?> deleteObject(String objectId) {
        try {
            objectIdValidator.validateOrThrow(objectId);
            
            // Phase 2: Redirect to leader if not leader
            if (!nodeState.isLeader()) {
                String leaderAddr = nodeState.getLeaderAddress();
                if (leaderAddr == null) {
                    return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("No leader - retry later");
                }
                URI redirectUri = UriComponentsBuilder.fromHttpUrl("http://" + leaderAddr + "/api/v1/objects")
                        .queryParam("key", objectId)
                        .build()
                        .encode()
                        .toUri();
                return ResponseEntity.status(HttpStatus.TEMPORARY_REDIRECT)
                        .location(redirectUri)
                        .header("X-Vault-Leader-Address", leaderAddr)
                        .build();
            }

            objectService.delete(objectId);
            return ResponseEntity.noContent().build();
            
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (com.vault.common.exception.StaleLeaderException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("Failed to delete object {}", objectId, e);
            if (e instanceof com.vault.common.exception.InsufficientQuorumException) {
                return ResponseEntity.status(507).body(e.getMessage());
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
}
