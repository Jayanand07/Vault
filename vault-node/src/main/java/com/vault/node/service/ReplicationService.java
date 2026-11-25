package com.vault.node.service;

import com.vault.common.dto.ReplicationResult;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.model.DurabilityTier;
import com.vault.node.model.Node;
import com.vault.node.placement.ConsistentHashRing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationService {

    private final ConsistentHashRing ring;
    private final NodeRegistryService nodeRegistryService;
    private final LocalStorageService localStorageService;
    private final WebClient internalWebClient;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;
    private final com.vault.node.repository.ReplicaRepository replicaRepository;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Value("${vault.replication.timeout-ms:5000}")
    private long replicationTimeoutMs = 5000L;

    public ReplicationResult replicate(String objectId, long version, DurabilityTier tier, Path tempFile, String checksum, long term) {
        int n = getReplicaCount(tier);
        int w = getWriteQuorum(tier);
        List<Node> targets = ring.getTargetNodes(objectId, n, tier);

        if (targets.size() < w) {
            vaultMetrics.incrementReplicationFailures();
            throw new InsufficientQuorumException(
                    "Need " + w + " confirmations for tier " + tier + ", but only " + targets.size() + " eligible nodes available");
        }
        if (targets.size() < n) {
            log.warn("Placement degraded for {} v{} ({}): only {} eligible targets available for N={} (W={})",
                    objectId, version, tier, targets.size(), n, w);
        }

        log.info("Replicating {} v{} to nodes: {}", objectId, version,
                targets.stream().map(Node::getAddress).collect(Collectors.toList()));

        List<CompletableFuture<Boolean>> futures = targets.stream()
                .map(node -> {
                    if (node.getNodeId().equals(nodeRegistryService.getSelfNodeId())) {
                        return CompletableFuture.supplyAsync(() -> {
                            try (InputStream is = Files.newInputStream(tempFile)) {
                                com.vault.common.dto.WriteResult localWrite = localStorageService.store(objectId, version, is);
                                if (!localWrite.getChecksumSha256().equals(checksum)) {
                                    log.error("Local store checksum mismatch for {}. Expected: {}, Actual: {}",
                                            objectId, checksum, localWrite.getChecksumSha256());
                                    return false;
                                }
                                com.vault.node.model.Replica localReplica = new com.vault.node.model.Replica();
                                localReplica.setObjectId(objectId);
                                localReplica.setVersion(version);
                                localReplica.setNodeId(nodeRegistryService.getSelfNodeId());
                                localReplica.setChecksumSha256(checksum);
                                replicaRepository.save(localReplica);
                                return true;
                            } catch (Exception e) {
                                log.error("Failed to store locally", e);
                                return false;
                            }
                        });
                    } else {
                        return replicateToNodeAsync(node, objectId, version, tempFile, checksum, term)
                                .orTimeout(replicationTimeoutMs, TimeUnit.MILLISECONDS)
                                .exceptionally(ex -> {
                                    log.warn("Failed to replicate to {}", node.getAddress(), ex);
                                    return false;
                                });
                    }
                })
                .toList();

        long acks = awaitQuorum(futures, w, replicationTimeoutMs);
        if (acks >= w) {
            return new ReplicationResult(objectId, version, checksum, acks);
        }
        vaultMetrics.incrementReplicationFailures();
        throw new InsufficientQuorumException("Need " + w + " confirmations, got " + acks);
    }

    private CompletableFuture<Boolean> replicateToNodeAsync(Node target, String objectId, long version, Path tempFile, String checksum, long term) {
        return internalWebClient.post()
                .uri("http://" + target.getAddress() + "/internal/replicate")
                .header("X-Vault-Node-Secret", nodeSecret)
                .header("X-Vault-Checksum", checksum)
                .header("X-Vault-Version", String.valueOf(version))
                .header("X-Vault-Term", String.valueOf(term))
                .header("X-Vault-Object-Id", objectId)
                .bodyValue(new FileSystemResource(tempFile))
                .retrieve()
                .toBodilessEntity()
                .map(response -> response.getStatusCode().is2xxSuccessful())
                .toFuture();
    }

    public ReplicationResult replicateTombstone(String objectId, long version, DurabilityTier tier, long term) {
        int n = getReplicaCount(tier);
        int w = getWriteQuorum(tier);
        List<Node> targets = ring.getTargetNodes(objectId, n, tier);

        if (targets.size() < w) {
            vaultMetrics.incrementReplicationFailures();
            throw new InsufficientQuorumException(
                    "Need " + w + " tombstone confirmations for tier " + tier + ", but only " + targets.size() + " eligible nodes available");
        }
        if (targets.size() < n) {
            log.warn("Tombstone placement degraded for {} v{} ({}): {} eligible targets for N={} (W={})",
                    objectId, version, tier, targets.size(), n, w);
        }

        log.info("Replicating TOMBSTONE for {} v{} to nodes: {}", objectId, version,
                targets.stream().map(Node::getAddress).collect(Collectors.toList()));

        List<CompletableFuture<Boolean>> futures = targets.stream()
                .map(node -> {
                    if (node.getNodeId().equals(nodeRegistryService.getSelfNodeId())) {
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                localStorageService.writeTombstoneMarker(objectId, version);
                                com.vault.node.model.Replica localTombstone = new com.vault.node.model.Replica();
                                localTombstone.setObjectId(objectId);
                                localTombstone.setVersion(version);
                                localTombstone.setNodeId(nodeRegistryService.getSelfNodeId());
                                localTombstone.setChecksumSha256("TOMBSTONE");
                                replicaRepository.save(localTombstone);
                                return true;
                            } catch (Exception e) {
                                log.error("Failed to tombstone locally", e);
                                return false;
                            }
                        });
                    } else {
                        java.net.URI uri = org.springframework.web.util.UriComponentsBuilder
                                .fromHttpUrl("http://" + node.getAddress() + "/internal/replicate")
                                .queryParam("key", objectId)
                                .queryParam("version", version)
                                .queryParam("term", term)
                                .build()
                                .encode()
                                .toUri();
                        return internalWebClient.delete()
                                .uri(uri)
                                .header("X-Vault-Node-Secret", nodeSecret)
                                .retrieve()
                                .toBodilessEntity()
                                .map(response -> response.getStatusCode().is2xxSuccessful())
                                .toFuture()
                                .orTimeout(replicationTimeoutMs, TimeUnit.MILLISECONDS)
                                .exceptionally(ex -> {
                                    log.warn("Failed to replicate tombstone to {}", node.getAddress(), ex);
                                    return false;
                                });
                    }
                })
                .toList();

        long acks = awaitQuorum(futures, w, replicationTimeoutMs);
        if (acks >= w) {
            return new ReplicationResult(objectId, version, "TOMBSTONE", acks);
        }
        vaultMetrics.incrementReplicationFailures();
        throw new InsufficientQuorumException("Need " + w + " confirmations, got " + acks);
    }

    /**
     * Resolves as soon as {@code w} peers acknowledge OR quorum becomes mathematically unreachable,
     * rather than blocking for slow/hung peers beyond quorum.
     */
    private long awaitQuorum(List<CompletableFuture<Boolean>> futures, int w, long timeoutMs) {
        if (w <= 0) {
            return 0L;
        }
        int total = futures.size();
        if (total < w) {
            return 0L;
        }

        CompletableFuture<Long> quorumOutcome = new CompletableFuture<>();
        AtomicInteger acks = new AtomicInteger(0);
        AtomicInteger completed = new AtomicInteger(0);

        for (CompletableFuture<Boolean> future : futures) {
            future.whenComplete((ok, ex) -> {
                int currentAcks = (ex == null && Boolean.TRUE.equals(ok)) ? acks.incrementAndGet() : acks.get();
                int done = completed.incrementAndGet();
                int remaining = total - done;
                if (currentAcks >= w) {
                    quorumOutcome.complete((long) currentAcks);
                } else if (currentAcks + remaining < w) {
                    quorumOutcome.complete((long) currentAcks);
                }
            });
        }

        return quorumOutcome
                .orTimeout(timeoutMs + 1000L, TimeUnit.MILLISECONDS)
                .exceptionally(ex -> (long) acks.get())
                .join();
    }

    private int getReplicaCount(DurabilityTier tier) {
        return tier.getN();
    }

    private int getWriteQuorum(DurabilityTier tier) {
        return tier.getW();
    }
}
