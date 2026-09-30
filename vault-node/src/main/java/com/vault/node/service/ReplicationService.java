package com.vault.node.service;

import com.vault.common.dto.ReplicationResult;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.model.DurabilityTier;
import com.vault.node.model.Node;
import com.vault.node.placement.ConsistentHashRing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.core.io.FileSystemResource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    public ReplicationResult replicate(String objectId, long version, DurabilityTier tier, Path tempFile, String checksum, long term) {
        // 1. Which nodes should have this file?
        int n = getReplicaCount(tier);
        int w = getWriteQuorum(tier);
        List<Node> targets = ring.getTargetNodes(objectId, n);

        log.info("Replicating {} v{} to nodes: {}", objectId, version, targets.stream().map(Node::getAddress).collect(Collectors.toList()));

        // 2. Send copies in PARALLEL
        List<CompletableFuture<Boolean>> futures = targets.stream()
                .map(node -> {
                    if (node.getNodeId().equals(nodeRegistryService.getSelfNodeId())) {
                        return CompletableFuture.supplyAsync(() -> {
                            try (InputStream is = Files.newInputStream(tempFile)) {
                                localStorageService.store(objectId, version, is);
                                return true;
                            } catch (Exception e) {
                                log.error("Failed to store locally", e);
                                return false;
                            }
                        });
                    } else {
                        return replicateToNodeAsync(node, objectId, version, tempFile, checksum, term)
                                .orTimeout(15, TimeUnit.SECONDS) // Larger timeout for file streams
                                .exceptionally(ex -> {
                                    log.warn("Failed to replicate to {}", node.getAddress(), ex);
                                    return false;
                                });
                    }
                })
                .toList();

        // 3. Wait for all to finish, count successes
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long acks = futures.stream().map(CompletableFuture::join).filter(ok -> ok).count();

        // 4. Did we get enough confirmations?
        if (acks >= w) {
            return new ReplicationResult(objectId, version, checksum, acks);
        } else {
            vaultMetrics.incrementReplicationFailures();
            throw new InsufficientQuorumException("Need " + w + " confirmations, got " + acks);
        }
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
        List<Node> targets = ring.getTargetNodes(objectId, n);

        log.info("Replicating TOMBSTONE for {} v{} to nodes: {}", objectId, version, targets.stream().map(Node::getAddress).collect(Collectors.toList()));

        List<CompletableFuture<Boolean>> futures = targets.stream()
                .map(node -> {
                    if (node.getNodeId().equals(nodeRegistryService.getSelfNodeId())) {
                        return CompletableFuture.supplyAsync(() -> {
                            try {
                                localStorageService.delete(objectId, version);
                                return true;
                            } catch (Exception e) {
                                log.error("Failed to tombstone locally", e);
                                return false;
                            }
                        });
                    } else {
                        return internalWebClient.delete()
                                .uri("http://" + node.getAddress() + "/internal/replicate/" + objectId + "?version=" + version + "&term=" + term)
                                .header("X-Vault-Node-Secret", nodeSecret)
                                .retrieve()
                                .toBodilessEntity()
                                .map(response -> response.getStatusCode().is2xxSuccessful())
                                .toFuture()
                                .orTimeout(5, java.util.concurrent.TimeUnit.SECONDS)
                                .exceptionally(ex -> {
                                    log.warn("Failed to replicate tombstone to {}", node.getAddress(), ex);
                                    return false;
                                });
                    }
                })
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long acks = futures.stream().map(CompletableFuture::join).filter(ok -> ok).count();

        if (acks >= w) {
            return new ReplicationResult(objectId, version, "TOMBSTONE", acks);
        } else {
            vaultMetrics.incrementReplicationFailures();
            throw new InsufficientQuorumException("Need " + w + " confirmations, got " + acks);
        }
    }
    
    private int getReplicaCount(DurabilityTier tier) {
        return tier.getN();
    }
    
    private int getWriteQuorum(DurabilityTier tier) {
        return tier.getW();
    }
}
