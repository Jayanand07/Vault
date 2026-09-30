package com.vault.node.service;

import com.vault.common.model.ReplicaStatus;
import com.vault.node.model.Node;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.core.io.Resource;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReadRepairService {

    private final ReplicaRepository replicaRepository;
    private final NodeRepository nodeRepository;
    private final LocalStorageService localStorageService;
    private final NodeRegistryService nodeRegistryService;
    private final WebClient internalWebClient;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Async
    @Transactional
    public void repair(String objectId, long expectedVersion, UUID brokenNodeId) {
        log.info("Starting background read-repair for object {}, version {} on node {}", objectId, expectedVersion, brokenNodeId);
        vaultMetrics.incrementRepairs();

        // 1. Find a healthy replica
        List<Replica> replicas = replicaRepository.findByObjectIdAndVersion(objectId, expectedVersion);
        Replica healthyReplica = replicas.stream()
                .filter(r -> r.getStatus() == ReplicaStatus.HEALTHY && !r.getNodeId().equals(brokenNodeId))
                .findFirst()
                .orElse(null);

        if (healthyReplica == null) {
            log.error("Read repair failed for {}: no healthy replicas found!", objectId);
            return;
        }

        Node sourceNode = nodeRepository.findById(healthyReplica.getNodeId()).orElse(null);
        if (sourceNode == null) {
            log.error("Read repair failed for {}: source node {} not found in DB", objectId, healthyReplica.getNodeId());
            return;
        }

        try {
            // 2. Fetch data from healthy replica and stream to disk
            Resource resource = internalWebClient.get()
                    .uri("http://" + sourceNode.getAddress() + "/internal/objects/" + objectId + "?version=" + expectedVersion)
                    .header("X-Vault-Node-Secret", nodeSecret)
                    .retrieve()
                    .bodyToMono(Resource.class)
                    .block();

            if (resource == null) {
                throw new IllegalStateException("Remote node returned empty body");
            }

            // 3. Store locally (if we are the broken node, else push to it)
            if (brokenNodeId.equals(nodeRegistryService.getSelfNodeId())) {
                try (InputStream is = resource.getInputStream()) {
                    localStorageService.store(objectId, expectedVersion, is);
                }

                // 4. Update local replica status to HEALTHY
                replicaRepository.findById(new ReplicaId(objectId, expectedVersion, brokenNodeId))
                        .ifPresent(r -> {
                            r.setStatus(ReplicaStatus.HEALTHY);
                            r.setVerifiedAt(OffsetDateTime.now());
                            replicaRepository.save(r);
                            log.info("Successfully repaired {} v{} locally.", objectId, expectedVersion);
                        });
            } else {
                // We shouldn't typically be repairing other nodes directly via this method in this setup,
                // but we could POST to their /internal/replicate endpoint if needed.
                // For this capstone, read repair is triggered locally when a read fails.
                log.info("Repair triggered for a remote node, skipping as it will repair itself on read.");
            }
        } catch (Exception e) {
            log.error("Read repair failed for {} from node {}", objectId, sourceNode.getAddress(), e);
        }
    }
}
