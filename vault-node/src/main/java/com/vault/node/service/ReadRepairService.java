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
                    com.vault.common.dto.WriteResult writeResult = localStorageService.store(objectId, expectedVersion, is);
                    if (!writeResult.getChecksumSha256().equals(healthyReplica.getChecksumSha256())) {
                        log.error("Read repair checksum mismatch for {} v{}. Expected: {}, Got: {}",
                                objectId, expectedVersion, healthyReplica.getChecksumSha256(), writeResult.getChecksumSha256());
                        return;
                    }
                }

                // 4. Update local replica status to HEALTHY
                Replica r = replicaRepository.findById(new ReplicaId(objectId, expectedVersion, brokenNodeId))
                        .orElseGet(() -> {
                            Replica newRep = new Replica();
                            newRep.setObjectId(objectId);
                            newRep.setVersion(expectedVersion);
                            newRep.setNodeId(brokenNodeId);
                            newRep.setChecksumSha256(healthyReplica.getChecksumSha256());
                            return newRep;
                        });
                r.setStatus(ReplicaStatus.HEALTHY);
                r.setVerifiedAt(OffsetDateTime.now());
                replicaRepository.save(r);
                log.info("Successfully repaired {} v{} locally.", objectId, expectedVersion);
            } else {
                Node targetNode = nodeRepository.findById(brokenNodeId).orElse(null);
                if (targetNode != null) {
                    internalWebClient.post()
                            .uri("http://" + targetNode.getAddress() + "/internal/replicate")
                            .header("X-Vault-Node-Secret", nodeSecret)
                            .header("X-Vault-Checksum", healthyReplica.getChecksumSha256())
                            .header("X-Vault-Version", String.valueOf(expectedVersion))
                            .header("X-Vault-Term", "1")
                            .header("X-Vault-Object-Id", objectId)
                            .bodyValue(resource)
                            .retrieve()
                            .toBodilessEntity()
                            .block();
                    log.info("Successfully pushed replica of {} v{} to remote node {}", objectId, expectedVersion, targetNode.getAddress());
                }
            }
        } catch (Exception e) {
            log.error("Read repair failed for {} from node {}", objectId, sourceNode.getAddress(), e);
        }
    }
}
