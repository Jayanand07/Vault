package com.vault.node.service;

import com.vault.common.model.ReplicaStatus;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReadRepairService {

    private final ReplicaRepository replicaRepository;
    private final NodeRepository nodeRepository;
    private final LocalStorageService localStorageService;
    private final NodeRegistryService nodeRegistryService;
    private final WebClient internalWebClient;
    private final PeerObjectClient peerObjectClient;
    private final NodeState nodeState;
    private final com.vault.node.metrics.VaultMetrics vaultMetrics;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Value("${vault.repair.max-concurrent-repairs:5}")
    private int maxConcurrentRepairs = 5;

    private final Set<String> inFlightRepairs = ConcurrentHashMap.newKeySet();
    private volatile Semaphore repairSemaphore;

    private Semaphore getRepairSemaphore() {
        Semaphore sem = this.repairSemaphore;
        if (sem == null) {
            synchronized (this) {
                sem = this.repairSemaphore;
                if (sem == null) {
                    sem = new Semaphore(Math.max(1, maxConcurrentRepairs));
                    this.repairSemaphore = sem;
                }
            }
        }
        return sem;
    }

    @Async
    public void repair(String objectId, long expectedVersion, UUID brokenNodeId) {
        String inFlightKey = objectId + "|v" + expectedVersion + "|" + brokenNodeId;
        if (!inFlightRepairs.add(inFlightKey)) {
            log.debug("Skipping duplicate in-flight repair for {} v{} on node {}", objectId, expectedVersion, brokenNodeId);
            return;
        }
        Semaphore sem = getRepairSemaphore();
        if (!sem.tryAcquire()) {
            inFlightRepairs.remove(inFlightKey);
            log.debug("Deferring repair for {} v{} on node {}: max-concurrent-repairs ({}) reached",
                    objectId, expectedVersion, brokenNodeId, maxConcurrentRepairs);
            return;
        }
        try {
            executeRepair(objectId, expectedVersion, brokenNodeId);
        } finally {
            sem.release();
            inFlightRepairs.remove(inFlightKey);
        }
    }

    private void executeRepair(String objectId, long expectedVersion, UUID brokenNodeId) {
        log.info("Starting background read-repair for object {}, version {} on node {}", objectId, expectedVersion, brokenNodeId);
        vaultMetrics.incrementRepairs();

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

        Path stagedFile = null;
        try {
            // Stage from healthy peer to temp and verify SHA-256 BEFORE installing or forwarding
            PeerObjectClient.FetchResult fetch = peerObjectClient.fetchVerifiedToTempFile(
                    sourceNode, objectId, expectedVersion, healthyReplica.getChecksumSha256(), -1L);
            if (fetch.status() != PeerObjectClient.FetchStatus.VERIFIED || fetch.tempFile() == null) {
                log.error("Read repair aborted for {} v{}: source {} failed verification ({})",
                        objectId, expectedVersion, sourceNode.getAddress(), fetch.status());
                return;
            }
            stagedFile = fetch.tempFile();

            if (brokenNodeId.equals(nodeRegistryService.getSelfNodeId())) {
                localStorageService.installVerifiedTempFile(objectId, expectedVersion, stagedFile);
                stagedFile = null; // Moved atomically to final destination

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
                log.info("Successfully repaired {} v{} locally after temp staging and SHA-256 verification.", objectId, expectedVersion);
            } else {
                Node targetNode = nodeRepository.findById(brokenNodeId).orElse(null);
                if (targetNode != null) {
                    long currentTerm = nodeState.getCurrentTerm();
                    internalWebClient.post()
                            .uri("http://" + targetNode.getAddress() + "/internal/replicate")
                            .header("X-Vault-Node-Secret", nodeSecret)
                            .header("X-Vault-Checksum", healthyReplica.getChecksumSha256())
                            .header("X-Vault-Version", String.valueOf(expectedVersion))
                            .header("X-Vault-Term", String.valueOf(currentTerm))
                            .header("X-Vault-Object-Id", objectId)
                            .bodyValue(new FileSystemResource(stagedFile))
                            .retrieve()
                            .toBodilessEntity()
                            .block();
                    log.info("Successfully pushed verified replica of {} v{} (term {}) to {}",
                            objectId, expectedVersion, currentTerm, targetNode.getAddress());
                }
            }
        } catch (Exception e) {
            log.error("Read repair failed for {} from node {}", objectId, sourceNode.getAddress(), e);
        } finally {
            PeerObjectClient.deleteQuietly(stagedFile);
        }
    }
}
