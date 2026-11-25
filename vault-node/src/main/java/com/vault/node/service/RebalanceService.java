package com.vault.node.service;

import com.vault.common.model.ReplicaStatus;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.Replica;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RebalanceService {

    private final MetadataService metadataService;
    private final NodeRegistryService nodeRegistryService;
    private final ConsistentHashRing hashRing;
    private final ReplicaRepository replicaRepository;
    private final ReadRepairService readRepairService;
    private final GarbageCollector garbageCollector;
    private final NodeState nodeState;

    @Value("${vault.rebalance.rate-limit-objects-per-sec:10}")
    private int rateLimitObjectsPerSec = 10;

    @Scheduled(fixedDelayString = "${vault.rebalance.interval-ms:60000}", initialDelay = 30000)
    public void reconcile() {
        if (!nodeState.isLeader()) {
            return; // Only leader orchestrates rebalancing
        }

        List<Node> activeNodes = nodeRegistryService.getActiveNodes();
        if (activeNodes.isEmpty()) {
            return;
        }

        List<ObjectMeta> allActive = metadataService.findAllActive();

        int maxActionsPerCycle = Math.max(1, rateLimitObjectsPerSec);
        int actionsQueued = 0;
        int repairsQueued = 0;
        int removalsQueued = 0;

        for (ObjectMeta obj : allActive) {
            if (actionsQueued >= maxActionsPerCycle) {
                log.info("Rebalance cycle reached per-cycle limit ({}); deferring remaining objects.", maxActionsPerCycle);
                break;
            }
            List<Node> shouldBeOn = hashRing.getTargetNodes(
                    obj.getObjectId(), obj.getDurabilityTier().getN(), obj.getDurabilityTier());
            List<Replica> currentReplicas = replicaRepository.findByObjectIdAndVersion(obj.getObjectId(), obj.getVersion());

            Set<UUID> shouldBeOnIds = shouldBeOn.stream().map(Node::getNodeId).collect(Collectors.toSet());
            Set<UUID> actuallyOnIds = currentReplicas.stream()
                    .filter(r -> r.getStatus() == ReplicaStatus.HEALTHY)
                    .map(Replica::getNodeId)
                    .collect(Collectors.toSet());

            // 1. Missing replicas -> trigger repair (rate-limited, deduplicated in ReadRepairService)
            for (Node target : shouldBeOn) {
                if (actionsQueued >= maxActionsPerCycle) {
                    break;
                }
                if (!actuallyOnIds.contains(target.getNodeId())) {
                    readRepairService.repair(obj.getObjectId(), obj.getVersion(), target.getNodeId());
                    repairsQueued++;
                    actionsQueued++;
                }
            }

            // 2. Extra replicas -> trigger garbage collection (only if we have enough healthy replicas)
            if (actionsQueued < maxActionsPerCycle && actuallyOnIds.size() >= obj.getDurabilityTier().getN()) {
                for (Replica r : currentReplicas) {
                    if (actionsQueued >= maxActionsPerCycle) {
                        break;
                    }
                    if (r.getStatus() == ReplicaStatus.HEALTHY && !shouldBeOnIds.contains(r.getNodeId())) {
                        garbageCollector.queueRemoval(r);
                        removalsQueued++;
                        actionsQueued++;
                    }
                }
            }
        }

        if (repairsQueued > 0 || removalsQueued > 0) {
            log.info("Rebalancing complete: {} repairs queued, {} removals queued.", repairsQueued, removalsQueued);
        }
    }
}
