package com.vault.node.election;

import com.vault.node.model.Node;
import com.vault.node.repository.ElectionRepository;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class HeartbeatService {

    private final ElectionRepository electionRepository;
    private final NodeRegistryService nodeRegistryService;
    private final NodeState nodeState;
    private final com.vault.node.service.FailurePredictionEngine failurePredictionEngine;
    private final WebClient internalWebClient = WebClient.builder().build();

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    private final java.util.Map<java.util.UUID, Integer> failureCounts = new java.util.concurrent.ConcurrentHashMap<>();

    @Scheduled(fixedRate = 2000)
    public void sendHeartbeats() {
        if (!nodeState.isLeader()) {
            return; // Only leaders send heartbeats
        }

        // 1. Renew lease in Postgres
        Long currentTerm = null;
        try {
            currentTerm = electionRepository.renewLease(nodeRegistryService.getSelfNodeId());
        } catch (org.springframework.dao.DataAccessException e) {
            log.warn("Database unavailable. Cannot renew lease. Stepping down.");
        }
        
        if (currentTerm == null) {
            log.warn("Failed to renew lease in Postgres. Stepping down as leader.");
            try {
                ElectionRepository.ElectionStateRow state = electionRepository.getElectionState();
                if (state != null) {
                    nodeState.setLeaderId(state.leaderId, state.currentTerm);
                } else {
                    nodeState.setLeaderId(null, nodeState.getCurrentTerm());
                }
            } catch (Exception e) {
                nodeState.setLeaderId(null, nodeState.getCurrentTerm());
            }
            return;
        }

        // 2. Broadcast heartbeat to all other active nodes
        List<Node> others = nodeRegistryService.getActiveNodes().stream()
                .filter(n -> !n.getNodeId().equals(nodeRegistryService.getSelfNodeId()))
                .toList();

        for (Node node : others) {
            long startTime = System.currentTimeMillis();
            internalWebClient.post()
                    .uri("http://" + node.getAddress() + "/internal/heartbeat?term=" + currentTerm + "&leaderId=" + nodeRegistryService.getSelfNodeId())
                    .header("X-Vault-Node-Secret", nodeSecret)
                    .retrieve()
                    .toBodilessEntity()
                    .subscribe(
                            response -> {
                                long latency = System.currentTimeMillis() - startTime;
                                failurePredictionEngine.recordLatency(node.getNodeId().toString(), latency);

                                Integer failures = failureCounts.getOrDefault(node.getNodeId(), 0);
                                if (failures > 0) {
                                    failureCounts.put(node.getNodeId(), 0);
                                    nodeRegistryService.markNodeUp(node.getNodeId());
                                }
                            },
                            error -> {
                                int failures = failureCounts.getOrDefault(node.getNodeId(), 0) + 1;
                                failureCounts.put(node.getNodeId(), failures);
                                log.debug("Failed to send heartbeat to {}: {} (count: {})", node.getAddress(), error.getMessage(), failures);
                                if (failures == 3) {
                                    nodeRegistryService.markNodeSuspect(node.getNodeId());
                                }
                            }
                    );
        }
    }
}
