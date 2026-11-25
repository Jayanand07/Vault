package com.vault.node.config;

import com.vault.node.election.NodeState;
import com.vault.node.service.NodeRegistryService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Custom health indicator that reports the overall health of this Vault node.
 * Goes beyond simple "is Spring Boot running" and checks:
 * - Does this node know who the cluster leader is?
 * - How many peers are visible in the ring?
 * - Is the node itself registered and UP?
 */
@Component("vaultCluster")
@RequiredArgsConstructor
public class VaultHealthIndicator implements HealthIndicator {

    private final NodeState nodeState;
    private final NodeRegistryService nodeRegistryService;

    @Override
    public Health health() {
        int activeNodes = nodeRegistryService.getActiveNodes().size();
        boolean hasLeader = nodeState.getLeaderId() != null;
        boolean isLeader = nodeState.isLeader();

        Map<String, Object> details = Map.of(
            "nodeId", nodeRegistryService.getSelfNodeId().toString(),
            "isLeader", isLeader,
            "leaderId", hasLeader ? nodeState.getLeaderId().toString() : "NONE",
            "term", nodeState.getCurrentTerm(),
            "activeNodes", activeNodes
        );

        if (!hasLeader) {
            return Health.down()
                    .withDetails(details)
                    .withDetail("reason", "No leader elected — cluster may be initializing or partitioned")
                    .build();
        }

        if (activeNodes < 2) {
            return Health.status("DEGRADED")
                    .withDetails(details)
                    .withDetail("reason", "Fewer than 2 active nodes — quorum writes will fail for STANDARD tier")
                    .build();
        }

        return Health.up().withDetails(details).build();
    }
}
