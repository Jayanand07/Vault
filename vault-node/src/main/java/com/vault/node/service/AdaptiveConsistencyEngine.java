package com.vault.node.service;

import com.vault.common.model.ConsistencyMode;
import com.vault.common.model.DurabilityTier;
import com.vault.node.repository.NodeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdaptiveConsistencyEngine {

    private final NodeRegistryService nodeRegistryService;
    private final NodeRepository nodeRepository;

    public ConsistencyMode determineConsistency(DurabilityTier tier) {
        // 1. Health Score
        long totalNodes = nodeRepository.count();
        int activeNodes = nodeRegistryService.getActiveNodes().size();
        
        double healthScore = totalNodes == 0 ? 0.0 : (double) activeNodes / totalNodes;

        // 2. Latency Score (mocked for now, assuming 50ms average -> good)
        // A real implementation would track rolling average from HeartbeatService
        double latencyScore = 0.9;

        // 3. Criticality Score
        double criticalityScore = switch (tier) {
            case CRITICAL -> 1.0;
            case STANDARD -> 0.6;
            case TEMPORARY -> 0.2;
        };

        // 4. Access Pattern (mocked to 0.8)
        double accessPatternScore = 0.8;

        double totalScore = (0.3 * healthScore) + 
                            (0.2 * latencyScore) + 
                            (0.4 * criticalityScore) + 
                            (0.1 * accessPatternScore);

        ConsistencyMode mode;
        if (totalScore >= 0.8) {
            mode = ConsistencyMode.STRONG;
        } else if (totalScore >= 0.5) {
            mode = ConsistencyMode.EVENTUAL;
        } else {
            mode = ConsistencyMode.BEST_EFFORT;
        }

        log.debug("ACT Evaluation - Health: {}, Latency: {}, Criticality: {}, Access: {} -> Score: {}, Mode: {}",
                healthScore, latencyScore, criticalityScore, accessPatternScore, totalScore, mode);

        return mode;
    }
}
