package com.vault.node.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class FailurePredictionEngine {

    // Track the last 5 latencies for each node
    private final Map<String, Queue<Long>> nodeLatencies = new ConcurrentHashMap<>();
    private final NodeRegistryService nodeRegistryService;

    public FailurePredictionEngine(NodeRegistryService nodeRegistryService) {
        this.nodeRegistryService = nodeRegistryService;
    }

    public void recordLatency(String nodeId, long latencyMs) {
        Queue<Long> latencies = nodeLatencies.computeIfAbsent(nodeId, k -> new LinkedList<>());
        synchronized (latencies) {
            latencies.add(latencyMs);
            if (latencies.size() > 5) {
                latencies.poll();
            }
            
            if (latencies.size() == 5) {
                analyzeTrend(nodeId, latencies);
            }
        }
    }

    private void analyzeTrend(String nodeId, Queue<Long> latencies) {
        Long[] lats = latencies.toArray(new Long[0]);
        
        // Simple PFD: If latency is strictly increasing for 3 consecutive ticks and > 1000ms
        // Or if the average of the last 2 is 50% higher than the first 3
        
        long sumFirst3 = lats[0] + lats[1] + lats[2];
        long avgFirst3 = sumFirst3 / 3;
        
        long sumLast2 = lats[3] + lats[4];
        long avgLast2 = sumLast2 / 2;
        
        if (avgFirst3 > 0 && avgLast2 > (avgFirst3 * 1.5) && avgLast2 > 500) {
            log.warn("[PFD] Node {} is showing severe latency degradation ({}ms -> {}ms). Marking as DEGRADED.", nodeId, avgFirst3, avgLast2);
            nodeRegistryService.markNodeDegraded(nodeId);
        }
    }
}
