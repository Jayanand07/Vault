package com.vault.node.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class CorruptionGenealogyTracker {

    private final NodeRegistryService nodeRegistryService;
    private final Map<UUID, Integer> corruptionCounts = new ConcurrentHashMap<>();

    public void recordCorruption(String objectId, UUID sourceNodeId, UUID reportingNodeId) {
        log.error("[CGT] objectId={} corrupted_on={} source_of_repair={}", objectId, sourceNodeId, reportingNodeId);

        int count = corruptionCounts.merge(sourceNodeId, 1, Integer::sum);
        
        if (count > 5) {
            log.error("[CGT] Node {} has exceeded corruption threshold ({}). Marking as SUSPECT.", sourceNodeId, count);
            nodeRegistryService.markNodeSuspect(sourceNodeId);
        }
    }
}
