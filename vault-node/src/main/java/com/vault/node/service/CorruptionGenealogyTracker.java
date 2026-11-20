package com.vault.node.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class CorruptionGenealogyTracker {

    private final NodeRegistryService nodeRegistryService;
    private final Cache<UUID, AtomicInteger> corruptionCounts = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(24))
            .build();

    public void recordCorruption(String objectId, UUID sourceNodeId, UUID reportingNodeId) {
        log.error("[CGT] objectId={} corrupted_on={} source_of_repair={}", objectId, sourceNodeId, reportingNodeId);

        AtomicInteger counter = corruptionCounts.get(sourceNodeId, k -> new AtomicInteger(0));
        int count = counter.incrementAndGet();
        
        if (count > 5) {
            log.error("[CGT] Node {} has exceeded corruption threshold ({}). Marking as SUSPECT.", sourceNodeId, count);
            nodeRegistryService.markNodeSuspect(sourceNodeId);
        }
    }
}
