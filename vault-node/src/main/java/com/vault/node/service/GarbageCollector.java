package com.vault.node.service;

import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.repository.ObjectMetaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor
@Slf4j
public class GarbageCollector {

    private final ObjectMetaRepository objectMetaRepository;
    private final com.vault.node.repository.ReplicaRepository replicaRepository;
    private final com.vault.node.repository.NodeRepository nodeRepository;
    private final NodeRegistryService nodeRegistryService;
    private final org.springframework.web.reactive.function.client.WebClient internalWebClient;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Value("${vault.node.data-dir}")
    private String dataDirStr;

    @Transactional
    public void queueRemoval(com.vault.node.model.Replica replica) {
        log.info("Queueing removal for extra replica on node {}: {} v{}", replica.getNodeId(), replica.getObjectId(), replica.getVersion());
        replicaRepository.delete(replica);
    }

    // Run every 24 hours (86400000 ms), but set lower for demo/capstone purposes (e.g. 2 minutes)
    @Scheduled(fixedDelayString = "${vault.gc.sweep-interval-ms:120000}")
    @Transactional
    public void sweep() {
        log.info("Starting Garbage Collection sweep...");

        OffsetDateTime tombstoneThreshold = OffsetDateTime.now().minus(72, ChronoUnit.HOURS);
        OffsetDateTime failedThreshold = OffsetDateTime.now().minus(24, ChronoUnit.HOURS);

        // 1. & 2. Find and delete old TOMBSTONED and FAILED rows
        List<ObjectMeta> toDelete = objectMetaRepository.findAll().stream()
                .filter(m -> 
                    (m.getStatus() == ObjectStatus.TOMBSTONED && m.getCreatedAt().isBefore(tombstoneThreshold)) ||
                    (m.getStatus() == ObjectStatus.FAILED && m.getCreatedAt().isBefore(failedThreshold)))
                .toList();

        if (!toDelete.isEmpty()) {
            for (ObjectMeta meta : toDelete) {
                List<com.vault.node.model.Replica> replicas = replicaRepository.findByObjectIdAndVersion(meta.getObjectId(), meta.getVersion());
                
                for (com.vault.node.model.Replica r : replicas) {
                    try {
                        com.vault.node.model.Node targetNode = nodeRepository.findById(r.getNodeId()).orElse(null);
                        if (targetNode != null) {
                            internalWebClient.delete()
                                    .uri("http://" + targetNode.getAddress() + "/internal/gc/" + r.getObjectId() + "?version=" + r.getVersion())
                                    .header("X-Vault-Node-Secret", nodeSecret)
                                    .retrieve().toBodilessEntity().block();
                        } else {
                            // Node doesn't exist anymore, just delete the dangling replica row
                            replicaRepository.delete(r);
                        }
                    } catch (Exception e) {
                        log.warn("GC: Failed to clean physical replica for {} on node {}", meta.getObjectId(), r.getNodeId());
                    }
                }
                
                // Only delete ObjectMeta if all replica rows have been successfully cleaned
                long remaining = replicaRepository.findByObjectIdAndVersion(meta.getObjectId(), meta.getVersion()).size();
                if (remaining == 0) {
                    objectMetaRepository.delete(meta);
                    log.info("GC: Deleted metadata for {} v{} completely.", meta.getObjectId(), meta.getVersion());
                } else {
                    log.warn("GC: Cannot delete metadata for {} v{} yet, {} replicas still remain.", meta.getObjectId(), meta.getVersion(), remaining);
                }
            }
        }

        // 3. Clean .tombstone marker files from disk
        Path tombstonesDir = Paths.get(dataDirStr, "tombstones");
        try (Stream<Path> stream = Files.walk(tombstonesDir)) {
            long deletedMarkers = stream.filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".tombstone"))
                    .filter(p -> {
                        try {
                            return Files.getLastModifiedTime(p).toInstant()
                                    .isBefore(tombstoneThreshold.toInstant());
                        } catch (IOException e) {
                            return false;
                        }
                    })
                    .peek(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException ignored) {}
                    }).count();
            
            if (deletedMarkers > 0) {
                log.info("GC: Deleted {} old .tombstone markers from disk.", deletedMarkers);
            }
        } catch (IOException e) {
            log.error("Failed to sweep tombstones directory", e);
        }

        log.info("Garbage Collection sweep complete.");
    }
}
