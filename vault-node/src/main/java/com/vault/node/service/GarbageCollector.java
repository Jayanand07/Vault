package com.vault.node.service;

import com.vault.common.model.ObjectStatus;
import com.vault.node.election.NodeState;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.Replica;
import com.vault.node.repository.ObjectMetaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

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
    private final LocalStorageService localStorageService;
    private final NodeState nodeState;

    @Value("${vault.cluster.node-secret}")
    private String nodeSecret;

    @Value("${vault.node.data-dir}")
    private String dataDirStr;

    @Value("${vault.gc.tombstone-ttl-hours:72}")
    private long tombstoneTtlHours = 72;

    @Value("${vault.gc.failed-version-ttl-hours:24}")
    private long failedVersionTtlHours = 24;

    @Value("${vault.gc.superseded-version-ttl-hours:24}")
    private long supersededVersionTtlHours = 24;

    /**
     * Deletes the extra replica DB row first, then removes its physical file locally or on the target peer.
     */
    public void queueRemoval(Replica replica) {
        log.info("Queueing removal for extra replica on node {}: {} v{}",
                replica.getNodeId(), replica.getObjectId(), replica.getVersion());
        deleteReplicaAndFile(replica);
    }

    /**
     * Best-effort cleanup of older versions strictly below {@code beforeExclusiveVersion} after a tombstone commits.
     */
    public void purgePriorVersions(String objectId, long beforeExclusiveVersion) {
        List<ObjectMeta> versions = objectMetaRepository.findByObjectIdOrderByVersionAsc(objectId);
        for (ObjectMeta oldVer : versions) {
            if (oldVer.getVersion() < beforeExclusiveVersion) {
                if (purgeReplicasForVersion(oldVer)) {
                    objectMetaRepository.delete(oldVer);
                }
            }
        }
    }

    // Run every 2 minutes by default
    @Scheduled(fixedDelayString = "${vault.gc.sweep-interval-ms:120000}")
    public void sweep() {
        if (!nodeState.isLeader()) {
            log.debug("Skipping GC sweep on non-leader node");
            return;
        }

        log.info("Starting Garbage Collection sweep...");

        OffsetDateTime tombstoneThreshold = OffsetDateTime.now().minus(tombstoneTtlHours, ChronoUnit.HOURS);
        OffsetDateTime failedThreshold = OffsetDateTime.now().minus(failedVersionTtlHours, ChronoUnit.HOURS);
        OffsetDateTime supersededThreshold = OffsetDateTime.now().minus(supersededVersionTtlHours, ChronoUnit.HOURS);

        // 1. & 2. Purge aged TOMBSTONED and FAILED rows per version (replicas + disk files BEFORE objects row)
        List<ObjectMeta> toDelete = objectMetaRepository.findGcCandidates(
                ObjectStatus.TOMBSTONED, ObjectStatus.FAILED,
                tombstoneThreshold, failedThreshold);

        for (ObjectMeta meta : toDelete) {
            if (meta.getStatus() == ObjectStatus.TOMBSTONED) {
                boolean priorAllCleared = true;
                List<ObjectMeta> allVersions = objectMetaRepository.findByObjectIdOrderByVersionAsc(meta.getObjectId());
                for (ObjectMeta oldVer : allVersions) {
                    if (oldVer.getVersion() < meta.getVersion()) {
                        if (purgeReplicasForVersion(oldVer)) {
                            objectMetaRepository.delete(oldVer);
                        } else {
                            priorAllCleared = false;
                        }
                    }
                }
                if (!priorAllCleared) {
                    log.warn("GC: Deferring deletion of tombstone {} v{} until all prior versions' replicas clear.",
                            meta.getObjectId(), meta.getVersion());
                    continue;
                }
            }

            if (purgeReplicasForVersion(meta)) {
                objectMetaRepository.delete(meta);
                log.info("GC: Deleted metadata for {} v{} completely.", meta.getObjectId(), meta.getVersion());
            } else {
                log.warn("GC: Cannot delete metadata for {} v{} yet; replicas still remain.",
                        meta.getObjectId(), meta.getVersion());
            }
        }

        // 2b. Purge aged superseded ACTIVE versions superseded by a newer ACTIVE or TOMBSTONED version
        List<ObjectMeta> superseded = objectMetaRepository.findSupersededActiveVersions(
                ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED, supersededThreshold);
        for (ObjectMeta oldVer : superseded) {
            if (purgeReplicasForVersion(oldVer)) {
                objectMetaRepository.delete(oldVer);
                log.info("GC: Purged superseded version {} v{}.", oldVer.getObjectId(), oldVer.getVersion());
            }
        }

        // 3. Clean .tombstone marker files from disk
        Path tombstonesDir = Paths.get(dataDirStr, "tombstones");
        if (Files.exists(tombstonesDir)) {
            try (Stream<Path> stream = Files.walk(tombstonesDir)) {
                List<Path> oldMarkers = stream.filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".tombstone"))
                        .filter(p -> {
                            try {
                                return Files.getLastModifiedTime(p).toInstant()
                                        .isBefore(tombstoneThreshold.toInstant());
                            } catch (IOException e) {
                                return false;
                            }
                        }).toList();

                long deletedMarkers = 0;
                for (Path p : oldMarkers) {
                    try {
                        Files.deleteIfExists(p);
                        deletedMarkers++;
                    } catch (IOException ignored) {}
                }

                if (deletedMarkers > 0) {
                    log.info("GC: Deleted {} old .tombstone markers from disk.", deletedMarkers);
                }
            } catch (IOException e) {
                log.error("Failed to sweep tombstones directory", e);
            }
        }

        log.info("Garbage Collection sweep complete.");
    }

    private boolean purgeReplicasForVersion(ObjectMeta meta) {
        List<Replica> replicas = replicaRepository.findByObjectIdAndVersion(meta.getObjectId(), meta.getVersion());
        for (Replica r : replicas) {
            deleteReplicaAndFile(r);
        }
        return replicaRepository.findByObjectIdAndVersion(meta.getObjectId(), meta.getVersion()).isEmpty();
    }

    private void deleteReplicaAndFile(Replica r) {
        try {
            if (r.getNodeId().equals(nodeRegistryService.getSelfNodeId())) {
                replicaRepository.delete(r);
                localStorageService.deleteVersionFiles(r.getObjectId(), r.getVersion());
                return;
            }
            com.vault.node.model.Node targetNode = nodeRepository.findById(r.getNodeId()).orElse(null);
            if (targetNode != null) {
                java.net.URI gcUri = org.springframework.web.util.UriComponentsBuilder
                        .fromHttpUrl("http://" + targetNode.getAddress() + "/internal/gc")
                        .queryParam("key", r.getObjectId())
                        .queryParam("version", r.getVersion())
                        .build()
                        .encode()
                        .toUri();
                internalWebClient.delete()
                        .uri(gcUri)
                        .header("X-Vault-Node-Secret", nodeSecret)
                        .retrieve().toBodilessEntity().block();
            } else {
                // Node doesn't exist anymore, remove dangling replica row
                replicaRepository.delete(r);
            }
        } catch (Exception e) {
            log.warn("GC: Failed to clean physical replica for {} v{} on node {}",
                    r.getObjectId(), r.getVersion(), r.getNodeId());
        }
    }
}
