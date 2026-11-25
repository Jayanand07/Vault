package com.vault.node.service;

import com.vault.common.model.ReplicaStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.common.util.ObjectKey;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class BackgroundScrubber {

    private final LocalStorageService localStorageService;
    private final ReplicaRepository replicaRepository;
    private final NodeRegistryService nodeRegistryService;
    private final ReadRepairService readRepairService;
    private final CorruptionGenealogyTracker corruptionGenealogyTracker;
    private final com.vault.node.repository.ObjectMetaRepository objectMetaRepository;

    @Value("${vault.repair.orphan-grace-seconds:60}")
    private long orphanGraceSeconds = 60L;

    // Run every 6 hours (21600000 ms) by default
    @Scheduled(fixedDelayString = "${vault.repair.scrub-interval-ms:21600000}")
    public void scrub() {
        List<Path> files = localStorageService.listAllDataFiles();
        log.info("Starting background scrub. Found {} files.", files.size());

        List<Replica> myReplicas = replicaRepository.findByNodeId(nodeRegistryService.getSelfNodeId());
        java.util.Map<String, Replica> replicaLookup = new java.util.HashMap<>();
        for (Replica r : myReplicas) {
            replicaLookup.put(ObjectKey.storageFileName(r.getObjectId(), r.getVersion()), r);
        }

        int checked = 0;
        int corrupt = 0;
        int orphaned = 0;

        for (Path file : files) {
            checked++;
            try {
                // filename format: {sha256(objectId)}.v{version}.dat
                String fileName = file.getFileName().toString();
                if (!fileName.endsWith(".dat")) continue;

                String baseName = fileName.substring(0, fileName.length() - 4); // remove .dat
                int vIndex = baseName.lastIndexOf(".v");
                if (vIndex == -1) continue;

                long version = Long.parseLong(baseName.substring(vIndex + 2));

                // Read file and checksum using streaming
                String actualChecksum = ChecksumUtil.sha256File(file);

                // Find replica in preloaded map using collision-free hashed filename
                Optional<Replica> replicaOpt = Optional.ofNullable(replicaLookup.get(fileName));

                if (replicaOpt.isEmpty()) {
                    Instant mtime = Files.getLastModifiedTime(file).toInstant();
                    if (orphanGraceSeconds > 0 && mtime.isAfter(Instant.now().minusSeconds(orphanGraceSeconds))) {
                        log.debug("Skipping recently written file {} within {}s orphan grace window", file, orphanGraceSeconds);
                        continue;
                    }
                    orphaned++;
                    log.warn("Scrubber found orphaned file (no metadata): {}", file);
                    try {
                        Files.deleteIfExists(file);
                        log.info("Deleted orphaned file: {}", file);
                    } catch (Exception e) {
                        log.error("Failed to delete orphaned file: {}", file);
                    }
                    continue;
                }

                Replica replica = replicaOpt.get();

                Optional<com.vault.node.model.ObjectMeta> metaOpt = objectMetaRepository.findById(
                    new com.vault.node.model.ObjectMetaId(replica.getObjectId(), version));

                if (metaOpt.isEmpty() || metaOpt.get().getStatus() == com.vault.common.model.ObjectStatus.TOMBSTONED) {
                    log.info("Scrubber found file for TOMBSTONED/missing metadata (offline during delete): {}", file);
                    try {
                        // Invariant: clear replica DB row BEFORE deleting the physical file
                        replicaRepository.delete(replica);
                        Files.deleteIfExists(file);
                        log.info("Cleaned up stale replica and file for {}", replica.getObjectId());
                    } catch (Exception e) {
                        log.error("Failed to clean stale replica: {}", file, e);
                    }
                    continue;
                }
                
                if (!actualChecksum.equals(replica.getChecksumSha256())) {
                    corrupt++;
                    log.error("Scrubber found CORRUPT file: {}. Expected {}, got {}", file, replica.getChecksumSha256(), actualChecksum);
                    
                    // Mark as corrupt in DB
                    replica.setStatus(ReplicaStatus.CORRUPT);
                    replicaRepository.save(replica);
                    
                    // Track corruption genealogy
                    corruptionGenealogyTracker.recordCorruption(replica.getObjectId(), nodeRegistryService.getSelfNodeId(), nodeRegistryService.getSelfNodeId());
                    
                    // Queue repair
                    readRepairService.repair(replica.getObjectId(), version, nodeRegistryService.getSelfNodeId());
                }

            } catch (Exception e) {
                log.error("Scrubber encountered error checking file {}", file, e);
            }
        }

        log.info("Scrub complete: checked={}, corrupt={}, orphaned={}", checked, corrupt, orphaned);
    }
}
