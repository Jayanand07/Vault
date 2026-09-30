package com.vault.node.service;

import com.vault.common.model.ReplicaStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.repository.ReplicaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
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

    // Run every 6 hours (21600000 ms), but set lower for demo/capstone purposes
    // We'll set it to 1 minute for easier testing in the capstone phase
    @Scheduled(fixedDelayString = "${vault.repair.scrub-interval-ms:60000}")
    public void scrub() {
        List<Path> files = localStorageService.listAllDataFiles();
        log.info("Starting background scrub. Found {} files.", files.size());

        int checked = 0;
        int corrupt = 0;
        int orphaned = 0;

        for (Path file : files) {
            checked++;
            try {
                // filename format: {objectId_with_slashes_replaced_by_underscores}.v{version}.dat
                String fileName = file.getFileName().toString();
                if (!fileName.endsWith(".dat")) continue;

                String baseName = fileName.substring(0, fileName.length() - 4); // remove .dat
                int vIndex = baseName.lastIndexOf(".v");
                if (vIndex == -1) continue;

                String objectIdUnderscores = baseName.substring(0, vIndex);
                // In a real system, we'd encode the objectId better (e.g. Base64 or keep slashes on disk in directories)
                // For this capstone, replacing underscores back to slashes works for simple names.
                // Alternatively, we just read the checksum and query by checking ALL replicas on this node.
                long version = Long.parseLong(baseName.substring(vIndex + 2));

                // Read file and checksum
                byte[] data = Files.readAllBytes(file);
                String actualChecksum = ChecksumUtil.sha256(data);

                // Find replica in DB
                // Since objectId might have had slashes, let's find all replicas for this node and match by checksum/version
                List<Replica> myReplicas = replicaRepository.findByNodeId(nodeRegistryService.getSelfNodeId());
                
                Optional<Replica> replicaOpt = myReplicas.stream()
                        .filter(r -> r.getVersion() == version && r.getObjectId().replace("/", "_").equals(objectIdUnderscores))
                        .findFirst();

                if (replicaOpt.isEmpty()) {
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
                
                // --- NEW LOGIC: Check ObjectMeta status ---
                // The objectId could have had underscores replaced with slashes, but the DB uses the original ID.
                // Since the file format was objectId_underscores, we can query by the replica's actual objectId.
                Optional<com.vault.node.model.ObjectMeta> metaOpt = objectMetaRepository.findById(
                    new com.vault.node.model.ObjectMetaId(replica.getObjectId(), version));
                
                if (metaOpt.isEmpty() || metaOpt.get().getStatus() == com.vault.common.model.ObjectStatus.TOMBSTONED) {
                    log.info("Scrubber found file for TOMBSTONED/missing metadata (offline during delete): {}", file);
                    try {
                        Files.deleteIfExists(file);
                        replicaRepository.delete(replica);
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
