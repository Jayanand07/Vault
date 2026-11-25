package com.vault.node.service;

import com.vault.common.dto.ReplicationResult;
import com.vault.common.dto.WriteResult;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.exception.StaleLeaderException;
import com.vault.common.model.DurabilityTier;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.NodeState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;

@Service
@RequiredArgsConstructor
@Slf4j
public class ObjectService {

    private final MetadataService metadataService;
    private final ReplicationService replicationService;
    private final NodeState nodeState;
    private final GarbageCollector garbageCollector;

    private final com.google.common.util.concurrent.Striped<java.util.concurrent.locks.Lock> objectLocks = com.google.common.util.concurrent.Striped.lock(256);

    public WriteResult put(String objectId, Path tempFile, DurabilityTier tier) throws IOException {
        java.util.concurrent.locks.Lock lock = objectLocks.get(objectId);
        lock.lock();
        try {
            String checksum = ChecksumUtil.sha256File(tempFile);
            long term = nodeState.getCurrentTerm();
            long fileSize = Files.size(tempFile);

            // 1. Claim a version number with bounded retry on concurrent version collision
            long version = claimVersionWithRetry(objectId, fileSize, checksum, term, tier);

            try {
                // 2. Replicate to target nodes
                ReplicationResult result = replicationService.replicate(objectId, version, tier, tempFile, checksum, term);

                // 3. Quorum met -> fenced commit
                metadataService.commitVersion(objectId, version);

                return new WriteResult(checksum, fileSize);
            } catch (Exception e) {
                // 3b. Quorum NOT met, lease lost, or replication failed -> rollback PENDING row
                try {
                    metadataService.rollbackVersion(objectId, version);
                } catch (Exception rollbackEx) {
                    log.warn("Failed to rollback version {} v{}", objectId, version, rollbackEx);
                }
                if (e instanceof InsufficientQuorumException iq) {
                    throw iq;
                }
                if (e instanceof StaleLeaderException sl) {
                    throw sl;
                }
                throw new RuntimeException("Replication failed for " + objectId, e);
            }
        } finally {
            lock.unlock();
        }
    }
    
    public void delete(String objectId) {
        java.util.concurrent.locks.Lock lock = objectLocks.get(objectId);
        lock.lock();
        try {
            com.vault.node.model.ObjectMeta latest = metadataService.findLatestActive(objectId);
            if (latest == null) {
                log.debug("Skipping delete for non-existent or already-tombstoned key {}", objectId);
                return;
            }

            DurabilityTier tier = latest.getDurabilityTier() != null
                    ? latest.getDurabilityTier()
                    : DurabilityTier.STANDARD;
            long term = nodeState.getCurrentTerm();

            // 1. Claim a version for tombstone
            long version = claimVersionWithRetry(objectId, 0, "TOMBSTONE", term, tier);

            try {
                // 2. Replicate tombstone using object's durability tier
                replicationService.replicateTombstone(objectId, version, tier, term);

                // 3. Quorum met -> fenced commit tombstone
                metadataService.commitTombstone(objectId, version);
            } catch (Exception e) {
                // 3b. Quorum NOT met, lease lost, or replication failed -> rollback PENDING row
                try {
                    metadataService.rollbackVersion(objectId, version);
                } catch (Exception rollbackEx) {
                    log.warn("Failed to rollback tombstone {} v{}", objectId, version, rollbackEx);
                }
                if (e instanceof InsufficientQuorumException iq) {
                    throw iq;
                }
                if (e instanceof StaleLeaderException sl) {
                    throw sl;
                }
                throw new RuntimeException("Tombstone replication failed for " + objectId, e);
            }

            // 4. Only after tombstone commits: purge prior versions' replicas and disk payloads
            garbageCollector.purgePriorVersions(objectId, version);
        } finally {
            lock.unlock();
        }
    }

    private long claimVersionWithRetry(String objectId, long size, String checksum, long term, DurabilityTier tier) {
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return metadataService.claimVersion(objectId, size, checksum, term, tier);
            } catch (StaleLeaderException sl) {
                throw sl;
            } catch (DataAccessException e) {
                if (attempt == maxAttempts) {
                    throw e;
                }
                log.warn("Version claim collision/serialization retry ({}/{}) for {}", attempt, maxAttempts, objectId);
            }
        }
        throw new IllegalStateException("Unreachable");
    }
}
