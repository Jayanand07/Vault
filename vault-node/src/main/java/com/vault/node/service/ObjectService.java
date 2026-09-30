package com.vault.node.service;

import com.vault.common.dto.ReplicationResult;
import com.vault.common.dto.WriteResult;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.model.DurabilityTier;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.NodeState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
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

    private final ConcurrentHashMap<String, ReentrantLock> objectLocks = new ConcurrentHashMap<>();

    public WriteResult put(String objectId, Path tempFile, DurabilityTier tier) throws IOException {
        ReentrantLock lock = objectLocks.computeIfAbsent(objectId, k -> new ReentrantLock());
        lock.lock();
        try {
            String checksum = ChecksumUtil.sha256File(tempFile);
            long term = nodeState.getCurrentTerm();
            long fileSize = Files.size(tempFile);

            // 1. Claim a version number
            long version = 0;
            try {
                version = metadataService.claimVersion(objectId, fileSize, checksum, term, tier);
            } catch (org.springframework.dao.DataIntegrityViolationException e) {
                // Race condition: another node claimed this version. Retry once.
                log.warn("Version claim collision for {}, retrying...", objectId);
                version = metadataService.claimVersion(objectId, fileSize, checksum, term, tier);
            }

            try {
                // 2. Replicate to target nodes
                ReplicationResult result = replicationService.replicate(objectId, version, tier, tempFile, checksum, term);

                // 3. Quorum met -> commit
                metadataService.commitVersion(objectId, version);

                return new WriteResult(checksum, fileSize);
            } catch (InsufficientQuorumException e) {
                // 3b. Quorum NOT met -> rollback
                metadataService.rollbackVersion(objectId, version);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }
    
    public void delete(String objectId) {
        ReentrantLock lock = objectLocks.computeIfAbsent(objectId, k -> new ReentrantLock());
        lock.lock();
        try {
            long term = nodeState.getCurrentTerm();
            
            // 1. Claim a version for tombstone
            long version = 0;
            try {
                version = metadataService.claimVersion(objectId, 0, "TOMBSTONE", term, DurabilityTier.STANDARD);
            } catch (org.springframework.dao.DataIntegrityViolationException e) {
                log.warn("Version claim collision for tombstone {}, retrying...", objectId);
                version = metadataService.claimVersion(objectId, 0, "TOMBSTONE", term, DurabilityTier.STANDARD);
            }
            
            try {
                // 2. Replicate tombstone
                replicationService.replicateTombstone(objectId, version, DurabilityTier.STANDARD, term);
                
                // 3. Quorum met -> commit tombstone
                metadataService.commitTombstone(objectId, version);
            } catch (InsufficientQuorumException e) {
                // 3b. Quorum NOT met -> rollback
                metadataService.rollbackVersion(objectId, version);
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }
}
