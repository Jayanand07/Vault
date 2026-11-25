package com.vault.node.service;

import com.vault.common.exception.StaleLeaderException;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.repository.ElectionRepository;
import com.vault.node.repository.ObjectMetaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class MetadataService {

    private final ObjectMetaRepository objectMetaRepository;
    private final ElectionRepository electionRepository;
    private final NodeRegistryService nodeRegistryService;

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public long claimVersion(String objectId, long size, String checksum, long term, DurabilityTier tier) {
        UUID selfId = nodeRegistryService.getSelfNodeId();
        if (!electionRepository.verifyAndFenceLeaderLease(selfId, term)) {
            throw new StaleLeaderException(
                    "Node " + selfId + " does not hold an active leader lease for term " + term);
        }

        // Find latest version across ALL statuses to determine next version
        Optional<ObjectMeta> maxVersion = objectMetaRepository.findFirstByObjectIdOrderByVersionDesc(objectId);
        long newVersion = maxVersion.map(m -> m.getVersion() + 1).orElse(1L);

        // Insert PENDING metadata row via explicit INSERT (Persistable.isNew() = true)
        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId(objectId);
        meta.setVersion(newVersion);
        meta.setSizeBytes(size);
        meta.setChecksumSha256(checksum);
        meta.setTerm(term);
        meta.setDurabilityTier(tier);
        meta.setStatus(ObjectStatus.PENDING);
        meta.markNew();

        objectMetaRepository.save(meta);
        objectMetaRepository.flush();

        return newVersion;
    }

    @Transactional
    public void commitVersion(String objectId, long version) {
        commitWithFencing(objectId, version, ObjectStatus.ACTIVE);
    }

    @Transactional
    public void commitTombstone(String objectId, long version) {
        commitWithFencing(objectId, version, ObjectStatus.TOMBSTONED);
    }

    private void commitWithFencing(String objectId, long version, ObjectStatus targetStatus) {
        ObjectMetaId id = new ObjectMetaId(objectId, version);
        ObjectMeta meta = objectMetaRepository.findById(id)
                .orElseThrow(() -> new IllegalStateException("ObjectMeta not found for " + objectId + " v" + version));
        if (meta.getStatus() != ObjectStatus.PENDING) {
            throw new IllegalStateException(
                    "Cannot transition " + objectId + " v" + version + " from " + meta.getStatus() + " to " + targetStatus);
        }
        UUID selfId = nodeRegistryService.getSelfNodeId();
        if (!electionRepository.verifyAndFenceLeaderLease(selfId, meta.getTerm())) {
            throw new StaleLeaderException(
                    "Stale leader or expired lease when committing " + objectId + " v" + version
                            + " for term " + meta.getTerm());
        }
        meta.setStatus(targetStatus);
        objectMetaRepository.save(meta);
    }

    @Transactional
    public void rollbackVersion(String objectId, long version) {
        ObjectMetaId id = new ObjectMetaId(objectId, version);
        objectMetaRepository.findById(id).ifPresent(meta -> {
            if (meta.getStatus() == ObjectStatus.PENDING) {
                meta.setStatus(ObjectStatus.FAILED);
                objectMetaRepository.save(meta);
            }
        });
    }

    @Transactional(readOnly = true)
    public ObjectMeta findLatestActive(String objectId) {
        return objectMetaRepository.findFirstByObjectIdAndStatusInOrderByVersionDesc(
                objectId, java.util.List.of(ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED))
                .filter(meta -> meta.getStatus() == ObjectStatus.ACTIVE)
                .orElse(null);
    }

    @Transactional(readOnly = true)
    public java.util.List<ObjectMeta> findAllActive() {
        return objectMetaRepository.findByStatusOnlyLatest(ObjectStatus.ACTIVE);
    }
    
    @Transactional(readOnly = true)
    public org.springframework.data.domain.Page<ObjectMeta> listObjects(String prefix, int limit) {
        String escapedPrefix = com.vault.common.util.ObjectKey.escapeLikePrefix(prefix);
        return objectMetaRepository.findByObjectIdStartingWithAndStatusOnlyLatest(
                escapedPrefix,
                ObjectStatus.ACTIVE,
                org.springframework.data.domain.PageRequest.of(0, limit > 0 ? limit : 100));
    }
}
