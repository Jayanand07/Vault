package com.vault.node.service;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.repository.ObjectMetaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MetadataService {

    private final ObjectMetaRepository objectMetaRepository;

    @Transactional
    public long claimVersion(String objectId, long size, String checksum, long term, DurabilityTier tier) {
        // Find latest version across ALL statuses to determine next version
        Optional<ObjectMeta> maxVersion = objectMetaRepository.findFirstByObjectIdOrderByVersionDesc(objectId);
        long newVersion = maxVersion.map(m -> m.getVersion() + 1).orElse(1L);

        // Alternatively, finding the max version over ALL statuses might be safer, 
        // but finding active ensures we always advance from the latest good state.
        
        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId(objectId);
        meta.setVersion(newVersion);
        meta.setSizeBytes(size);
        meta.setChecksumSha256(checksum);
        meta.setTerm(term);
        meta.setDurabilityTier(tier);
        meta.setStatus(ObjectStatus.PENDING);
        
        objectMetaRepository.save(meta);
        
        return newVersion;
    }

    @Transactional
    public void commitVersion(String objectId, long version) {
        ObjectMetaId id = new ObjectMetaId(objectId, version);
        objectMetaRepository.findById(id).ifPresent(meta -> {
            meta.setStatus(ObjectStatus.ACTIVE);
            objectMetaRepository.save(meta);
        });
    }

    @Transactional
    public void commitTombstone(String objectId, long version) {
        ObjectMetaId id = new ObjectMetaId(objectId, version);
        objectMetaRepository.findById(id).ifPresent(meta -> {
            meta.setStatus(ObjectStatus.TOMBSTONED);
            objectMetaRepository.save(meta);
        });
    }

    @Transactional
    public void rollbackVersion(String objectId, long version) {
        ObjectMetaId id = new ObjectMetaId(objectId, version);
        objectMetaRepository.findById(id).ifPresent(meta -> {
            meta.setStatus(ObjectStatus.FAILED);
            objectMetaRepository.save(meta);
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
        return objectMetaRepository.findByObjectIdStartingWithAndStatusOnlyLatest(
                prefix == null ? "" : prefix, 
                ObjectStatus.ACTIVE, 
                org.springframework.data.domain.PageRequest.of(0, limit > 0 ? limit : 100));
    }
}
