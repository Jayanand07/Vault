package com.vault.node.repository;

import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ObjectMetaRepository extends JpaRepository<ObjectMeta, ObjectMetaId> {
    Optional<ObjectMeta> findFirstByObjectIdAndStatusInOrderByVersionDesc(String objectId, java.util.Collection<ObjectStatus> statuses);
    
    Optional<ObjectMeta> findFirstByObjectIdOrderByVersionDesc(String objectId);
    @org.springframework.data.jpa.repository.Query("SELECT m FROM ObjectMeta m WHERE m.version = (SELECT MAX(m2.version) FROM ObjectMeta m2 WHERE m2.objectId = m.objectId) AND m.status = :status")
    java.util.List<ObjectMeta> findByStatusOnlyLatest(@org.springframework.data.repository.query.Param("status") ObjectStatus status);
    
    @org.springframework.data.jpa.repository.Query("SELECT m FROM ObjectMeta m WHERE m.objectId LIKE CONCAT(:prefix, '%') ESCAPE '\\' AND m.version = (SELECT MAX(m2.version) FROM ObjectMeta m2 WHERE m2.objectId = m.objectId) AND m.status = :status")
    org.springframework.data.domain.Page<ObjectMeta> findByObjectIdStartingWithAndStatusOnlyLatest(@org.springframework.data.repository.query.Param("prefix") String prefix, @org.springframework.data.repository.query.Param("status") ObjectStatus status, org.springframework.data.domain.Pageable pageable);

    @org.springframework.data.jpa.repository.Query("SELECT m FROM ObjectMeta m WHERE (m.status = :tombstoned AND m.createdAt < :tombstoneThreshold) OR (m.status = :failed AND m.createdAt < :failedThreshold)")
    java.util.List<ObjectMeta> findGcCandidates(
            @org.springframework.data.repository.query.Param("tombstoned") ObjectStatus tombstoned,
            @org.springframework.data.repository.query.Param("failed") ObjectStatus failed,
            @org.springframework.data.repository.query.Param("tombstoneThreshold") java.time.OffsetDateTime tombstoneThreshold,
            @org.springframework.data.repository.query.Param("failedThreshold") java.time.OffsetDateTime failedThreshold);

    java.util.List<ObjectMeta> findByObjectId(String objectId);

    java.util.List<ObjectMeta> findByObjectIdOrderByVersionAsc(String objectId);

    @org.springframework.data.jpa.repository.Query("SELECT m FROM ObjectMeta m WHERE m.status = :active AND m.createdAt < :supersededThreshold AND m.version < (SELECT MAX(m2.version) FROM ObjectMeta m2 WHERE m2.objectId = m.objectId AND m2.status IN (:active, :tombstoned))")
    java.util.List<ObjectMeta> findSupersededActiveVersions(
            @org.springframework.data.repository.query.Param("active") ObjectStatus active,
            @org.springframework.data.repository.query.Param("tombstoned") ObjectStatus tombstoned,
            @org.springframework.data.repository.query.Param("supersededThreshold") java.time.OffsetDateTime supersededThreshold);
}
