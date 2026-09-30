package com.vault.node.repository;

import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReplicaRepository extends JpaRepository<Replica, ReplicaId> {
    
    java.util.List<Replica> findByObjectIdAndVersion(String objectId, Long version);
    
    java.util.List<Replica> findByNodeId(java.util.UUID nodeId);
    
    @org.springframework.data.jpa.repository.Query("SELECT r FROM Replica r WHERE r.nodeId = :nodeId AND r.version = (SELECT MAX(r2.version) FROM Replica r2 WHERE r2.objectId = r.objectId AND r2.nodeId = :nodeId)")
    java.util.List<Replica> findLatestByNodeId(@org.springframework.data.repository.query.Param("nodeId") java.util.UUID nodeId);
}
