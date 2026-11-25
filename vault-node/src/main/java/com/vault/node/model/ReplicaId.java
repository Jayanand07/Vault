package com.vault.node.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicaId implements Serializable {
    private String objectId;
    private Long version;
    private UUID nodeId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicaId replicaId = (ReplicaId) o;
        return Objects.equals(objectId, replicaId.objectId) &&
                Objects.equals(version, replicaId.version) &&
                Objects.equals(nodeId, replicaId.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, version, nodeId);
    }
}
