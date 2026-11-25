package com.vault.node.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Objects;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ObjectMetaId implements Serializable {
    private String objectId;
    private Long version;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectMetaId that = (ObjectMetaId) o;
        return Objects.equals(objectId, that.objectId) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, version);
    }
}
