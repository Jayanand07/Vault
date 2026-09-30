package com.vault.node.model;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Entity
@Table(name = "objects")
@IdClass(ObjectMetaId.class)
@Data
@NoArgsConstructor
public class ObjectMeta {
    @Id
    private String objectId;

    @Id
    private Long version;

    @Column(nullable = false)
    private Long sizeBytes;

    @Column(nullable = false, length = 64)
    private String checksumSha256;

    private String contentType = "application/octet-stream";

    @Enumerated(EnumType.STRING)
    private DurabilityTier durabilityTier = DurabilityTier.STANDARD;

    @Column(nullable = false)
    private Long term;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ObjectStatus status = ObjectStatus.PENDING;

    private OffsetDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
    }
}
