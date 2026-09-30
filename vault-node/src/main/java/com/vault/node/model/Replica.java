package com.vault.node.model;

import com.vault.common.model.ReplicaStatus;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "replicas")
@IdClass(ReplicaId.class)
@Data
@NoArgsConstructor
public class Replica {
    @Id
    private String objectId;

    @Id
    private Long version;

    @Id
    private UUID nodeId;

    @Column(nullable = false, length = 64)
    private String checksumSha256;

    @Enumerated(EnumType.STRING)
    private ReplicaStatus status = ReplicaStatus.HEALTHY;

    private OffsetDateTime verifiedAt;
    private OffsetDateTime createdAt;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "nodeId", insertable = false, updatable = false)
    private Node node;

    // Optional: relation to ObjectMeta, but it's composite key mapping
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumns({
        @JoinColumn(name="objectId", referencedColumnName="objectId", insertable = false, updatable = false),
        @JoinColumn(name="version", referencedColumnName="version", insertable = false, updatable = false)
    })
    private ObjectMeta objectMeta;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
    }
}
