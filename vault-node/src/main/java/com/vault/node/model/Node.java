package com.vault.node.model;

import com.vault.common.model.NodeStatus;
import com.vault.common.model.PfdStatus;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "nodes")
@Data
@NoArgsConstructor
public class Node {
    @Id
    private UUID nodeId;

    @Column(nullable = false, unique = true)
    private String address;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NodeStatus status = NodeStatus.UP;

    @Enumerated(EnumType.STRING)
    @Column(name = "pfd_status")
    private PfdStatus pfdStatus = PfdStatus.HEALTHY;

    private Long diskTotalBytes = 0L;
    private Long diskUsedBytes = 0L;
    private Long objectCount = 0L;

    private OffsetDateTime lastHeartbeat;
    private OffsetDateTime joinedAt;
    private OffsetDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        joinedAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
