package com.vault.node.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "election_state")
@Data
@NoArgsConstructor
public class ElectionState {
    @Id
    private Integer id = 1;

    @Column(nullable = false)
    private Long currentTerm = 0L;

    private UUID leaderId;

    private OffsetDateTime leaseExpires;

    private OffsetDateTime updatedAt;

    @PrePersist
    @PreUpdate
    protected void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}
