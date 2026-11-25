package com.vault.node.model;

import com.vault.common.model.IdempotencyStatus;
import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;

@Entity
@Table(name = "idempotency_keys")
@Data
@NoArgsConstructor
public class IdempotencyKey {
    @Id
    private String key;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private IdempotencyStatus status = IdempotencyStatus.IN_PROGRESS;

    private Integer responseCode;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String responsePayload;

    private OffsetDateTime createdAt;
    private OffsetDateTime expiresAt;

    @PrePersist
    protected void onCreate() {
        createdAt = OffsetDateTime.now();
        if (expiresAt == null) {
            expiresAt = OffsetDateTime.now().plusHours(24);
        }
    }
}
