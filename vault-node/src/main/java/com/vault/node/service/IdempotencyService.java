package com.vault.node.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vault.common.model.IdempotencyStatus;
import com.vault.node.model.IdempotencyKey;
import com.vault.node.repository.IdempotencyKeyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    public enum ClaimStatus {
        CLAIMED,
        IN_PROGRESS,
        REPLAY
    }

    public record ClaimResult(ClaimStatus status, Integer responseCode, String responsePayload) {
        public static ClaimResult claimed() {
            return new ClaimResult(ClaimStatus.CLAIMED, null, null);
        }

        public static ClaimResult inProgress() {
            return new ClaimResult(ClaimStatus.IN_PROGRESS, null, null);
        }

        public static ClaimResult replay(Integer code, String payload) {
            return new ClaimResult(ClaimStatus.REPLAY, code, payload);
        }
    }

    private final IdempotencyKeyRepository repository;
    private final ObjectMapper objectMapper;
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final PlatformTransactionManager transactionManager;

    /**
     * Atomically claims {@code key} for execution, takes over expired or stale
     * (>30s) IN_PROGRESS rows, or returns a cached DONE replay without race windows.
     */
    public ClaimResult claimOrReplay(String key) {
        if (key == null || key.isBlank()) {
            return ClaimResult.claimed();
        }

        for (int attempt = 0; attempt < 2; attempt++) {
            Instant nowInstant = Instant.now();
            Timestamp nowTs = Timestamp.from(nowInstant);
            Timestamp expiresTs = Timestamp.from(nowInstant.plus(24, ChronoUnit.HOURS));
            Timestamp staleBeforeTs = Timestamp.from(nowInstant.minusSeconds(30));

            if (tryAtomicInsert(key, nowTs, expiresTs)) {
                return ClaimResult.claimed();
            }

            if (tryAtomicReclaimExpiredOrStale(key, nowTs, expiresTs, staleBeforeTs)) {
                return ClaimResult.claimed();
            }

            Optional<IdempotencyKey> existing = repository.findById(key);
            if (existing.isPresent()) {
                IdempotencyKey ik = existing.get();
                if (ik.getExpiresAt() != null && ik.getExpiresAt().toInstant().isBefore(nowInstant)) {
                    continue;
                }
                if (ik.getStatus() == IdempotencyStatus.DONE) {
                    return ClaimResult.replay(ik.getResponseCode(), ik.getResponsePayload());
                }
                return ClaimResult.inProgress();
            }
        }

        return ClaimResult.inProgress();
    }

    private boolean tryAtomicInsert(String key, Timestamp nowTs, Timestamp expiresTs) {
        TransactionTemplate requiresNew = new TransactionTemplate(transactionManager);
        requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        try {
            Integer inserted = requiresNew.execute(status -> jdbcTemplate.update(
                    """
                    INSERT INTO idempotency_keys (key, status, created_at, expires_at)
                    VALUES (:key, 'IN_PROGRESS', :now, :expiresAt)
                    """,
                    new MapSqlParameterSource()
                            .addValue("key", key)
                            .addValue("now", nowTs)
                            .addValue("expiresAt", expiresTs)));
            return inserted != null && inserted == 1;
        } catch (DataIntegrityViolationException duplicateKey) {
            return false;
        }
    }

    private boolean tryAtomicReclaimExpiredOrStale(
            String key, Timestamp nowTs, Timestamp expiresTs, Timestamp staleBeforeTs) {
        TransactionTemplate requiresNew = new TransactionTemplate(transactionManager);
        requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        Integer updated = requiresNew.execute(status -> jdbcTemplate.update(
                """
                UPDATE idempotency_keys
                SET status = 'IN_PROGRESS',
                    response_code = NULL,
                    response_payload = NULL,
                    created_at = :now,
                    expires_at = :expiresAt
                WHERE key = :key
                  AND (
                       expires_at < :now
                    OR (status = 'IN_PROGRESS' AND created_at < :staleBefore)
                  )
                """,
                new MapSqlParameterSource()
                        .addValue("key", key)
                        .addValue("now", nowTs)
                        .addValue("expiresAt", expiresTs)
                        .addValue("staleBefore", staleBeforeTs)));
        return updated != null && updated == 1;
    }

    @Transactional
    public void completeKey(String key, int responseCode, Object responsePayload) {
        if (key == null || key.isBlank()) {
            return;
        }
        repository.findById(key).ifPresent(ik -> {
            ik.setStatus(IdempotencyStatus.DONE);
            ik.setResponseCode(responseCode);
            ik.setExpiresAt(OffsetDateTime.now().plusHours(24));
            try {
                ik.setResponsePayload(responsePayload != null ? objectMapper.writeValueAsString(responsePayload) : null);
            } catch (Exception e) {
                log.error("Failed to serialize response payload for idempotency key {}", key, e);
            }
            repository.save(ik);
        });
    }

    /**
     * Releases an in-progress idempotency claim when an operation fails transiently
     * (e.g., 507 quorum loss, 409 stale leader, 500 internal error) so client retries can proceed.
     */
    @Transactional
    public void releaseKey(String key) {
        if (key == null || key.isBlank()) {
            return;
        }
        jdbcTemplate.update(
                "DELETE FROM idempotency_keys WHERE key = :key AND status = 'IN_PROGRESS'",
                new MapSqlParameterSource("key", key));
    }

    public Object deserializePayload(String rawJson) {
        if (rawJson == null || rawJson.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(rawJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return rawJson;
        }
    }
}
