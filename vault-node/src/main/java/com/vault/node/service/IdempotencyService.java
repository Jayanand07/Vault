package com.vault.node.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vault.common.model.IdempotencyStatus;
import com.vault.node.model.IdempotencyKey;
import com.vault.node.repository.IdempotencyKeyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class IdempotencyService {

    private final IdempotencyKeyRepository repository;
    private final ObjectMapper objectMapper;

    @Transactional
    public boolean claimKey(String key) {
        if (key == null || key.isBlank()) {
            return true; // No idempotency key provided, proceed normally
        }

        Optional<IdempotencyKey> existing = repository.findById(key);
        if (existing.isPresent()) {
            IdempotencyKey ik = existing.get();
            if (ik.getStatus() == IdempotencyStatus.DONE) {
                // Should return cached response. For simplicity in Phase 1, we just return false (reject).
                return false; 
            }
            if (ik.getStatus() == IdempotencyStatus.IN_PROGRESS && 
                ik.getCreatedAt().plusSeconds(30).isAfter(OffsetDateTime.now())) {
                // Request is currently being processed
                return false;
            }
            // If it's IN_PROGRESS but older than 30s, assume it crashed and we can retry
            ik.setStatus(IdempotencyStatus.IN_PROGRESS);
            ik.setCreatedAt(OffsetDateTime.now());
            repository.save(ik);
            return true;
        }

        IdempotencyKey newKey = new IdempotencyKey();
        newKey.setKey(key);
        newKey.setStatus(IdempotencyStatus.IN_PROGRESS);
        repository.save(newKey);
        return true;
    }

    @Transactional
    public void completeKey(String key, int responseCode, Object responsePayload) {
        if (key == null || key.isBlank()) return;

        repository.findById(key).ifPresent(ik -> {
            ik.setStatus(IdempotencyStatus.DONE);
            ik.setResponseCode(responseCode);
            try {
                ik.setResponsePayload(objectMapper.writeValueAsString(responsePayload));
            } catch (Exception e) {
                log.error("Failed to serialize response payload for idempotency key {}", key, e);
            }
            repository.save(ik);
        });
    }
}
