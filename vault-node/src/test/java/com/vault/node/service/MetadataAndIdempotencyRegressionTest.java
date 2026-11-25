package com.vault.node.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vault.common.dto.WriteResult;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.model.ConsistencyMode;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.controller.ObjectController;
import com.vault.node.controller.S3CompatibleController;
import com.vault.node.election.NodeState;
import com.vault.node.metrics.VaultMetrics;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.ElectionRepository;
import com.vault.node.repository.IdempotencyKeyRepository;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.ObjectIdValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Regression tests for Item 5:
 * - V-07: MetadataService.claimVersion uses explicit INSERT and never silently overwrites via JPA merge()
 * - P0-08: commitVersion / commitTombstone fence against stale term or expired leader lease
 * - V-06: Idempotency preserved across 307 redirect & transient failures, replays cached 201,
 *         enforces expires_at, and avoids junk rows for unkeyed S3 PUTs
 */
@DataJpaTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:vault_item5_test;MODE=PostgreSQL;NON_KEYWORDS=KEY;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "vault.storage.max-object-size-bytes=1048576"
})
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({
        MetadataService.class,
        ElectionRepository.class,
        IdempotencyService.class,
        ObjectIdValidator.class,
        UploadSpooler.class,
        ObjectController.class,
        S3CompatibleController.class,
        MetadataAndIdempotencyRegressionTest.TestJacksonConfig.class
})
class MetadataAndIdempotencyRegressionTest {

    @TestConfiguration
    static class TestJacksonConfig {
        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }

    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID OTHER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @TempDir
    Path tempDir;

    @Autowired private MetadataService metadataService;
    @Autowired private IdempotencyService idempotencyService;
    @Autowired private IdempotencyKeyRepository idempotencyKeyRepository;
    @Autowired private NamedParameterJdbcTemplate jdbcTemplate;
    @Autowired private PlatformTransactionManager transactionManager;
    @Autowired private ObjectController objectController;
    @Autowired private S3CompatibleController s3Controller;

    @SpyBean private ObjectMetaRepository objectMetaRepository;

    @MockBean private LocalStorageService localStorageService;
    @MockBean private ObjectService objectService;
    @MockBean private PeerObjectClient peerObjectClient;
    @MockBean private NodeState nodeState;
    @MockBean private ConsistentHashRing ring;
    @MockBean private WebClient internalWebClient;
    @MockBean private ReadRepairService readRepairService;
    @MockBean private NodeRegistryService nodeRegistryService;
    @MockBean private AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    @MockBean private CorruptionGenealogyTracker corruptionGenealogyTracker;
    @MockBean private VaultMetrics vaultMetrics;
    @MockBean private ReplicaRepository replicaRepository;

    @BeforeEach
    void setUp() {
        jdbcTemplate.getJdbcTemplate().execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id UUID PRIMARY KEY,
                    address VARCHAR(255) NOT NULL,
                    status VARCHAR(32) NOT NULL
                )
                """);
        jdbcTemplate.getJdbcTemplate().execute("""
                CREATE TABLE IF NOT EXISTS election_state (
                    id INT PRIMARY KEY,
                    current_term BIGINT NOT NULL DEFAULT 0,
                    leader_id UUID,
                    lease_expires TIMESTAMP WITH TIME ZONE,
                    updated_at TIMESTAMP WITH TIME ZONE
                )
                """);

        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM idempotency_keys");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM objects");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM election_state");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM nodes");

        jdbcTemplate.update(
                "INSERT INTO nodes (node_id, address, status) VALUES (:id, :addr, 'UP')",
                new MapSqlParameterSource().addValue("id", SELF_ID).addValue("addr", "127.0.0.1:8081"));
        jdbcTemplate.update(
                "INSERT INTO nodes (node_id, address, status) VALUES (:id, :addr, 'UP')",
                new MapSqlParameterSource().addValue("id", OTHER_ID).addValue("addr", "127.0.0.1:8082"));
        setElectionState(SELF_ID, 1L, Timestamp.from(Instant.now().plusSeconds(60)));

        reset(objectMetaRepository, objectService);
        when(nodeRegistryService.getSelfNodeId()).thenReturn(SELF_ID);
        when(nodeState.isLeader()).thenReturn(true);
        when(nodeState.getCurrentTerm()).thenReturn(1L);
        when(adaptiveConsistencyEngine.determineConsistency(any())).thenReturn(ConsistencyMode.EVENTUAL);
        ReflectionTestUtils.setField(objectController, "nodeSecret", TestEphemeralCredentials.nodeCredential());
    }

    @Test
    void claimVersionUsesExplicitInsertAndNeverOverwritesExistingVersionViaMerge() {
        ObjectMeta v1 = new ObjectMeta();
        v1.setObjectId("race/doc.txt");
        v1.setVersion(1L);
        v1.setSizeBytes(10L);
        v1.setChecksumSha256("sha-v1");
        v1.setTerm(1L);
        v1.setDurabilityTier(DurabilityTier.STANDARD);
        v1.setStatus(ObjectStatus.ACTIVE);
        objectMetaRepository.saveAndFlush(v1);

        // Simulate Writer #1 committing v2 in an independent transaction between Writer #2's MAX(version) read and INSERT
        doAnswer(inv -> {
            TransactionTemplate requiresNew = new TransactionTemplate(transactionManager);
            requiresNew.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
            requiresNew.executeWithoutResult(status -> jdbcTemplate.update("""
                    INSERT INTO objects (object_id, version, size_bytes, checksum_sha256, content_type, durability_tier, term, status, created_at)
                    VALUES ('race/doc.txt', 2, 20, 'winner-sha-v2', 'application/octet-stream', 'STANDARD', 1, 'ACTIVE', CURRENT_TIMESTAMP)
                    """, new MapSqlParameterSource()));
            return Optional.of(v1);
        }).when(objectMetaRepository).findFirstByObjectIdOrderByVersionDesc("race/doc.txt");

        assertThrows(DataIntegrityViolationException.class,
                () -> metadataService.claimVersion("race/doc.txt", 99L, "loser-sha-v2", 1L, DurabilityTier.STANDARD),
                "Concurrent duplicate version claim must throw DataIntegrityViolationException via explicit INSERT");

        ObjectMeta preserved = objectMetaRepository.findById(new ObjectMetaId("race/doc.txt", 2L)).orElseThrow();
        assertEquals("winner-sha-v2", preserved.getChecksumSha256(),
                "Explicit INSERT must never silently overwrite committed v2 via JPA merge()");
        assertEquals(ObjectStatus.ACTIVE, preserved.getStatus());
    }

    @Test
    void commitVersionAndTombstoneFenceAgainstExpiredLeaseAndStaleLeader() {
        setElectionState(SELF_ID, 5L, Timestamp.from(Instant.now().plusSeconds(60)));
        when(nodeState.getCurrentTerm()).thenReturn(5L);

        long vPut = metadataService.claimVersion("fenced/put.bin", 12L, "sha-put", 5L, DurabilityTier.STANDARD);
        long vDel = metadataService.claimVersion("fenced/del.bin", 0L, "TOMBSTONE", 5L, DurabilityTier.STANDARD);

        // 1. Expire lease before commitVersion
        setElectionState(SELF_ID, 5L, Timestamp.from(Instant.now().minusSeconds(10)));
        assertThrows(RuntimeException.class,
                () -> metadataService.commitVersion("fenced/put.bin", vPut),
                "commitVersion must reject commit when leader lease has expired");
        assertEquals(ObjectStatus.PENDING,
                objectMetaRepository.findById(new ObjectMetaId("fenced/put.bin", vPut)).orElseThrow().getStatus(),
                "Object version must NEVER be marked ACTIVE after lease expiry");

        // 2. Steal leadership in higher term before commitTombstone
        setElectionState(OTHER_ID, 6L, Timestamp.from(Instant.now().plusSeconds(60)));
        assertThrows(RuntimeException.class,
                () -> metadataService.commitTombstone("fenced/del.bin", vDel),
                "commitTombstone must reject commit when leadership moved to another node/term");
        assertEquals(ObjectStatus.PENDING,
                objectMetaRepository.findById(new ObjectMetaId("fenced/del.bin", vDel)).orElseThrow().getStatus(),
                "Tombstone must NEVER be marked TOMBSTONED by a stale leader");
    }

    @Test
    void idempotencyPreservedAcrossRedirectAndTransientFailureAndReplaysAndExpires() throws Exception {
        Path uploadFile = tempDir.resolve("payload.bin");
        Files.writeString(uploadFile, "idempotent-payload-bytes", StandardCharsets.UTF_8);

        // A. Follower 307 redirect must NOT consume idempotency key
        when(nodeState.isLeader()).thenReturn(false);
        when(nodeState.getLeaderAddress()).thenReturn("127.0.0.1:8082");
        ResponseEntity<?> redirectResp = objectController.putObject(
                "idem/redirect.txt", "key-redirect-1", DurabilityTier.STANDARD, uploadFile);
        assertEquals(307, redirectResp.getStatusCode().value());

        when(nodeState.isLeader()).thenReturn(true);
        when(objectService.put(anyString(), any(), any())).thenReturn(new WriteResult("sha-redir", 24L));
        ResponseEntity<?> afterRedirectResp = objectController.putObject(
                "idem/redirect.txt", "key-redirect-1", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, afterRedirectResp.getStatusCode().value(),
                "Leader must accept request with X-Idempotency-Key after follower 307 redirect");

        // B. Transient quorum failure (507) must release key so retry succeeds
        reset(objectService);
        when(objectService.put(anyString(), any(), any()))
                .thenThrow(new InsufficientQuorumException("Need 2 confirmations, got 1"))
                .thenReturn(new WriteResult("sha-retry", 24L));
        ResponseEntity<?> firstAttempt = objectController.putObject(
                "idem/retry.txt", "key-retry-2", DurabilityTier.STANDARD, uploadFile);
        assertEquals(507, firstAttempt.getStatusCode().value());

        ResponseEntity<?> secondAttempt = objectController.putObject(
                "idem/retry.txt", "key-retry-2", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, secondAttempt.getStatusCode().value(),
                "Retry with same X-Idempotency-Key after transient 507 must succeed instead of 409");

        // C. DONE replay returns cached 201 payload without re-executing ObjectService.put
        reset(objectService);
        when(objectService.put(anyString(), any(), any())).thenReturn(new WriteResult("sha-cached-99", 42L));
        ResponseEntity<?> initialDone = objectController.putObject(
                "idem/replay.txt", "key-replay-3", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, initialDone.getStatusCode().value());

        ResponseEntity<?> replayed = objectController.putObject(
                "idem/replay.txt", "key-replay-3", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, replayed.getStatusCode().value(),
                "Repeated request with completed X-Idempotency-Key must replay cached 201 instead of returning 409");
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) replayed.getBody();
        assertNotNull(body);
        assertEquals("sha-cached-99", body.get("checksum"));
        assertEquals(42, ((Number) body.get("size")).intValue());
        verify(objectService, times(1)).put(anyString(), any(), any());

        // D. Expired DONE key must allow fresh execution
        jdbcTemplate.update(
                "UPDATE idempotency_keys SET expires_at = :expired WHERE key = 'key-replay-3'",
                new MapSqlParameterSource().addValue("expired", Timestamp.from(Instant.now().minusSeconds(3600))));
        when(objectService.put(anyString(), any(), any())).thenReturn(new WriteResult("sha-after-expiry", 55L));
        ResponseEntity<?> expiredRetry = objectController.putObject(
                "idem/replay.txt", "key-replay-3", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, expiredRetry.getStatusCode().value());
        verify(objectService, times(2)).put(anyString(), any(), any());

        // D2. StaleLeaderException returns 409 Conflict and releases idempotency key for retry
        reset(objectService);
        when(objectService.put(anyString(), any(), any()))
                .thenThrow(new com.vault.common.exception.StaleLeaderException("Lease lost"))
                .thenReturn(new WriteResult("sha-recovered", 11L));
        ResponseEntity<?> staleResp = objectController.putObject(
                "idem/fenced.txt", "key-stale-409", DurabilityTier.STANDARD, uploadFile);
        assertEquals(409, staleResp.getStatusCode().value());
        ResponseEntity<?> retryAfterNewLeader = objectController.putObject(
                "idem/fenced.txt", "key-stale-409", DurabilityTier.STANDARD, uploadFile);
        assertEquals(201, retryAfterNewLeader.getStatusCode().value());

        // D3. Concurrent duplicate idempotency claims allow at most one CLAIMED winner
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch gate = new CountDownLatch(1);
            List<Callable<IdempotencyService.ClaimResult>> tasks = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                tasks.add(() -> {
                    assertTrue(gate.await(5, TimeUnit.SECONDS));
                    return idempotencyService.claimOrReplay("concurrent-key-99");
                });
            }
            List<Future<IdempotencyService.ClaimResult>> futures = new ArrayList<>();
            for (Callable<IdempotencyService.ClaimResult> task : tasks) {
                futures.add(pool.submit(task));
            }
            gate.countDown();
            long winners = 0;
            for (Future<IdempotencyService.ClaimResult> f : futures) {
                if (f.get(5, TimeUnit.SECONDS).status() == IdempotencyService.ClaimStatus.CLAIMED) {
                    winners++;
                }
            }
            assertEquals(1L, winners, "Exactly one concurrent caller may win idempotency claim");
        } finally {
            pool.shutdownNow();
        }

        // E. Unkeyed S3 PUT must not create junk rows in idempotency_keys
        long keysBeforeS3 = idempotencyKeyRepository.count();
        MockHttpServletRequest s3Req = new MockHttpServletRequest("PUT", "/s3/bucket-a/folder/item.bin");
        s3Req.setContentType("application/octet-stream");
        s3Req.setContent("s3-raw-body".getBytes(StandardCharsets.UTF_8));
        ResponseEntity<?> s3Resp = s3Controller.putObject("bucket-a", s3Req);
        assertEquals(200, s3Resp.getStatusCode().value());
        assertEquals(keysBeforeS3, idempotencyKeyRepository.count(),
                "S3 PUT without X-Idempotency-Key must not insert synthetic rows into idempotency_keys");
    }

    private void setElectionState(UUID leaderId, long term, Timestamp leaseExpires) {
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM election_state");
        jdbcTemplate.update(
                "INSERT INTO election_state (id, current_term, leader_id, lease_expires, updated_at) " +
                        "VALUES (1, :term, :leaderId, :expires, CURRENT_TIMESTAMP)",
                new MapSqlParameterSource()
                        .addValue("term", term)
                        .addValue("leaderId", leaderId)
                        .addValue("expires", leaseExpires));
    }
}
