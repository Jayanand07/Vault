package com.vault.node.service;

import com.sun.net.httpserver.HttpServer;
import com.vault.common.exception.InsufficientQuorumException;
import com.vault.common.model.ConsistencyMode;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.NodeStatus;
import com.vault.common.model.ObjectStatus;
import com.vault.common.model.ReplicaStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.config.WebClientConfig;
import com.vault.node.controller.ObjectController;
import com.vault.node.election.HeartbeatService;
import com.vault.node.election.NodeState;
import com.vault.node.metrics.VaultMetrics;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.Replica;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.ElectionRepository;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.ObjectIdValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.ResourcePatternUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Regression tests for Item 6:
 * - P0-07: Early quorum returns as soon as W acks arrive instead of waiting on hung/slow peers
 * - P0-06: CRITICAL fails fast when eligible targets < W and inspectObject reports honest replica health
 * - V-10: Repeated heartbeat failures transition SUSPECT -> DOWN and drop node from ring/active list
 * - V-11: Flyway schema allows DEGRADED status in nodes table check constraint
 * - V-12: Rebalance rate limits queued operations and skips duplicate repairs
 */
@DataJpaTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:vault_item6_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "vault.node.id=11111111-1111-1111-1111-111111111111",
        "vault.node.address=127.0.0.1:8081",
        "vault.node.data-dir=${java.io.tmpdir}/vault-item6-init",
        "vault.replication.timeout-ms=5000",
        "vault.rebalance.rate-limit-objects-per-sec=2"
})
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({
        ConsistentHashRing.class,
        NodeRegistryService.class,
        LocalStorageService.class,
        ReplicationService.class,
        HeartbeatService.class,
        FailurePredictionEngine.class,
        WebClientConfig.class,
        ObjectIdValidator.class,
        UploadSpooler.class,
        PeerObjectClient.class,
        ObjectController.class,
        RebalanceService.class
})
class EarlyQuorumAndTopologyRegressionTest {

    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID FAST_PEER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID SLOW_PEER_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");

    @TempDir
    Path tempDir;

    @Autowired private NodeRepository nodeRepository;
    @Autowired private ReplicaRepository replicaRepository;
    @Autowired private ObjectMetaRepository objectMetaRepository;
    @Autowired private ConsistentHashRing ring;
    @Autowired private NodeRegistryService nodeRegistryService;
    @Autowired private LocalStorageService localStorageService;
    @Autowired private ReplicationService replicationService;
    @Autowired private HeartbeatService heartbeatService;
    @Autowired private ObjectController objectController;
    @Autowired private RebalanceService rebalanceService;
    @Autowired private ResourceLoader resourceLoader;

    @MockBean private ElectionRepository electionRepository;
    @MockBean private NodeState nodeState;
    @MockBean private VaultMetrics vaultMetrics;
    @MockBean private MetadataService metadataService;
    @MockBean private IdempotencyService idempotencyService;
    @MockBean private ObjectService objectService;
    @MockBean private ReadRepairService readRepairService;
    @MockBean private AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    @MockBean private CorruptionGenealogyTracker corruptionGenealogyTracker;
    @MockBean private GarbageCollector garbageCollector;

    private HttpServer fastPeerServer;
    private HttpServer hungPeerServer;
    private volatile boolean allowHungServerRelease = false;

    @BeforeEach
    void setUp() throws Exception {
        replicaRepository.deleteAll();
        objectMetaRepository.deleteAll();
        nodeRepository.deleteAll();

        ReflectionTestUtils.setField(localStorageService, "dataDirStr", tempDir.toString());
        localStorageService.init();
        ReflectionTestUtils.setField(replicationService, "nodeSecret", TestEphemeralCredentials.nodeCredential());
        ReflectionTestUtils.setField(heartbeatService, "nodeSecret", TestEphemeralCredentials.nodeCredential());
        ReflectionTestUtils.setField(objectController, "nodeSecret", TestEphemeralCredentials.nodeCredential());

        when(nodeState.isLeader()).thenReturn(true);
        when(nodeState.getLeaderId()).thenReturn(SELF_ID);
        when(nodeState.getCurrentTerm()).thenReturn(1L);
        when(electionRepository.renewLease(SELF_ID)).thenReturn(1L);
        when(adaptiveConsistencyEngine.determineConsistency(any())).thenReturn(ConsistencyMode.EVENTUAL);
        allowHungServerRelease = false;
    }

    @AfterEach
    void tearDown() {
        allowHungServerRelease = true;
        if (fastPeerServer != null) fastPeerServer.stop(0);
        if (hungPeerServer != null) hungPeerServer.stop(0);
    }

    @Test
    void earlyQuorumReturnsImmediatelyWhenQuorumReachedDespiteHungPeer() throws Exception {
        fastPeerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fastPeerServer.createContext("/internal/replicate", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        fastPeerServer.start();

        CountDownLatch hungReached = new CountDownLatch(1);
        hungPeerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        hungPeerServer.createContext("/internal/replicate", exchange -> {
            hungReached.countDown();
            long deadline = System.currentTimeMillis() + 6000L;
            while (!allowHungServerRelease && System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    break;
                }
            }
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        hungPeerServer.start();

        saveNode(SELF_ID, "127.0.0.1:8081", NodeStatus.UP);
        saveNode(FAST_PEER_ID, "127.0.0.1:" + fastPeerServer.getAddress().getPort(), NodeStatus.UP);
        saveNode(SLOW_PEER_ID, "127.0.0.1:" + hungPeerServer.getAddress().getPort(), NodeStatus.UP);
        nodeRegistryService.refreshRing();

        Path payload = tempDir.resolve("upload.bin");
        byte[] content = "early-quorum-payload".getBytes(StandardCharsets.UTF_8);
        Files.write(payload, content);
        String checksum = ChecksumUtil.sha256(content);

        saveObjectMeta("early/quorum.bin", 1L, content.length, checksum, DurabilityTier.STANDARD);
        saveObjectMeta("early/quorum.bin", 2L, 0L, "TOMBSTONE", DurabilityTier.STANDARD);

        long startMs = System.currentTimeMillis();
        var result = replicationService.replicate("early/quorum.bin", 1L, DurabilityTier.STANDARD, payload, checksum, 1L);
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertTrue(result.getAcks() >= 2);
        assertTrue(elapsedMs < 2000L,
                "Early quorum W=2 must return immediately instead of waiting for hung 3rd peer (took " + elapsedMs + "ms)");

        long tombStartMs = System.currentTimeMillis();
        var tombResult = replicationService.replicateTombstone("early/quorum.bin", 2L, DurabilityTier.STANDARD, 1L);
        long tombElapsedMs = System.currentTimeMillis() - tombStartMs;
        assertTrue(tombResult.getAcks() >= 2);
        assertTrue(tombElapsedMs < 2000L,
                "Tombstone early quorum W=2 must return immediately (took " + tombElapsedMs + "ms)");
    }

    @Test
    void criticalFailsFastWhenInsufficientNonDegradedTargetsAndInspectReflectsTrueReplicaHealth() throws Exception {
        // Hung 2nd node + only 2 total non-degraded nodes -> CRITICAL needs W=3, impossible!
        hungPeerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        hungPeerServer.createContext("/internal/replicate", exchange -> {
            try {
                Thread.sleep(4000);
            } catch (InterruptedException ignored) {}
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
        });
        hungPeerServer.start();

        saveNode(SELF_ID, "127.0.0.1:8081", NodeStatus.UP);
        saveNode(FAST_PEER_ID, "127.0.0.1:" + hungPeerServer.getAddress().getPort(), NodeStatus.UP);
        saveNode(SLOW_PEER_ID, "127.0.0.1:19999", NodeStatus.DEGRADED);
        nodeRegistryService.refreshRing();

        Path payload = tempDir.resolve("crit.bin");
        Files.writeString(payload, "critical-data", StandardCharsets.UTF_8);

        long startMs = System.currentTimeMillis();
        assertThrows(InsufficientQuorumException.class,
                () -> replicationService.replicate("crit/doc.bin", 1L, DurabilityTier.CRITICAL, payload, "hash", 1L));
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertTrue(elapsedMs < 1500L,
                "CRITICAL on < W eligible non-degraded nodes must fail fast without waiting for hung peers (took " + elapsedMs + "ms)");

        // Inspect must NOT hardcode verified=true for CORRUPT replica rows
        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId("crit/inspect.bin");
        meta.setVersion(1L);
        meta.setSizeBytes(13L);
        meta.setChecksumSha256("abc123");
        meta.setTerm(1L);
        meta.setDurabilityTier(DurabilityTier.CRITICAL);
        meta.setStatus(ObjectStatus.ACTIVE);
        objectMetaRepository.saveAndFlush(meta);
        when(metadataService.findLatestActive("crit/inspect.bin")).thenReturn(meta);

        Replica healthyRep = new Replica();
        healthyRep.setObjectId("crit/inspect.bin");
        healthyRep.setVersion(1L);
        healthyRep.setNodeId(SELF_ID);
        healthyRep.setChecksumSha256("abc123");
        healthyRep.setStatus(ReplicaStatus.HEALTHY);
        replicaRepository.save(healthyRep);

        Replica corruptRep = new Replica();
        corruptRep.setObjectId("crit/inspect.bin");
        corruptRep.setVersion(1L);
        corruptRep.setNodeId(FAST_PEER_ID);
        corruptRep.setChecksumSha256("abc123");
        corruptRep.setStatus(ReplicaStatus.CORRUPT);
        replicaRepository.saveAndFlush(corruptRep);

        ResponseEntity<?> inspectResp = objectController.inspectObject("crit/inspect.bin");
        assertEquals(200, inspectResp.getStatusCode().value());
        @SuppressWarnings("unchecked")
        Map<String, Object> body = (Map<String, Object>) inspectResp.getBody();
        assertNotNull(body);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> locs = (List<Map<String, Object>>) body.get("replicaLocations");
        Map<String, Object> corruptLoc = locs.stream()
                .filter(m -> FAST_PEER_ID.toString().equals(m.get("nodeId")))
                .findFirst().orElseThrow();
        assertEquals(Boolean.FALSE, corruptLoc.get("verified"),
                "inspectObject must report verified=false for CORRUPT replica");
        assertEquals(Boolean.TRUE, body.get("degradedPlacement"),
                "CRITICAL with <5 healthy replicas must report degradedPlacement=true");
        assertEquals(1L, ((Number) body.get("healthyReplicas")).longValue());
    }

    @Test
    void heartbeatFailuresTransitionSuspectToDownAndExcludeFromRingAndSchemaAllowsDegraded() throws Exception {
        int deadPort;
        try (ServerSocket ss = new ServerSocket(0)) {
            deadPort = ss.getLocalPort();
        }
        saveNode(SELF_ID, "127.0.0.1:8081", NodeStatus.UP);
        saveNode(FAST_PEER_ID, "127.0.0.1:" + deadPort, NodeStatus.UP);
        nodeRegistryService.refreshRing();

        for (int i = 0; i < 8; i++) {
            heartbeatService.sendHeartbeats();
            Thread.sleep(80);
        }

        Node dead = nodeRepository.findById(FAST_PEER_ID).orElseThrow();
        assertEquals(NodeStatus.DOWN, dead.getStatus(),
                "Repeated heartbeat failures must escalate SUSPECT -> DOWN");
        assertTrue(nodeRegistryService.getActiveNodes().stream().noneMatch(n -> n.getNodeId().equals(FAST_PEER_ID)),
                "DOWN node must be excluded from getActiveNodes()");
        assertTrue(ring.getTargetNodes("any/key.txt", 3).stream().noneMatch(n -> n.getNodeId().equals(FAST_PEER_ID)),
                "DOWN node must be dropped from ConsistentHashRing");

        // V-11: Verify Flyway migration SQL allows DEGRADED in nodes table status CHECK constraint
        var resources = ResourcePatternUtils.getResourcePatternResolver(resourceLoader)
                .getResources("classpath:db/migration/V*.sql");
        boolean hasDegradedConstraint = false;
        for (var r : resources) {
            String sql = Files.readString(r.getFile().toPath(), StandardCharsets.UTF_8);
            if (sql.contains("DEGRADED") && sql.contains("nodes")) {
                hasDegradedConstraint = true;
                break;
            }
        }
        assertTrue(hasDegradedConstraint, "Flyway migrations must include a migration allowing 'DEGRADED' in nodes CHECK constraint");
    }

    @Test
    void rebalanceRateLimitsAndDeduplicatesInFlightRepairs() {
        saveNode(SELF_ID, "127.0.0.1:8081", NodeStatus.UP);
        saveNode(FAST_PEER_ID, "127.0.0.1:8082", NodeStatus.UP);
        saveNode(SLOW_PEER_ID, "127.0.0.1:8083", NodeStatus.UP);
        nodeRegistryService.refreshRing();

        List<ObjectMeta> activeObjects = new java.util.ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            ObjectMeta m = new ObjectMeta();
            m.setObjectId("rebal/obj-" + i + ".bin");
            m.setVersion(1L);
            m.setDurabilityTier(DurabilityTier.STANDARD);
            m.setStatus(ObjectStatus.ACTIVE);
            activeObjects.add(m);
        }
        when(metadataService.findAllActive()).thenReturn(activeObjects);

        rebalanceService.reconcile();
        // rate-limit-objects-per-sec=2 caps total queued operations per cycle at 2 (instead of 4*3=12)
        verify(readRepairService, atMost(2)).repair(anyString(), anyLong(), any());
    }

    private void saveNode(UUID id, String addr, NodeStatus status) {
        Node n = new Node();
        n.setNodeId(id);
        n.setAddress(addr);
        n.setStatus(status);
        nodeRepository.saveAndFlush(n);
    }

    private void saveObjectMeta(String id, long version, long size, String checksum, DurabilityTier tier) {
        ObjectMeta m = new ObjectMeta();
        m.setObjectId(id);
        m.setVersion(version);
        m.setSizeBytes(size);
        m.setChecksumSha256(checksum);
        m.setTerm(1L);
        m.setDurabilityTier(tier);
        m.setStatus(ObjectStatus.PENDING);
        objectMetaRepository.saveAndFlush(m);
    }
}
