package com.vault.node.service;

import com.sun.net.httpserver.HttpServer;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.NodeStatus;
import com.vault.common.model.ObjectStatus;
import com.vault.common.model.ReplicaStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.config.WebClientConfig;
import com.vault.node.controller.InternalController;
import com.vault.node.election.ElectionService;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
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
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.ByteArrayInputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Regression tests for Item 4:
 * - V-04: GC purges replicas + physical files per version before deleting objects rows (no FK crash), plus superseded versions
 * - V-05: ObjectService.delete honors object durability tier and cleans physical data only after tombstone commit
 * - V-12: GC sweep runs only on leader; queueRemoval clears replica row then deletes physical file
 * - V-21: Deleting a non-existent key does not claim/write a tombstone version
 */
@DataJpaTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:vault_gc_test;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.username=sa",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "vault.node.data-dir=${java.io.tmpdir}/vault-gc-init",
        "vault.gc.tombstone-ttl-hours=72",
        "vault.gc.failed-version-ttl-hours=24",
        "vault.gc.superseded-version-ttl-hours=24"
})
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Import({GarbageCollector.class, LocalStorageService.class, ObjectIdValidator.class, WebClientConfig.class})
class DeleteGcLifecycleRegressionTest {

    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final String NODE_SECRET = TestEphemeralCredentials.nodeCredential();

    @TempDir
    Path tempDir;

    @Autowired private ObjectMetaRepository objectMetaRepository;
    @Autowired private ReplicaRepository replicaRepository;
    @Autowired private NodeRepository nodeRepository;
    @Autowired private GarbageCollector garbageCollector;
    @Autowired private LocalStorageService localStorageService;
    @Autowired private ObjectIdValidator objectIdValidator;

    @MockBean private NodeRegistryService nodeRegistryService;
    @MockBean private NodeState nodeState;
    @MockBean private ReplicationService replicationService;
    @MockBean private ElectionService electionService;

    private InternalController internalController;
    private HttpServer localInternalHttp;
    private String selfAddress;

    @BeforeEach
    void setUp() throws Exception {
        ReflectionTestUtils.setField(localStorageService, "dataDirStr", tempDir.toString());
        localStorageService.init();
        ReflectionTestUtils.setField(garbageCollector, "dataDirStr", tempDir.toString());

        when(nodeRegistryService.getSelfNodeId()).thenReturn(SELF_ID);
        when(nodeState.isLeader()).thenReturn(true);
        when(nodeState.getCurrentTerm()).thenReturn(1L);

        internalController = new InternalController(
                localStorageService, replicaRepository, nodeRegistryService,
                electionService, nodeState, objectIdValidator);
        ReflectionTestUtils.setField(internalController, "nodeSecret", NODE_SECRET);

        localInternalHttp = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        localInternalHttp.createContext("/internal/gc", exchange -> {
            String query = exchange.getRequestURI().getQuery();
            String key = extractQueryParam(query, "key");
            long version = Long.parseLong(extractQueryParam(query, "version"));
            String secret = exchange.getRequestHeaders().getFirst("X-Vault-Node-Secret");
            var resp = internalController.garbageCollectReplica(key, version, secret);
            exchange.sendResponseHeaders(resp.getStatusCode().value(), -1);
            exchange.close();
        });
        localInternalHttp.start();
        selfAddress = "127.0.0.1:" + localInternalHttp.getAddress().getPort();

        Node self = new Node();
        self.setNodeId(SELF_ID);
        self.setAddress(selfAddress);
        self.setStatus(NodeStatus.UP);
        nodeRepository.saveAndFlush(self);
    }

    @AfterEach
    void tearDown() {
        if (localInternalHttp != null) {
            localInternalHttp.stop(0);
        }
    }

    @Test
    void gcSweepPurgesTombstonedWithOldVersionReplicasAndSupersededVersionsWithoutFkCrash() throws Exception {
        byte[] v1Data = "tombstoned-v1-payload".getBytes(StandardCharsets.UTF_8);
        String v1Hash = ChecksumUtil.sha256(v1Data);
        localStorageService.store("docs/deleted.txt", 1L, new ByteArrayInputStream(v1Data));
        Path tombV1Dat = localStorageService.getFilePath("docs/deleted.txt", 1L);

        saveObjectMeta("docs/deleted.txt", 1L, v1Data.length, v1Hash, ObjectStatus.ACTIVE,
                OffsetDateTime.now().minusHours(90));
        saveReplica("docs/deleted.txt", 1L, SELF_ID, v1Hash);

        saveObjectMeta("docs/deleted.txt", 2L, 0L, "TOMBSTONE", ObjectStatus.TOMBSTONED,
                OffsetDateTime.now().minusHours(80));
        saveReplica("docs/deleted.txt", 2L, SELF_ID, "TOMBSTONE");

        // Overwritten object: v1 is old & superseded, v2 is current ACTIVE
        byte[] ov1 = "overwritten-v1".getBytes(StandardCharsets.UTF_8);
        byte[] ov2 = "current-v2".getBytes(StandardCharsets.UTF_8);
        localStorageService.store("docs/overwritten.txt", 1L, new ByteArrayInputStream(ov1));
        localStorageService.store("docs/overwritten.txt", 2L, new ByteArrayInputStream(ov2));
        Path ov1Path = localStorageService.getFilePath("docs/overwritten.txt", 1L);
        Path ov2Path = localStorageService.getFilePath("docs/overwritten.txt", 2L);

        saveObjectMeta("docs/overwritten.txt", 1L, ov1.length, ChecksumUtil.sha256(ov1), ObjectStatus.ACTIVE,
                OffsetDateTime.now().minusHours(48));
        saveReplica("docs/overwritten.txt", 1L, SELF_ID, ChecksumUtil.sha256(ov1));
        saveObjectMeta("docs/overwritten.txt", 2L, ov2.length, ChecksumUtil.sha256(ov2), ObjectStatus.ACTIVE,
                OffsetDateTime.now());
        saveReplica("docs/overwritten.txt", 2L, SELF_ID, ChecksumUtil.sha256(ov2));

        // 1. Non-leader must NOT run sweep (V-12)
        when(nodeState.isLeader()).thenReturn(false);
        garbageCollector.sweep();
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("docs/deleted.txt", 2L)).isPresent(),
                "Non-leader node must not run GC sweep");

        // 2. Leader sweep must clear replicas + disk files BEFORE objects delete without FK crash (V-04)
        when(nodeState.isLeader()).thenReturn(true);
        assertDoesNotThrow(() -> garbageCollector.sweep());

        assertFalse(Files.exists(tombV1Dat), "Tombstoned v1 physical file must be deleted by GC");
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("docs/deleted.txt", 1L)).isEmpty());
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("docs/deleted.txt", 2L)).isEmpty());

        assertFalse(Files.exists(ov1Path), "Superseded overwritten v1 physical file must be collected");
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("docs/overwritten.txt", 1L)).isEmpty());
        assertTrue(Files.exists(ov2Path), "Current ACTIVE v2 file must remain intact");
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("docs/overwritten.txt", 2L)).isPresent());
    }

    @Test
    void internalGcMustNotDeletePhysicalFileIfReplicaRowDeletionFails() throws Exception {
        LocalStorageService mockStorage = mock(LocalStorageService.class);
        ReplicaRepository failingRepo = mock(ReplicaRepository.class);
        doThrow(new DataAccessResourceFailureException("DB down"))
                .when(failingRepo).deleteById(any(ReplicaId.class));

        InternalController ctrl = new InternalController(
                mockStorage, failingRepo, nodeRegistryService, electionService, nodeState, objectIdValidator);
        ReflectionTestUtils.setField(ctrl, "nodeSecret", NODE_SECRET);

        var resp = ctrl.garbageCollectReplica("safe/keep.dat", 1L, NODE_SECRET);
        assertEquals(500, resp.getStatusCode().value());
        verifyNoInteractions(mockStorage);
    }

    @Test
    void queueRemovalClearsReplicaRowAndRemovesPhysicalFile() throws Exception {
        byte[] bytes = "extra-replica".getBytes(StandardCharsets.UTF_8);
        String hash = ChecksumUtil.sha256(bytes);
        localStorageService.store("rebalance/extra.bin", 1L, new ByteArrayInputStream(bytes));
        Path diskFile = localStorageService.getFilePath("rebalance/extra.bin", 1L);

        saveObjectMeta("rebalance/extra.bin", 1L, bytes.length, hash, ObjectStatus.ACTIVE, OffsetDateTime.now());
        Replica extra = saveReplica("rebalance/extra.bin", 1L, SELF_ID, hash);

        garbageCollector.queueRemoval(extra);

        assertTrue(replicaRepository.findById(new ReplicaId("rebalance/extra.bin", 1L, SELF_ID)).isEmpty(),
                "Extra replica DB row must be removed");
        assertFalse(Files.exists(diskFile), "queueRemoval must delete physical file after clearing DB row");
    }

    @Test
    void objectServiceDeleteRespectsTierAndSkipsNonExistentKey() {
        MetadataService metaMock = mock(MetadataService.class);
        ReplicationService repMock = mock(ReplicationService.class);
        NodeState stateMock = mock(NodeState.class);
        GarbageCollector gcMock = mock(GarbageCollector.class);
        when(stateMock.getCurrentTerm()).thenReturn(2L);

        ObjectService svc = new ObjectService(metaMock, repMock, stateMock, gcMock);

        // V-21: Deleting missing key must NOT write a tombstone
        when(metaMock.findLatestActive("missing/key.txt")).thenReturn(null);
        svc.delete("missing/key.txt");
        verify(metaMock, never()).claimVersion(eq("missing/key.txt"), anyLong(), anyString(), anyLong(), any());

        // V-05: Deleting CRITICAL object must claim & replicate tombstone using CRITICAL tier, then purge prior data
        ObjectMeta criticalMeta = new ObjectMeta();
        criticalMeta.setObjectId("crit/file.bin");
        criticalMeta.setVersion(1L);
        criticalMeta.setDurabilityTier(DurabilityTier.CRITICAL);
        criticalMeta.setStatus(ObjectStatus.ACTIVE);
        when(metaMock.findLatestActive("crit/file.bin")).thenReturn(criticalMeta);
        when(metaMock.claimVersion("crit/file.bin", 0L, "TOMBSTONE", 2L, DurabilityTier.CRITICAL)).thenReturn(2L);

        svc.delete("crit/file.bin");
        verify(metaMock).claimVersion("crit/file.bin", 0L, "TOMBSTONE", 2L, DurabilityTier.CRITICAL);
        verify(repMock).replicateTombstone("crit/file.bin", 2L, DurabilityTier.CRITICAL, 2L);
        verify(metaMock).commitTombstone("crit/file.bin", 2L);
        verify(gcMock).purgePriorVersions("crit/file.bin", 2L);
    }

    private ObjectMeta saveObjectMeta(String id, long ver, long size, String sha, ObjectStatus st, OffsetDateTime ts) {
        ObjectMeta m = new ObjectMeta();
        m.setObjectId(id);
        m.setVersion(ver);
        m.setSizeBytes(size);
        m.setChecksumSha256(sha);
        m.setDurabilityTier(DurabilityTier.STANDARD);
        m.setTerm(1L);
        m.setStatus(st);
        m.setCreatedAt(ts);
        ObjectMeta saved = objectMetaRepository.saveAndFlush(m);
        // Backdate past @PrePersist
        if (ts != null) {
            saved.setCreatedAt(ts);
            objectMetaRepository.saveAndFlush(saved);
        }
        return saved;
    }

    private Replica saveReplica(String id, long ver, UUID nodeId, String sha) {
        Replica r = new Replica();
        r.setObjectId(id);
        r.setVersion(ver);
        r.setNodeId(nodeId);
        r.setChecksumSha256(sha);
        r.setStatus(ReplicaStatus.HEALTHY);
        return replicaRepository.saveAndFlush(r);
    }

    private String extractQueryParam(String query, String name) {
        for (String part : query.split("&")) {
            int eq = part.indexOf('=');
            if (eq > 0 && part.substring(0, eq).equals(name)) {
                return java.net.URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
            }
        }
        return "";
    }
}
