package com.vault.node.service;

import com.sun.net.httpserver.HttpServer;
import com.vault.common.model.ConsistencyMode;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.NodeStatus;
import com.vault.common.model.ObjectStatus;
import com.vault.common.model.ReplicaStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.config.WebClientConfig;
import com.vault.node.controller.InternalController;
import com.vault.node.controller.ObjectController;
import com.vault.node.election.ElectionService;
import com.vault.node.election.NodeState;
import com.vault.node.metrics.VaultMetrics;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.ObjectIdValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * Regression tests for Item 3:
 * - P0-01: never serve unverified bytes (local or internal peer GET) before SHA-256 verification
 * - P0-02: ReadRepair stages peer stream in temp, verifies SHA-256 BEFORE installing
 * - V-01: transparent peer fallback on missing/corrupt local replica at R=1 and ring shifts
 * - V-02: peer transfers > 256 KB must stream without DataBufferLimitException
 * - V-03: ReadRepair push sends current NodeState term, not hardcoded "1"
 */
class VerifiedReadPeerFallbackRegressionTest {

    private static final String NODE_SECRET = TestEphemeralCredentials.nodeCredential();
    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID PEER_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    @TempDir
    Path tempDir;

    private LocalStorageService localStorageService;
    private MetadataService metadataService;
    private ObjectIdValidator validator;
    private IdempotencyService idempotencyService;
    private ObjectService objectService;
    private UploadSpooler uploadSpooler;
    private NodeState nodeState;
    private ConsistentHashRing ring;
    private WebClient webClient;
    private PeerObjectClient peerObjectClient;
    private ReadRepairService readRepairService;
    private NodeRegistryService nodeRegistryService;
    private AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    private CorruptionGenealogyTracker corruptionGenealogyTracker;
    private VaultMetrics vaultMetrics;
    private ReplicaRepository replicaRepository;
    private NodeRepository nodeRepository;
    private ElectionService electionService;

    private ObjectController objectController;
    private InternalController internalController;
    private HttpServer peerServer;

    @BeforeEach
    void setUp() throws Exception {
        localStorageService = new LocalStorageService();
        ReflectionTestUtils.setField(localStorageService, "dataDirStr", tempDir.toString());
        localStorageService.init();

        metadataService = mock(MetadataService.class);
        validator = new ObjectIdValidator();
        idempotencyService = mock(IdempotencyService.class);
        objectService = mock(ObjectService.class);
        uploadSpooler = new UploadSpooler(134217728L);
        nodeState = mock(NodeState.class);
        ring = new ConsistentHashRing(10);
        webClient = new WebClientConfig().internalWebClient();
        nodeRegistryService = mock(NodeRegistryService.class);
        adaptiveConsistencyEngine = mock(AdaptiveConsistencyEngine.class);
        corruptionGenealogyTracker = mock(CorruptionGenealogyTracker.class);
        vaultMetrics = mock(VaultMetrics.class);
        replicaRepository = mock(ReplicaRepository.class);
        nodeRepository = mock(NodeRepository.class);
        electionService = mock(ElectionService.class);

        when(nodeRegistryService.getSelfNodeId()).thenReturn(SELF_ID);
        when(nodeState.getCurrentTerm()).thenReturn(4L);
        when(adaptiveConsistencyEngine.determineConsistency(any())).thenReturn(ConsistencyMode.EVENTUAL);

        peerObjectClient = new PeerObjectClient(webClient, NODE_SECRET, 134217728L);

        readRepairService = new ReadRepairService(
                replicaRepository, nodeRepository, localStorageService,
                nodeRegistryService, webClient, peerObjectClient, nodeState, vaultMetrics
        );
        ReflectionTestUtils.setField(readRepairService, "nodeSecret", NODE_SECRET);

        objectController = new ObjectController(
                localStorageService, metadataService, validator, idempotencyService,
                objectService, uploadSpooler, peerObjectClient, nodeState, ring, webClient,
                readRepairService, nodeRegistryService, adaptiveConsistencyEngine,
                corruptionGenealogyTracker, vaultMetrics, replicaRepository
        );
        ReflectionTestUtils.setField(objectController, "nodeSecret", NODE_SECRET);

        internalController = new InternalController(
                localStorageService, replicaRepository, nodeRegistryService,
                electionService, nodeState, validator
        );
        ReflectionTestUtils.setField(internalController, "nodeSecret", NODE_SECRET);
    }

    @AfterEach
    void tearDown() {
        if (peerServer != null) {
            peerServer.stop(0);
        }
    }

    @Test
    void localAndInternalGetMustNotStreamAnyByteOnSameSizeCorruptFile() throws Exception {
        byte[] good = "GOOD-PAYLOAD-DATA-12".getBytes(StandardCharsets.UTF_8);
        String checksum = ChecksumUtil.sha256(good);
        localStorageService.store("reports/secret.txt", 1L, new ByteArrayInputStream(good));

        // Corrupt 1 byte IN PLACE (same 20-byte length!)
        Path stored = localStorageService.getFilePath("reports/secret.txt", 1L, checksum);
        byte[] corrupted = good.clone();
        corrupted[0] = 'X';
        Files.write(stored, corrupted);

        ObjectMeta meta = activeMeta("reports/secret.txt", 1L, good.length, checksum, DurabilityTier.TEMPORARY);
        when(metadataService.findLatestActive("reports/secret.txt")).thenReturn(meta);
        when(replicaRepository.findByObjectIdAndVersion("reports/secret.txt", 1L)).thenReturn(List.of());

        MockMvc mockMvc = MockMvcBuilders.standaloneSetup(objectController, internalController).build();
        MvcResult restResult = mockMvc.perform(get("/api/v1/objects").param("key", "reports/secret.txt"))
                .andReturn();
        if (restResult.getRequest().isAsyncStarted()) {
            try {
                restResult.getAsyncResult(2000);
            } catch (Exception ignored) {}
        }
        String restBody = restResult.getResponse().getContentAsString();
        assertNotEquals(200, restResult.getResponse().getStatus(),
                "GET must not return 200 OK when local file fails checksum verification and no healthy peer exists");
        assertFalse(restBody.contains("XOOD-PAYLOAD"),
                "Corrupt bytes must never be written to client response before checksum verification");

        // Internal GET must also verify checksum before serving to peers
        Replica selfReplica = new Replica();
        selfReplica.setObjectId("reports/secret.txt");
        selfReplica.setVersion(1L);
        selfReplica.setNodeId(SELF_ID);
        selfReplica.setChecksumSha256(checksum);
        when(replicaRepository.findById(new ReplicaId("reports/secret.txt", 1L, SELF_ID)))
                .thenReturn(Optional.of(selfReplica));

        var internalResp = internalController.getInternalObject("reports/secret.txt", 1L, NODE_SECRET);
        if (internalResp.getStatusCode().is2xxSuccessful() && internalResp.getBody() != null) {
            byte[] internalBytes = readBody(internalResp.getBody());
            assertFalse(new String(internalBytes, StandardCharsets.UTF_8).contains("XOOD-PAYLOAD"),
                    "Internal peer GET must never stream corrupt bytes");
            fail("Internal peer GET should reject corrupt local file before returning 200 OK");
        }
    }

    @Test
    void getFallsBackToPeerWhenLocalMissingOrCorruptForLargePayloadAtR1() throws Exception {
        // 350 KB payload (> WebClient default 256 KB limit)
        byte[] payload = new byte[350 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) ('A' + (i % 23));
        }
        String checksum = ChecksumUtil.sha256(payload);

        peerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peerServer.createContext("/internal/objects", exchange -> {
            byte[] secretHeader = exchange.getRequestHeaders().getFirst("X-Vault-Node-Secret").getBytes(StandardCharsets.UTF_8);
            if (!NODE_SECRET.equals(new String(secretHeader, StandardCharsets.UTF_8))) {
                exchange.sendResponseHeaders(401, -1);
                return;
            }
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(payload);
            }
        });
        peerServer.start();
        String peerAddress = "127.0.0.1:" + peerServer.getAddress().getPort();

        Node peerNode = new Node();
        peerNode.setNodeId(PEER_ID);
        peerNode.setAddress(peerAddress);
        peerNode.setStatus(NodeStatus.UP);
        ring.rebuild(List.of(peerNode));

        Replica peerRep = new Replica();
        peerRep.setObjectId("video/large.bin");
        peerRep.setVersion(1L);
        peerRep.setNodeId(PEER_ID);
        peerRep.setChecksumSha256(checksum);
        peerRep.setStatus(ReplicaStatus.HEALTHY);

        ObjectMeta meta = activeMeta("video/large.bin", 1L, payload.length, checksum, DurabilityTier.STANDARD);
        when(metadataService.findLatestActive("video/large.bin")).thenReturn(meta);
        when(replicaRepository.findByObjectIdAndVersion("video/large.bin", 1L)).thenReturn(List.of(peerRep));
        when(nodeRegistryService.findNode(PEER_ID)).thenReturn(Optional.of(peerNode));
        // Note: intentionally test ReplicaRepository-based discovery even before ring rebuild or with empty ring
        ring.rebuild(List.of());

        var response = objectController.getObject("video/large.bin", new MockHttpServletResponse());
        assertEquals(200, response.getStatusCode().value(),
                "R=1 read on node without local replica must fall back transparently to healthy peer");
        byte[] received = readBody(response.getBody());
        assertArrayEquals(payload, received, "350 KB peer fallback payload must match byte-for-byte");

        // Also test corrupt local replica -> transparent fallback to healthy peer (>256 KB)
        localStorageService.store("video/large.bin", 1L, new ByteArrayInputStream(payload));
        Path localPath = localStorageService.getFilePath("video/large.bin", 1L);
        byte[] corruptLocal = payload.clone();
        corruptLocal[0] ^= 0x01;
        Files.write(localPath, corruptLocal);

        var healedResponse = objectController.getObject("video/large.bin", new MockHttpServletResponse());
        assertEquals(200, healedResponse.getStatusCode().value(),
                "GET must transparently fall back to healthy peer when local copy fails pre-verification");
        assertArrayEquals(payload, readBody(healedResponse.getBody()));
    }

    @Test
    void readRepairStagesAndRejectsCorruptPeerWithoutOverwritingLocalReplica() throws Exception {
        byte[] badPeerPayload = new byte[300 * 1024];
        String expectedGoodChecksum = ChecksumUtil.sha256("other-good-data".getBytes(StandardCharsets.UTF_8));

        peerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peerServer.createContext("/internal/objects", exchange -> {
            exchange.sendResponseHeaders(200, badPeerPayload.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(badPeerPayload);
            }
        });
        peerServer.start();

        Node badSource = new Node();
        badSource.setNodeId(PEER_ID);
        badSource.setAddress("127.0.0.1:" + peerServer.getAddress().getPort());
        badSource.setStatus(NodeStatus.UP);

        Replica candidateSource = new Replica();
        candidateSource.setObjectId("corrupt/repair.bin");
        candidateSource.setVersion(1L);
        candidateSource.setNodeId(PEER_ID);
        candidateSource.setChecksumSha256(expectedGoodChecksum);
        candidateSource.setStatus(ReplicaStatus.HEALTHY);

        when(replicaRepository.findByObjectIdAndVersion("corrupt/repair.bin", 1L))
                .thenReturn(List.of(candidateSource));
        when(nodeRepository.findById(PEER_ID)).thenReturn(Optional.of(badSource));

        readRepairService.repair("corrupt/repair.bin", 1L, SELF_ID);

        assertFalse(Files.exists(localStorageService.getFilePath("corrupt/repair.bin", 1L)),
                "Failed peer checksum during repair must never install an orphan or corrupt file into objectsDir");
        assertTrue(localStorageService.listAllDataFiles().isEmpty(),
                "Staging verification failure must leave zero files in objectsDir");
    }

    @Test
    void readRepairStagesVerifiesBeforeInstallAndSendsCurrentTermOver256Kb() throws Exception {
        byte[] payload = new byte[350 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0x7F);
        }
        String checksum = ChecksumUtil.sha256(payload);

        AtomicReference<String> receivedTerm = new AtomicReference<>();
        AtomicReference<byte[]> receivedPushBytes = new AtomicReference<>();

        peerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peerServer.createContext("/internal/objects", exchange -> {
            exchange.sendResponseHeaders(200, payload.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(payload);
            }
        });
        peerServer.createContext("/internal/replicate", exchange -> {
            receivedTerm.set(exchange.getRequestHeaders().getFirst("X-Vault-Term"));
            receivedPushBytes.set(exchange.getRequestBody().readAllBytes());
            exchange.sendResponseHeaders(200, -1);
        });
        peerServer.start();
        String addr = "127.0.0.1:" + peerServer.getAddress().getPort();

        Node sourceNode = new Node();
        sourceNode.setNodeId(SELF_ID);
        sourceNode.setAddress(addr);
        sourceNode.setStatus(NodeStatus.UP);

        Node targetNode = new Node();
        targetNode.setNodeId(PEER_ID);
        targetNode.setAddress(addr);
        targetNode.setStatus(NodeStatus.UP);

        Replica healthySource = new Replica();
        healthySource.setObjectId("large/rebalance.bin");
        healthySource.setVersion(1L);
        healthySource.setNodeId(SELF_ID);
        healthySource.setChecksumSha256(checksum);
        healthySource.setStatus(ReplicaStatus.HEALTHY);

        when(replicaRepository.findByObjectIdAndVersion("large/rebalance.bin", 1L))
                .thenReturn(List.of(healthySource));
        when(nodeRepository.findById(SELF_ID)).thenReturn(Optional.of(sourceNode));
        when(nodeRepository.findById(PEER_ID)).thenReturn(Optional.of(targetNode));
        when(nodeState.getCurrentTerm()).thenReturn(4L);

        readRepairService.repair("large/rebalance.bin", 1L, PEER_ID);

        assertEquals("4", receivedTerm.get(),
                "ReadRepair must forward current NodeState term instead of hardcoded '1'");
        assertArrayEquals(payload, receivedPushBytes.get(),
                "ReadRepair must stream >256 KB replica without DataBufferLimitException");
    }

    private ObjectMeta activeMeta(String objectId, long version, long size, String checksum, DurabilityTier tier) {
        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId(objectId);
        meta.setVersion(version);
        meta.setSizeBytes(size);
        meta.setChecksumSha256(checksum);
        meta.setDurabilityTier(tier);
        meta.setTerm(4L);
        meta.setStatus(ObjectStatus.ACTIVE);
        return meta;
    }

    private byte[] readBody(Object body) throws Exception {
        if (body instanceof StreamingResponseBody srb) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            srb.writeTo(out);
            return out.toByteArray();
        }
        if (body instanceof org.springframework.core.io.Resource res) {
            try (InputStream is = res.getInputStream()) {
                return is.readAllBytes();
            }
        }
        if (body instanceof byte[] bytes) {
            return bytes;
        }
        return new byte[0];
    }
}
