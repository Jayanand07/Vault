package com.vault.node;

import com.sun.net.httpserver.HttpServer;
import com.vault.common.model.NodeStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.ElectionService;
import com.vault.node.election.HeartbeatService;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.model.Replica;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.RateLimitFilter;
import com.vault.node.service.LocalStorageService;
import com.vault.node.service.NodeRegistryService;
import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end single-JVM multi-node integration test exercising real Servlet filters,
 * HTTP controllers, bounded streaming, early quorum, verified fallback reads, and tombstones.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:vault_e2e_test;MODE=PostgreSQL;NON_KEYWORDS=KEY;DB_CLOSE_DELAY=-1",
                "spring.datasource.username=sa",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.flyway.enabled=false",
                "spring.jpa.hibernate.ddl-auto=create-drop",
                "vault.node.id=11111111-1111-1111-1111-111111111111",
                "vault.node.address=127.0.0.1:8081",
                "vault.node.data-dir=${java.io.tmpdir}/vault-e2e-init",
                "vault.security.rate-limit-write-per-sec=50",
                "vault.security.rate-limit-read-per-sec=100",
                "vault.election.election-timeout-min-ms=8500",
                "vault.election.election-timeout-max-ms=9500",
                "vault.replication.timeout-ms=3000"
        }
)
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
class VaultIntegrationTest {

    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID PEER_2_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final UUID PEER_3_ID = UUID.fromString("33333333-3333-3333-3333-333333333333");
    private static final String API_KEY = TestEphemeralCredentials.clientKey();

    @TempDir
    Path tempDir;

    @Autowired private TestRestTemplate restTemplate;
    @Autowired private NodeRepository nodeRepository;
    @Autowired private ObjectMetaRepository objectMetaRepository;
    @Autowired private ReplicaRepository replicaRepository;
    @Autowired private NamedParameterJdbcTemplate jdbcTemplate;
    @Autowired private LocalStorageService localStorageService;
    @Autowired private NodeRegistryService nodeRegistryService;
    @Autowired private NodeState nodeState;
    @Autowired private ElectionService electionService;

    // Mock only HeartbeatService so @Scheduled ticks do not run Postgres RETURNING SQL against H2
    @MockBean private HeartbeatService heartbeatService;

    private HttpServer peer2;
    private HttpServer peer3;
    private volatile byte[] peer2Bytes = new byte[0];
    private volatile byte[] peer3Bytes = new byte[0];
    private volatile boolean delayPeer3 = false;

    @BeforeEach
    void setUp() throws Exception {
        ReflectionTestUtils.setField(localStorageService, "dataDirStr", tempDir.toString());
        localStorageService.init();

        peer2 = createPeerServer(PEER_2_ID, bytes -> peer2Bytes = bytes, () -> peer2Bytes, () -> false);
        peer3 = createPeerServer(PEER_3_ID, bytes -> peer3Bytes = bytes, () -> peer3Bytes, () -> delayPeer3);
        peer2.start();
        peer3.start();

        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM idempotency_keys");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM replicas");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM objects");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM election_state");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM nodes");

        saveNode(SELF_ID, "127.0.0.1:8081", NodeStatus.UP);
        saveNode(PEER_2_ID, "127.0.0.1:" + peer2.getAddress().getPort(), NodeStatus.UP);
        saveNode(PEER_3_ID, "127.0.0.1:" + peer3.getAddress().getPort(), NodeStatus.UP);
        nodeRegistryService.refreshRing();

        jdbcTemplate.update(
                "INSERT INTO election_state (id, current_term, leader_id, lease_expires, updated_at) " +
                        "VALUES (1, 1, :leader, :expires, CURRENT_TIMESTAMP)",
                new MapSqlParameterSource()
                        .addValue("leader", SELF_ID)
                        .addValue("expires", Timestamp.from(Instant.now().plusSeconds(60))));
        nodeState.setLeaderId(SELF_ID, 1L);
    }

    @AfterEach
    void tearDown() {
        if (peer2 != null) peer2.stop(0);
        if (peer3 != null) peer3.stop(0);
    }

    private HttpServer createPeerServer(
            UUID peerId,
            java.util.function.Consumer<byte[]> storeConsumer,
            java.util.function.Supplier<byte[]> loadSupplier,
            java.util.function.BooleanSupplier shouldDelay) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/internal/replicate", ex -> {
            if (shouldDelay.getAsBoolean()) {
                try {
                    Thread.sleep(4500);
                } catch (InterruptedException ignored) {}
                ex.sendResponseHeaders(500, -1);
                ex.close();
                return;
            }
            if ("POST".equals(ex.getRequestMethod())) {
                byte[] body = ex.getRequestBody().readAllBytes();
                storeConsumer.accept(body);
                String objectId = ex.getRequestHeaders().getFirst("X-Vault-Object-Id");
                long version = Long.parseLong(ex.getRequestHeaders().getFirst("X-Vault-Version"));
                String checksum = ex.getRequestHeaders().getFirst("X-Vault-Checksum");
                Replica rep = new Replica();
                rep.setObjectId(objectId);
                rep.setVersion(version);
                rep.setNodeId(peerId);
                rep.setChecksumSha256(checksum);
                replicaRepository.saveAndFlush(rep);
            }
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        server.createContext("/internal/objects", ex -> {
            byte[] data = loadSupplier.get();
            ex.sendResponseHeaders(200, data.length);
            try (var os = ex.getResponseBody()) {
                os.write(data);
            }
        });
        return server;
    }

    @Test
    void testFullWriteReadRepairInspectAndDeleteCycle() throws Exception {
        // 0. Verify ElectionService & RateLimitFilter config wiring (V-15)
        Integer timeoutMs = (Integer) ReflectionTestUtils.getField(electionService, "currentElectionTimeoutMs");
        assertNotNull(timeoutMs);
        assertTrue(timeoutMs >= 8500 && timeoutMs < 9500,
                "Configured min/max election timeout must apply cleanly (got " + timeoutMs + ")");

        RateLimitFilter filter = new RateLimitFilter(1, 3);
        FilterChain okChain = (req, res) -> {};
        for (int i = 0; i < 3; i++) {
            MockHttpServletRequest getReq = new MockHttpServletRequest("GET", "/api/v1/objects");
            MockHttpServletResponse getRes = new MockHttpServletResponse();
            filter.doFilter(getReq, getRes, okChain);
            assertEquals(200, getRes.getStatus(), "GET within read limit must pass");
        }
        MockHttpServletRequest write1 = new MockHttpServletRequest("PUT", "/api/v1/objects");
        MockHttpServletResponse writeRes1 = new MockHttpServletResponse();
        filter.doFilter(write1, writeRes1, okChain);
        assertEquals(200, writeRes1.getStatus());

        MockHttpServletRequest write2 = new MockHttpServletRequest("PUT", "/api/v1/objects");
        MockHttpServletResponse writeRes2 = new MockHttpServletResponse();
        filter.doFilter(write2, writeRes2, okChain);
        assertEquals(429, writeRes2.getStatus(), "Second PUT exceeding write limit=1/s must return 429");

        // 1. Unauthenticated request blocked
        ResponseEntity<String> unauth = restTemplate.getForEntity("/api/v1/cluster/status", String.class);
        assertEquals(HttpStatus.UNAUTHORIZED, unauth.getStatusCode());

        // 2. Early quorum PUT completes quickly even while peer3 stalls
        delayPeer3 = true;
        byte[] earlyData = "early-quorum-e2e".getBytes(StandardCharsets.UTF_8);
        HttpHeaders earlyHeaders = new HttpHeaders();
        earlyHeaders.set("X-API-Key", API_KEY);
        earlyHeaders.set("X-Idempotency-Key", "e2e-early-1");
        earlyHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        long startMs = System.currentTimeMillis();
        ResponseEntity<Map> earlyResp = restTemplate.exchange(
                "/api/v1/objects?key=e2e/early.txt",
                HttpMethod.PUT,
                new HttpEntity<>(earlyData, earlyHeaders),
                Map.class);
        long elapsedMs = System.currentTimeMillis() - startMs;
        assertEquals(HttpStatus.CREATED, earlyResp.getStatusCode());
        assertTrue(elapsedMs < 2000L, "Early quorum should finish without waiting for hung peer3 (took " + elapsedMs + "ms)");
        delayPeer3 = false;

        // 3. Full 3-copy upload with X-Idempotency-Key replay
        byte[] payload = "e2e-distributed-vault-payload-data".getBytes(StandardCharsets.UTF_8);
        String expectedSha = ChecksumUtil.sha256(payload);

        HttpHeaders putHeaders = new HttpHeaders();
        putHeaders.set("X-API-Key", API_KEY);
        putHeaders.set("X-Idempotency-Key", "e2e-idem-1");
        putHeaders.set("X-Vault-Durability", "STANDARD");
        putHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        ResponseEntity<Map> putResp = restTemplate.exchange(
                "/api/v1/objects?key=e2e/folder/doc.txt",
                HttpMethod.PUT,
                new HttpEntity<>(payload, putHeaders),
                Map.class);
        assertEquals(HttpStatus.CREATED, putResp.getStatusCode());
        assertEquals(expectedSha, putResp.getBody().get("checksum"));

        ResponseEntity<Map> replayResp = restTemplate.exchange(
                "/api/v1/objects?key=e2e/folder/doc.txt",
                HttpMethod.PUT,
                new HttpEntity<>(payload, putHeaders),
                Map.class);
        assertEquals(HttpStatus.CREATED, replayResp.getStatusCode());
        assertEquals("true", replayResp.getHeaders().getFirst("X-Idempotency-Replayed"));
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("e2e/folder/doc.txt", 2L)).isEmpty());

        // 4. Corrupt local disk payload -> GET refuses corrupt bytes & streams verified copy across peers
        Path localFile = localStorageService.getFilePath("e2e/folder/doc.txt", 1L);
        byte[] corrupt = "X2e-distributed-vault-payload-data".getBytes(StandardCharsets.UTF_8);
        assertEquals(payload.length, corrupt.length);
        Files.write(localFile, corrupt);

        HttpHeaders authHeaders = new HttpHeaders();
        authHeaders.set("X-API-Key", API_KEY);
        ResponseEntity<byte[]> getResp = restTemplate.exchange(
                "/api/v1/objects/e2e/folder/doc.txt",
                HttpMethod.GET,
                new HttpEntity<>(authHeaders),
                byte[].class);
        assertEquals(HttpStatus.OK, getResp.getStatusCode());
        assertArrayEquals(payload, getResp.getBody());

        // 5. Inspect returns expected fields
        ResponseEntity<Map> inspectResp = restTemplate.exchange(
                "/api/v1/objects/inspect?key=e2e/folder/doc.txt",
                HttpMethod.GET,
                new HttpEntity<>(authHeaders),
                Map.class);
        assertEquals(HttpStatus.OK, inspectResp.getStatusCode());
        assertEquals("ACTIVE", inspectResp.getBody().get("status"));

        // 6. Delete -> 204, subsequent GET -> 404
        ResponseEntity<Void> delResp = restTemplate.exchange(
                "/api/v1/objects?key=e2e/folder/doc.txt",
                HttpMethod.DELETE,
                new HttpEntity<>(authHeaders),
                Void.class);
        assertEquals(HttpStatus.NO_CONTENT, delResp.getStatusCode());

        ResponseEntity<String> afterDeleteGet = restTemplate.exchange(
                "/api/v1/objects?key=e2e/folder/doc.txt",
                HttpMethod.GET,
                new HttpEntity<>(authHeaders),
                String.class);
        assertEquals(HttpStatus.NOT_FOUND, afterDeleteGet.getStatusCode());
    }

    private void saveNode(UUID id, String addr, NodeStatus status) {
        Node n = new Node();
        n.setNodeId(id);
        n.setAddress(addr);
        n.setStatus(status);
        nodeRepository.saveAndFlush(n);
    }
}
