package com.vault.node;

import com.sun.net.httpserver.HttpServer;
import com.vault.common.model.NodeStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.node.election.NodeState;
import com.vault.node.model.Node;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.repository.ElectionRepository;
import com.vault.node.repository.NodeRepository;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.service.GarbageCollector;
import com.vault.node.service.LocalStorageService;
import com.vault.node.service.NodeRegistryService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

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
 * Real PostgreSQL 15 integration test backed by Flyway V1..V7 + Hibernate ddl-auto=validate.
 * Fails loudly when vault.postgres-it.required=true (or CI=true) if no Postgres/Docker is available.
 */
@EnabledIf("com.vault.node.PostgresTestTarget#shouldExecute")
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.task.scheduling.enabled=false",
                "spring.jpa.hibernate.ddl-auto=validate",
                "spring.flyway.enabled=true",
                "vault.node.id=11111111-1111-1111-1111-111111111111",
                "vault.node.address=127.0.0.1:8081",
                "vault.replication.timeout-ms=3000"
        }
)
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
class VaultPostgresIntegrationTest {

    private static PostgresTestTarget POSTGRES;
    private static Path tempDataDir;

    @BeforeAll
    static void initDataDir() throws Exception {
        POSTGRES = PostgresTestTarget.startOrFailIfRequired("vault");
        tempDataDir = Files.createTempDirectory("vault-pg-it-");
    }

    @AfterAll
    static void cleanupDataDir() throws Exception {
        if (POSTGRES != null) {
            POSTGRES.close();
        }
        if (tempDataDir != null && Files.exists(tempDataDir)) {
            try (var s = Files.walk(tempDataDir)) {
                s.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (Exception ignored) {}
                });
            }
        }
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("vault.node.data-dir", () -> tempDataDir.toAbsolutePath().toString());
    }

    private static final UUID SELF_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID PEER_2_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
    private static final String API_KEY = TestEphemeralCredentials.clientKey();
    private static final String NODE_SECRET = TestEphemeralCredentials.nodeCredential();

    @Autowired private TestRestTemplate restTemplate;
    @Autowired private ElectionRepository electionRepository;
    @Autowired private NodeRepository nodeRepository;
    @Autowired private ObjectMetaRepository objectMetaRepository;
    @Autowired private NamedParameterJdbcTemplate jdbcTemplate;
    @Autowired private NodeRegistryService nodeRegistryService;
    @Autowired private LocalStorageService localStorageService;
    @Autowired private GarbageCollector garbageCollector;
    @Autowired private NodeState nodeState;

    private HttpServer peerServer;
    private volatile byte[] peerBytes = new byte[0];

    @BeforeEach
    void setUp() throws Exception {
        peerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        peerServer.createContext("/internal/replicate", ex -> {
            if ("POST".equals(ex.getRequestMethod())) {
                peerBytes = ex.getRequestBody().readAllBytes();
            }
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        peerServer.createContext("/internal/gc", ex -> {
            ex.sendResponseHeaders(200, -1);
            ex.close();
        });
        peerServer.createContext("/internal/objects", ex -> {
            ex.sendResponseHeaders(200, peerBytes.length);
            try (var os = ex.getResponseBody()) {
                os.write(peerBytes);
            }
        });
        peerServer.start();

        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM idempotency_keys");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM replicas");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM objects");
        jdbcTemplate.getJdbcTemplate().execute("UPDATE election_state SET leader_id = NULL, current_term = 0, lease_expires = NULL WHERE id = 1");
        jdbcTemplate.getJdbcTemplate().execute("DELETE FROM nodes");

        Node self = new Node();
        self.setNodeId(SELF_ID);
        self.setAddress("127.0.0.1:8081");
        self.setStatus(NodeStatus.UP);
        nodeRepository.saveAndFlush(self);

        Node peer = new Node();
        peer.setNodeId(PEER_2_ID);
        peer.setAddress("127.0.0.1:" + peerServer.getAddress().getPort());
        peer.setStatus(NodeStatus.UP);
        nodeRepository.saveAndFlush(peer);

        nodeRegistryService.refreshRing();
    }

    @AfterEach
    void tearDown() {
        if (peerServer != null) {
            peerServer.stop(0);
        }
    }

    @Test
    void postgresFlywayConstraintsElectionFencingIdempotencyAndGcWorkEndToEnd() throws Exception {
        // 1. V6 Flyway CHECK constraint permits persisting DEGRADED in real PostgreSQL
        nodeRegistryService.markNodeDegraded(PEER_2_ID.toString());
        assertEquals(NodeStatus.DEGRADED, nodeRepository.findById(PEER_2_ID).orElseThrow().getStatus());
        nodeRegistryService.markNodeUp(PEER_2_ID);

        // 2. Real Postgres RETURNING lease election + contention + fencing
        Long term1 = electionRepository.tryClaimLeadership(SELF_ID);
        assertNotNull(term1);
        assertNull(electionRepository.tryClaimLeadership(PEER_2_ID),
                "Active lease must block other candidate in Postgres");
        assertTrue(electionRepository.verifyAndFenceLeaderLease(SELF_ID, term1));
        nodeState.setLeaderId(SELF_ID, term1);

        // 3. Raw PUT -> Postgres explicit INSERT, fenced commit, JSONB idempotency replay
        byte[] data = "real-postgres-testcontainers-payload".getBytes(StandardCharsets.UTF_8);
        String expectedSha = ChecksumUtil.sha256(data);

        HttpHeaders headers = new HttpHeaders();
        headers.set("X-API-Key", API_KEY);
        headers.set("X-Idempotency-Key", "pg-idem-1");
        headers.set("X-Vault-Durability", "STANDARD");
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        ResponseEntity<Map> putResp = restTemplate.exchange(
                "/api/v1/objects?key=pg/folder/item.bin",
                HttpMethod.PUT,
                new HttpEntity<>(data, headers),
                Map.class);
        assertEquals(HttpStatus.CREATED, putResp.getStatusCode());
        assertEquals(expectedSha, putResp.getBody().get("checksum"));

        ResponseEntity<Map> replayResp = restTemplate.exchange(
                "/api/v1/objects?key=pg/folder/item.bin",
                HttpMethod.PUT,
                new HttpEntity<>(data, headers),
                Map.class);
        assertEquals(HttpStatus.CREATED, replayResp.getStatusCode());
        assertEquals("true", replayResp.getHeaders().getFirst("X-Idempotency-Replayed"));

        // 4. Expired lease causes HTTP 409 StaleLeaderException and does not commit v2 ACTIVE
        jdbcTemplate.update(
                "UPDATE election_state SET lease_expires = :expired WHERE id = 1",
                new MapSqlParameterSource("expired", Timestamp.from(Instant.now().minusSeconds(10))));

        HttpHeaders putV2Headers = new HttpHeaders();
        putV2Headers.set("X-API-Key", API_KEY);
        putV2Headers.set("X-Idempotency-Key", "pg-idem-v2");
        putV2Headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);

        ResponseEntity<String> stalePut = restTemplate.exchange(
                "/api/v1/objects?key=pg/folder/item.bin",
                HttpMethod.PUT,
                new HttpEntity<>(data, putV2Headers),
                String.class);
        assertEquals(HttpStatus.CONFLICT, stalePut.getStatusCode());
        assertTrue(objectMetaRepository.findById(new ObjectMetaId("pg/folder/item.bin", 2L)).isEmpty());

        // Re-claim leadership in term 2 and overwrite cleanly (creating ACTIVE v2)
        Long term2 = electionRepository.tryClaimLeadership(SELF_ID);
        assertNotNull(term2);
        nodeState.setLeaderId(SELF_ID, term2);

        // 5. Delete creates tombstone -> backdate -> GarbageCollector.sweep() deletes replicas BEFORE objects row cleanly
        HttpHeaders auth = new HttpHeaders();
        auth.set("X-API-Key", API_KEY);
        ResponseEntity<Void> delResp = restTemplate.exchange(
                "/api/v1/objects?key=pg/folder/item.bin",
                HttpMethod.DELETE,
                new HttpEntity<>(auth),
                Void.class);
        assertEquals(HttpStatus.NO_CONTENT, delResp.getStatusCode());

        var tombMeta = objectMetaRepository.findById(new ObjectMetaId("pg/folder/item.bin", 2L)).orElseThrow();
        assertEquals("TOMBSTONE", tombMeta.getChecksumSha256(),
                "V7 VARCHAR(64) schema must persist exact unpadded TOMBSTONE marker");

        jdbcTemplate.update(
                "UPDATE objects SET created_at = NOW() - INTERVAL '100 hours' WHERE object_id = 'pg/folder/item.bin'",
                new MapSqlParameterSource());
        assertDoesNotThrow(() -> garbageCollector.sweep());
        assertTrue(objectMetaRepository.findByObjectId("pg/folder/item.bin").isEmpty(),
                "Postgres GC sweep must cleanly clear all replicas & objects without FK error");
    }
}
