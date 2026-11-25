package com.vault.node.service;

import com.vault.common.dto.WriteResult;
import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.common.util.ObjectKey;
import com.vault.node.TestEphemeralCredentials;
import com.vault.node.controller.InternalController;
import com.vault.node.controller.ObjectController;
import com.vault.node.controller.S3CompatibleController;
import com.vault.node.election.ElectionService;
import com.vault.node.election.NodeState;
import com.vault.node.metrics.VaultMetrics;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.Replica;
import com.vault.node.model.ReplicaId;
import com.vault.node.placement.ConsistentHashRing;
import com.vault.node.repository.ReplicaRepository;
import com.vault.node.security.ObjectIdValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Regression tests for Item 2:
 * - P0-03: slash-safe routes across /api/v1/objects, /s3, and /internal
 * - P0-04: collision-free ObjectKey storage layout for a/b vs a_b
 * - P0-05 & V-09: raw binary PUT contract rejecting multipart/form-data and form-urlencoded (415)
 * - V-08: hard max object size enforcement (413) for known and unknown Content-Length
 */
@WebMvcTest(
        controllers = {ObjectController.class, InternalController.class, S3CompatibleController.class},
        properties = {
                "vault.storage.max-object-size-bytes=128"
        }
)
@ContextConfiguration(initializers = TestEphemeralCredentials.class)
@Import({ObjectIdValidator.class, UploadSpooler.class, PeerObjectClient.class})
class ObjectIdentityUploadContractRegressionTest {

    private static final String API_KEY = TestEphemeralCredentials.clientKey();
    private static final String NODE_SECRET = TestEphemeralCredentials.nodeCredential();

    @Autowired
    private MockMvc mockMvc;

    @MockBean private LocalStorageService localStorageService;
    @MockBean private MetadataService metadataService;
    @MockBean private IdempotencyService idempotencyService;
    @MockBean private ObjectService objectService;
    @MockBean private NodeState nodeState;
    @MockBean private ConsistentHashRing ring;
    @MockBean private WebClient internalWebClient;
    @MockBean private ReadRepairService readRepairService;
    @MockBean private NodeRegistryService nodeRegistryService;
    @MockBean private AdaptiveConsistencyEngine adaptiveConsistencyEngine;
    @MockBean private CorruptionGenealogyTracker corruptionGenealogyTracker;
    @MockBean private VaultMetrics vaultMetrics;
    @MockBean private ReplicaRepository replicaRepository;
    @MockBean private ElectionService electionService;

    @BeforeEach
    void setUpMocks() throws Exception {
        UUID selfId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        when(nodeState.isLeader()).thenReturn(true);
        when(nodeState.getCurrentTerm()).thenReturn(1L);
        when(nodeRegistryService.getSelfNodeId()).thenReturn(selfId);
        when(idempotencyService.claimOrReplay(any())).thenReturn(IdempotencyService.ClaimResult.claimed());
        when(adaptiveConsistencyEngine.determineConsistency(any()))
                .thenReturn(com.vault.common.model.ConsistencyMode.EVENTUAL);
        when(localStorageService.getRelativePath(anyString(), anyLong()))
                .thenAnswer(inv -> ObjectKey.relativeStoragePath(inv.getArgument(0), inv.getArgument(1)));
        when(objectService.put(anyString(), any(Path.class), any(DurabilityTier.class)))
                .thenAnswer(inv -> {
                    Path p = inv.getArgument(1);
                    String sha = ChecksumUtil.sha256File(p);
                    long sz = java.nio.file.Files.size(p);
                    return new WriteResult(sha, sz);
                });
    }

    @Test
    void storageLayoutMustIsolateSlashAndUnderscoreKeys(@TempDir Path tempDir) throws Exception {
        LocalStorageService storage = new LocalStorageService();
        ReflectionTestUtils.setField(storage, "dataDirStr", tempDir.toString());
        storage.init();

        byte[] payload = "identical-content".getBytes(StandardCharsets.UTF_8);
        String checksum = ChecksumUtil.sha256(payload);

        storage.store("a/b", 1L, new ByteArrayInputStream(payload));
        storage.store("a_b", 1L, new ByteArrayInputStream(payload));

        Path slashPath = storage.getFilePath("a/b", 1L, checksum);
        Path underscorePath = storage.getFilePath("a_b", 1L, checksum);
        assertNotEquals(slashPath, underscorePath, "a/b and a_b must map to distinct files");

        storage.delete("a/b", 1L);
        assertTrue(storage.getFileSize("a_b", 1L, checksum) > 0,
                "Deleting a/b must not delete distinct object a_b");
    }

    @Test
    void restRoutesSupportSlashKeysViaPathAndQueryContract() throws Exception {
        byte[] body = "hello-slash".getBytes(StandardCharsets.UTF_8);

        mockMvc.perform(put("/api/v1/objects/reports/q3.pdf")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.objectId").value("reports/q3.pdf"));

        mockMvc.perform(put("/api/v1/objects")
                        .param("key", "reports/q3.pdf")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.objectId").value("reports/q3.pdf"));

        ObjectMeta meta = new ObjectMeta();
        meta.setObjectId("reports/q3.pdf");
        meta.setVersion(1L);
        meta.setSizeBytes((long) body.length);
        meta.setChecksumSha256(ChecksumUtil.sha256(body));
        meta.setDurabilityTier(DurabilityTier.STANDARD);
        meta.setStatus(ObjectStatus.ACTIVE);
        when(metadataService.findLatestActive("reports/q3.pdf")).thenReturn(meta);
        when(replicaRepository.findByObjectIdAndVersion("reports/q3.pdf", 1L)).thenReturn(List.of());

        mockMvc.perform(get("/api/v1/objects/reports/q3.pdf/inspect")
                        .header("X-API-Key", API_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectId").value("reports/q3.pdf"))
                .andExpect(jsonPath("$.storagePath").value(ObjectKey.relativeStoragePath("reports/q3.pdf", 1L)));

        mockMvc.perform(get("/api/v1/objects/inspect")
                        .param("key", "reports/q3.pdf")
                        .header("X-API-Key", API_KEY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.objectId").value("reports/q3.pdf"));

        mockMvc.perform(delete("/api/v1/objects/reports/q3.pdf")
                        .header("X-API-Key", API_KEY))
                .andExpect(status().isNoContent());

        mockMvc.perform(delete("/api/v1/objects")
                        .param("key", "reports/q3.pdf")
                        .header("X-API-Key", API_KEY))
                .andExpect(status().isNoContent());
    }

    @Test
    void internalRoutesSupportSlashKeysWithoutSingleSegmentBreakage() throws Exception {
        UUID selfId = UUID.fromString("11111111-1111-1111-1111-111111111111");
        Replica rep = new Replica();
        rep.setObjectId("reports/q3.pdf");
        rep.setVersion(1L);
        rep.setNodeId(selfId);
        rep.setChecksumSha256(ChecksumUtil.sha256("ok".getBytes(StandardCharsets.UTF_8)));

        when(replicaRepository.findById(new ReplicaId("reports/q3.pdf", 1L, selfId)))
                .thenReturn(Optional.of(rep));
        when(localStorageService.openVerifiedStream(eq("reports/q3.pdf"), eq(1L), anyString(), eq(-1L)))
                .thenReturn(new ByteArrayInputStream("ok".getBytes(StandardCharsets.UTF_8)));

        mockMvc.perform(get("/internal/objects")
                        .param("key", "reports/q3.pdf")
                        .param("version", "1")
                        .header("X-Vault-Node-Secret", NODE_SECRET))
                .andExpect(status().isOk());

        mockMvc.perform(delete("/internal/replicate")
                        .param("key", "reports/q3.pdf")
                        .param("version", "1")
                        .param("term", "1")
                        .header("X-Vault-Node-Secret", NODE_SECRET))
                .andExpect(status().isOk());

        mockMvc.perform(delete("/internal/gc")
                        .param("key", "reports/q3.pdf")
                        .param("version", "1")
                        .header("X-Vault-Node-Secret", NODE_SECRET))
                .andExpect(status().isOk());
    }

    @Test
    void putRejectsMultipartAndFormUrlEncodedWith415() throws Exception {
        byte[] fakeMultipart = ("--b\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n\r\nhi\r\n--b--\r\n")
                .getBytes(StandardCharsets.UTF_8);

        mockMvc.perform(put("/api/v1/objects/doc.txt")
                        .header("X-API-Key", API_KEY)
                        .contentType("multipart/form-data; boundary=b")
                        .content(fakeMultipart))
                .andExpect(status().isUnsupportedMediaType());

        mockMvc.perform(put("/api/v1/objects/doc.txt")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content("raw-text-via-curl-d"))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void putEnforcesHardMaxObjectSizeLimitWith413EvenWithoutContentLength() throws Exception {
        byte[] oversized = new byte[256]; // > 128 bytes configured limit

        mockMvc.perform(put("/api/v1/objects/oversized.bin")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content(oversized))
                .andExpect(status().isPayloadTooLarge());

        // Simulate chunked upload where Content-Length is unset (-1) so streaming byte guard must trigger 413
        mockMvc.perform(put("/api/v1/objects/oversized-chunked.bin")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .with(req -> {
                            req.setContent(oversized);
                            req.removeHeader("Content-Length");
                            return req;
                        }))
                .andExpect(status().isPayloadTooLarge());

        mockMvc.perform(put("/s3/bucket/oversized.bin")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content(oversized))
                .andExpect(status().isPayloadTooLarge());
    }

    @Test
    void invalidObjectKeysReturn400Not500() throws Exception {
        mockMvc.perform(put("/api/v1/objects")
                        .param("key", "bad//key.txt")
                        .header("X-API-Key", API_KEY)
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .content("abc".getBytes(StandardCharsets.UTF_8)))
                .andExpect(status().isBadRequest());
    }
}
