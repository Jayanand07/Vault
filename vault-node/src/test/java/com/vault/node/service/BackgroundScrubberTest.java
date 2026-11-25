package com.vault.node.service;

import com.vault.common.model.ObjectStatus;
import com.vault.common.util.ChecksumUtil;
import com.vault.common.util.ObjectKey;
import com.vault.node.model.ObjectMeta;
import com.vault.node.model.ObjectMetaId;
import com.vault.node.model.Replica;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.repository.ReplicaRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BackgroundScrubberTest {

    @Mock private LocalStorageService localStorageService;
    @Mock private ReplicaRepository replicaRepository;
    @Mock private NodeRegistryService nodeRegistryService;
    @Mock private ReadRepairService readRepairService;
    @Mock private CorruptionGenealogyTracker corruptionGenealogyTracker;
    @Mock private ObjectMetaRepository objectMetaRepository;

    @InjectMocks
    private BackgroundScrubber backgroundScrubber;

    private Path tempDir;
    private Path tempFile;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("vault-scrubber-test");
        // Collision-free filename pattern <sha256(objectId)>.v1.dat
        tempFile = tempDir.resolve(ObjectKey.storageFileName("test-obj", 1L));
        Files.writeString(tempFile, "test data");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(tempFile)) Files.delete(tempFile);
        if (Files.exists(tempDir)) Files.delete(tempDir);
    }

    @Test
    void testScrubberDeletesTombstonedReplica() throws Exception {
        UUID selfNodeId = UUID.randomUUID();
        when(nodeRegistryService.getSelfNodeId()).thenReturn(selfNodeId);
        when(localStorageService.listAllDataFiles()).thenReturn(List.of(tempFile));

        String checksum = ChecksumUtil.sha256("test data".getBytes());
        
        Replica replica = new Replica();
        replica.setObjectId("test-obj");
        replica.setVersion(1L);
        replica.setChecksumSha256(checksum);
        replica.setNodeId(selfNodeId);
        
        when(replicaRepository.findByNodeId(selfNodeId)).thenReturn(List.of(replica));

        ObjectMeta meta = new ObjectMeta();
        meta.setStatus(ObjectStatus.TOMBSTONED);
        
        when(objectMetaRepository.findById(new ObjectMetaId("test-obj", 1L)))
                .thenReturn(Optional.of(meta));

        backgroundScrubber.scrub();

        // Assert file was deleted physically
        assertFalse(Files.exists(tempFile));
        
        // Assert Replica row was deleted from DB
        verify(replicaRepository, times(1)).delete(replica);
    }

    @Test
    void testScrubberDeletesOrphanedReplicaWhenMetaMissing() throws Exception {
        UUID selfNodeId = UUID.randomUUID();
        when(nodeRegistryService.getSelfNodeId()).thenReturn(selfNodeId);
        when(localStorageService.listAllDataFiles()).thenReturn(List.of(tempFile));

        String checksum = ChecksumUtil.sha256("test data".getBytes());
        
        Replica replica = new Replica();
        replica.setObjectId("test-obj");
        replica.setVersion(1L);
        replica.setChecksumSha256(checksum);
        replica.setNodeId(selfNodeId);
        
        when(replicaRepository.findByNodeId(selfNodeId)).thenReturn(List.of(replica));

        // Simulate meta missing (already GC'd)
        when(objectMetaRepository.findById(new ObjectMetaId("test-obj", 1L)))
                .thenReturn(Optional.empty());

        backgroundScrubber.scrub();

        // Assert file was deleted physically
        assertFalse(Files.exists(tempFile));
        
        // Assert Replica row was deleted from DB
        verify(replicaRepository, times(1)).delete(replica);
    }

    @Test
    void testScrubberNeverCollidesSlashAndUnderscoreKeys(@TempDir java.nio.file.Path dir) throws Exception {
        UUID selfNodeId = UUID.randomUUID();
        when(nodeRegistryService.getSelfNodeId()).thenReturn(selfNodeId);

        byte[] bytes = "identical-bytes".getBytes();
        String checksum = ChecksumUtil.sha256(bytes);

        Path slashFile = dir.resolve(ObjectKey.storageFileName("a/b", 1L));
        Path underscoreFile = dir.resolve(ObjectKey.storageFileName("a_b", 1L));
        Files.write(slashFile, bytes);
        Files.write(underscoreFile, bytes);
        when(localStorageService.listAllDataFiles()).thenReturn(List.of(slashFile, underscoreFile));

        Replica slashRep = new Replica();
        slashRep.setObjectId("a/b");
        slashRep.setVersion(1L);
        slashRep.setChecksumSha256(checksum);
        slashRep.setNodeId(selfNodeId);

        Replica underscoreRep = new Replica();
        underscoreRep.setObjectId("a_b");
        underscoreRep.setVersion(1L);
        underscoreRep.setChecksumSha256(checksum);
        underscoreRep.setNodeId(selfNodeId);

        when(replicaRepository.findByNodeId(selfNodeId)).thenReturn(List.of(slashRep, underscoreRep));

        ObjectMeta tombMeta = new ObjectMeta();
        tombMeta.setStatus(ObjectStatus.TOMBSTONED);
        ObjectMeta activeMeta = new ObjectMeta();
        activeMeta.setStatus(ObjectStatus.ACTIVE);

        when(objectMetaRepository.findById(new ObjectMetaId("a/b", 1L))).thenReturn(Optional.of(tombMeta));
        when(objectMetaRepository.findById(new ObjectMetaId("a_b", 1L))).thenReturn(Optional.of(activeMeta));

        backgroundScrubber.scrub();

        assertFalse(Files.exists(slashFile), "Tombstoned a/b file must be cleaned");
        assertTrue(Files.exists(underscoreFile), "Active a_b file must remain untouched");
    }
}
