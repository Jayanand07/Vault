package com.vault.node.service;

import com.vault.common.dto.WriteResult;
import com.vault.common.model.DurabilityTier;
import com.vault.node.election.NodeState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ObjectServiceTest {

    @Mock
    private MetadataService metadataService;

    @Mock
    private ReplicationService replicationService;

    @Mock
    private NodeState nodeState;

    @InjectMocks
    private ObjectService objectService;

    @Test
    void testPutRetriesOnDataIntegrityViolation() throws Exception {
        String objectId = "test-obj";
        Path tempFile = Files.createTempFile("test", ".tmp");
        Files.write(tempFile, "hello world".getBytes());
        long term = 1L;
        DurabilityTier tier = DurabilityTier.STANDARD;

        when(nodeState.getCurrentTerm()).thenReturn(term);

        // First call throws DataIntegrityViolationException
        when(metadataService.claimVersion(eq(objectId), anyLong(), anyString(), eq(term), eq(tier)))
                .thenThrow(new DataIntegrityViolationException("Unique constraint violated"))
                .thenReturn(2L); // Second call succeeds with version 2

        WriteResult result = objectService.put(objectId, tempFile, tier);

        assertNotNull(result);
        
        // Verify claimVersion was called twice
        verify(metadataService, times(2)).claimVersion(eq(objectId), anyLong(), anyString(), eq(term), eq(tier));
        
        // Verify commit happened with the retried version (2L)
        verify(metadataService, times(1)).commitVersion(objectId, 2L);
        
        Files.deleteIfExists(tempFile);
    }
}
