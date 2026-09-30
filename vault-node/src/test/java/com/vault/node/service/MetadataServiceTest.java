package com.vault.node.service;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.repository.ObjectMetaRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MetadataServiceTest {

    @Mock
    private ObjectMetaRepository repository;

    @InjectMocks
    private MetadataService metadataService;

    private ObjectMeta mockMeta;

    @BeforeEach
    void setUp() {
        mockMeta = new ObjectMeta();
        mockMeta.setObjectId("test-obj");
        mockMeta.setVersion(1L);
        mockMeta.setStatus(ObjectStatus.ACTIVE);
        mockMeta.setDurabilityTier(DurabilityTier.STANDARD);
    }

    @Test
    void testFindLatestActiveFound() {
        when(repository.findFirstByObjectIdAndStatusInOrderByVersionDesc("test-obj", List.of(ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED)))
                .thenReturn(Optional.of(mockMeta));

        ObjectMeta result = metadataService.findLatestActive("test-obj");

        assertNotNull(result);
        assertEquals("test-obj", result.getObjectId());
        assertEquals(1L, result.getVersion());
    }

    @Test
    void testFindLatestActiveNotFound() {
        when(repository.findFirstByObjectIdAndStatusInOrderByVersionDesc("test-obj", List.of(ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED)))
                .thenReturn(Optional.empty());

        ObjectMeta result = metadataService.findLatestActive("test-obj");

        assertNull(result);
    }

    @Test
    void testClaimVersionNoPrevious() {
        when(repository.findFirstByObjectIdOrderByVersionDesc("new-obj"))
                .thenReturn(Optional.empty());
                
        when(repository.save(any(ObjectMeta.class))).thenAnswer(invocation -> invocation.getArgument(0));

        long version = metadataService.claimVersion("new-obj", 100L, "hash", 1L, DurabilityTier.CRITICAL);

        assertEquals(1L, version);
    }

    @Test
    void testClaimVersionWithPrevious() {
        when(repository.findFirstByObjectIdOrderByVersionDesc("test-obj"))
                .thenReturn(Optional.of(mockMeta));
                
        when(repository.save(any(ObjectMeta.class))).thenAnswer(invocation -> invocation.getArgument(0));

        long version = metadataService.claimVersion("test-obj", 200L, "hash", 1L, DurabilityTier.STANDARD);

        assertEquals(2L, version);
    }
}
