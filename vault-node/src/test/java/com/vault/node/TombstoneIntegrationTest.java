package com.vault.node;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.service.MetadataService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TombstoneIntegrationTest {

    @Mock
    private ObjectMetaRepository repository;

    @InjectMocks
    private MetadataService metadataService;

    @Test
    void testTombstonePreventsResurrection() {
        String objectId = "test-tombstone-obj";

        // 1. Simulate ACTIVE row (version 1) is the latest
        ObjectMeta v1 = new ObjectMeta();
        v1.setObjectId(objectId);
        v1.setVersion(1L);
        v1.setStatus(ObjectStatus.ACTIVE);
        
        when(repository.findFirstByObjectIdAndStatusInOrderByVersionDesc(
                eq(objectId), 
                eq(List.of(ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED))))
            .thenReturn(Optional.of(v1));

        ObjectMeta latestActive = metadataService.findLatestActive(objectId);
        assertNotNull(latestActive);
        assertEquals(1L, latestActive.getVersion());

        // 2. Simulate TOMBSTONED row (version 2) is now the latest
        ObjectMeta v2 = new ObjectMeta();
        v2.setObjectId(objectId);
        v2.setVersion(2L);
        v2.setStatus(ObjectStatus.TOMBSTONED);

        when(repository.findFirstByObjectIdAndStatusInOrderByVersionDesc(
                eq(objectId), 
                eq(List.of(ObjectStatus.ACTIVE, ObjectStatus.TOMBSTONED))))
            .thenReturn(Optional.of(v2));

        // 3. Query latest active, should return null (404)
        ObjectMeta latestAfterTombstone = metadataService.findLatestActive(objectId);
        assertNull(latestAfterTombstone, "Tombstoned object should not be returned as active");
    }
}
