package com.vault.node;

import com.vault.common.model.DurabilityTier;
import com.vault.common.model.ObjectStatus;
import com.vault.node.model.ObjectMeta;
import com.vault.node.repository.ObjectMetaRepository;
import com.vault.node.service.MetadataService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers
class TombstoneIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("vault")
            .withUsername("vault")
            .withPassword("vault_secret");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("vault.node.id", () -> "test-node");
        registry.add("vault.cluster.seeds", () -> "localhost:8080");
        registry.add("vault.cluster.node-secret", () -> "test_secret");
    }

    @Autowired
    private ObjectMetaRepository repository;

    @Autowired
    private MetadataService metadataService;

    @Test
    void testTombstonePreventsResurrection() {
        String objectId = "test-tombstone-obj";

        // 1. Insert ACTIVE row (version 1)
        ObjectMeta v1 = new ObjectMeta();
        v1.setObjectId(objectId);
        v1.setVersion(1L);
        v1.setSizeBytes(100L);
        v1.setChecksumSha256("hash");
        v1.setTerm(1L);
        v1.setDurabilityTier(DurabilityTier.STANDARD);
        v1.setStatus(ObjectStatus.ACTIVE);
        repository.save(v1);
        
        assertNotNull(metadataService.findLatestActive(objectId));

        // 2. Insert TOMBSTONED row (version 2)
        ObjectMeta v2 = new ObjectMeta();
        v2.setObjectId(objectId);
        v2.setVersion(2L);
        v2.setSizeBytes(0L);
        v2.setChecksumSha256("TOMBSTONE");
        v2.setTerm(1L);
        v2.setDurabilityTier(DurabilityTier.STANDARD);
        v2.setStatus(ObjectStatus.TOMBSTONED);
        repository.save(v2);

        // 3. Query latest active, should return null (404)
        ObjectMeta latest = metadataService.findLatestActive(objectId);
        assertNull(latest, "Tombstoned object should not be returned as active");
    }
}
