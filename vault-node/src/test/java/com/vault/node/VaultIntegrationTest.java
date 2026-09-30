package com.vault.node;

import com.vault.common.model.DurabilityTier;
import com.vault.node.election.NodeState;
import com.vault.node.service.ObjectService;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class VaultIntegrationTest {

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
        registry.add("vault.node.id", () -> "test-node-1");
        registry.add("vault.cluster.seeds", () -> "localhost:8080");
        registry.add("vault.cluster.node-secret", () -> "test_secret");
    }

    @Autowired
    private ObjectService objectService;
    
    @Autowired
    private NodeState nodeState;

    @Test
    @Disabled("Requires a multi-node mock setup for quorum writes, testing locally without peers fails")
    void testFullWriteReadDeleteCycle() {
        // This is a placeholder for the integration test.
        // A real integration test would spin up 3 Spring Boot contexts.
        // Since we only spin up one node here, we can't meet w=2 for durability tier STANDARD.
        // But the CI structure is here.
        
        // 1. Write
        // byte[] data = "Hello World".getBytes();
        // var result = objectService.put("integration-test-obj", data, DurabilityTier.TEMPORARY);
        
        // 2. Read
        // ...
        
        // 3. Delete
        // ...
    }
}
