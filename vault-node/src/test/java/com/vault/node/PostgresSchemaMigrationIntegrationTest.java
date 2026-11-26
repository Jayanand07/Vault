package com.vault.node;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression & acceptance gate tests for P0-01 (Batch A):
 * - Confirms V1..V6 schema fails Hibernate ddl-auto=validate on CHAR(64) checksum columns
 * - Verifies V7 normalizes objects.checksum_sha256 and replicas.checksum_sha256 to VARCHAR(64)
 * - Verifies V1..V6 -> V7 upgrade preserves 64-hex digests and unpadded 'TOMBSTONE' markers
 * - Verifies fresh V1..V7 migration passes Hibernate ddl-auto=validate across all entities
 */
@EnabledIf("com.vault.node.PostgresTestTarget#shouldExecute")
class PostgresSchemaMigrationIntegrationTest {

    private static PostgresTestTarget postgres;

    @BeforeAll
    static void startPostgres() {
        postgres = PostgresTestTarget.startOrFailIfRequired("vault_migration_base");
    }

    @AfterAll
    static void stopPostgres() {
        if (postgres != null) {
            postgres.close();
        }
    }

    @Test
    void preV7SchemaFailsHibernateValidationConfirmingChecksumTypeMismatch() throws Exception {
        postgres.withFreshDatabase("pre_v7", ds -> {
            Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .target("6")
                    .load()
                    .migrate();

            assertEquals("character", columnDataType(ds, "objects", "checksum_sha256"));
            assertEquals("character", columnDataType(ds, "replicas", "checksum_sha256"));

            Throwable err = assertThrows(Throwable.class, () -> validateHibernateEntities(ds),
                    "V1..V6 CHAR(64) checksum columns must fail Hibernate ddl-auto=validate");
            String msg = flattenMessages(err).toLowerCase();
            assertTrue(msg.contains("checksum_sha256") && msg.contains("bpchar"),
                    "Expected bpchar/checksum_sha256 validation failure, got: " + msg);
        });
    }

    @Test
    void upgradeFromV6ToV7PreservesDataAndTombstoneMarkersAndPassesHibernateValidation() throws Exception {
        String digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        UUID nodeId = UUID.fromString("11111111-1111-1111-1111-111111111111");

        postgres.withFreshDatabase("upgrade_v7", ds -> {
            // 1. Migrate only to V6 (the previous production/CI baseline)
            Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .target("6")
                    .load()
                    .migrate();

            // 2. Seed pre-V7 rows including an ACTIVE SHA-256 object and a literal TOMBSTONE marker
            try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO nodes (node_id, address, status) VALUES ('" + nodeId + "', '127.0.0.1:8081', 'UP')");
                stmt.execute("INSERT INTO nodes (node_id, address, status) VALUES ('22222222-2222-2222-2222-222222222222', '127.0.0.1:8082', 'DEGRADED')");
                stmt.execute("INSERT INTO objects (object_id, version, size_bytes, checksum_sha256, durability_tier, term, status) " +
                        "VALUES ('docs/report.pdf', 1, 1024, '" + digest + "', 'STANDARD', 1, 'ACTIVE')");
                stmt.execute("INSERT INTO replicas (object_id, version, node_id, checksum_sha256, status) " +
                        "VALUES ('docs/report.pdf', 1, '" + nodeId + "', '" + digest + "', 'HEALTHY')");
                stmt.execute("INSERT INTO objects (object_id, version, size_bytes, checksum_sha256, durability_tier, term, status) " +
                        "VALUES ('docs/deleted.txt', 2, 0, 'TOMBSTONE', 'STANDARD', 1, 'TOMBSTONED')");
                stmt.execute("INSERT INTO replicas (object_id, version, node_id, checksum_sha256, status) " +
                        "VALUES ('docs/deleted.txt', 2, '" + nodeId + "', 'TOMBSTONE', 'HEALTHY')");
                stmt.execute("INSERT INTO idempotency_keys (key, status, response_code, response_payload) " +
                        "VALUES ('upgrade-idem-1', 'DONE', 201, '{\"status\":\"ok\"}'::jsonb)");
            }

            // 3. Upgrade cleanly through V7 without altering V2/V3
            Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .load()
                    .migrate();

            // 4. Verify PostgreSQL column metadata is now VARCHAR(64)
            assertEquals("character varying", columnDataType(ds, "objects", "checksum_sha256"));
            assertEquals(64, columnMaxLength(ds, "objects", "checksum_sha256"));
            assertEquals("character varying", columnDataType(ds, "replicas", "checksum_sha256"));
            assertEquals(64, columnMaxLength(ds, "replicas", "checksum_sha256"));

            // 5. Verify existing rows and exact TOMBSTONE string length (no CHAR(64) trailing padding)
            assertEquals(digest, queryChecksum(ds, "objects", "docs/report.pdf", 1));
            assertEquals(digest, queryChecksum(ds, "replicas", "docs/report.pdf", 1));
            String tombObj = queryChecksum(ds, "objects", "docs/deleted.txt", 2);
            String tombRep = queryChecksum(ds, "replicas", "docs/deleted.txt", 2);
            assertEquals("TOMBSTONE", tombObj);
            assertEquals(9, tombObj.length());
            assertEquals("TOMBSTONE", tombRep);
            assertEquals(9, tombRep.length());

            // 6. Verify Hibernate ddl-auto=validate passes for ALL entities on the upgraded schema
            EntityManagerFactory emf = validateHibernateEntities(ds);
            try {
                EntityManager em = emf.createEntityManager();
                Long activeCount = em.createQuery("SELECT COUNT(o) FROM ObjectMeta o", Long.class).getSingleResult();
                Long replicaCount = em.createQuery("SELECT COUNT(r) FROM Replica r", Long.class).getSingleResult();
                Long nodeCount = em.createQuery("SELECT COUNT(n) FROM Node n", Long.class).getSingleResult();
                assertEquals(2L, activeCount);
                assertEquals(2L, replicaCount);
                assertEquals(2L, nodeCount);
                em.close();
            } finally {
                emf.close();
            }
        });
    }

    @Test
    void freshDatabaseMigratesV1ThroughV7AndPassesHibernateValidation() throws Exception {
        postgres.withFreshDatabase("fresh_v7", ds -> {
            var result = Flyway.configure()
                    .dataSource(ds)
                    .locations("classpath:db/migration")
                    .load()
                    .migrate();
            assertTrue(result.migrationsExecuted >= 7, "Expected V1..V7 migrations on fresh Postgres database");

            EntityManagerFactory emf = validateHibernateEntities(ds);
            try {
                assertNotNull(emf.getMetamodel().entity(com.vault.node.model.ObjectMeta.class));
                assertNotNull(emf.getMetamodel().entity(com.vault.node.model.Replica.class));
                assertNotNull(emf.getMetamodel().entity(com.vault.node.model.Node.class));
                assertNotNull(emf.getMetamodel().entity(com.vault.node.model.ElectionState.class));
                assertNotNull(emf.getMetamodel().entity(com.vault.node.model.IdempotencyKey.class));
            } finally {
                emf.close();
            }
        });
    }

    private static EntityManagerFactory validateHibernateEntities(DataSource ds) {
        LocalContainerEntityManagerFactoryBean factoryBean = new LocalContainerEntityManagerFactoryBean();
        factoryBean.setDataSource(ds);
        factoryBean.setPackagesToScan("com.vault.node.model");
        factoryBean.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        factoryBean.setJpaPropertyMap(Map.of(
                "hibernate.hbm2ddl.auto", "validate",
                "hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect",
                "hibernate.physical_naming_strategy", "org.hibernate.boot.model.naming.CamelCaseToUnderscoresNamingStrategy",
                "hibernate.implicit_naming_strategy", "org.springframework.boot.orm.jpa.hibernate.SpringImplicitNamingStrategy"
        ));
        factoryBean.afterPropertiesSet();
        return factoryBean.getObject();
    }

    private static String columnDataType(DataSource ds, String table, String column) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?")) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Missing column " + table + "." + column);
                return rs.getString(1);
            }
        }
    }

    private static int columnMaxLength(DataSource ds, String table, String column) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? AND column_name = ?")) {
            ps.setString(1, table);
            ps.setString(2, column);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Missing column length for " + table + "." + column);
                return rs.getInt(1);
            }
        }
    }

    private static String queryChecksum(DataSource ds, String table, String objectId, long version) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT checksum_sha256 FROM " + table + " WHERE object_id = ? AND version = ?")) {
            ps.setString(1, objectId);
            ps.setLong(2, version);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Missing row in " + table + " for " + objectId + " v" + version);
                return rs.getString(1);
            }
        }
    }

    private static String flattenMessages(Throwable t) {
        StringBuilder sb = new StringBuilder();
        Throwable cur = t;
        while (cur != null) {
            if (cur.getMessage() != null) {
                sb.append(cur.getMessage()).append(" | ");
            }
            cur = cur.getCause();
        }
        return sb.toString();
    }
}
