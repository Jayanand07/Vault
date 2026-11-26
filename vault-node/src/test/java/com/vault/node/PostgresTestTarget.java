package com.vault.node;

import org.junit.jupiter.api.Assumptions;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.UUID;

/**
 * Shared real-PostgreSQL test lifecycle helper supporting either an explicit external Postgres
 * instance (VAULT_IT_POSTGRES_URL) or Docker Testcontainers, while failing loudly rather than
 * skipping when mandatory CI enforcement is active.
 */
public final class PostgresTestTarget implements AutoCloseable {

    private final PostgreSQLContainer<?> container;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final boolean ownsContainer;

    private PostgresTestTarget(
            PostgreSQLContainer<?> container,
            String jdbcUrl,
            String username,
            String password,
            boolean ownsContainer) {
        this.container = container;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.ownsContainer = ownsContainer;
    }

    private static volatile Boolean cachedDockerAvailable;

    public static boolean isMandatory() {
        if (Boolean.parseBoolean(System.getProperty("vault.postgres-it.required", "false"))) {
            return true;
        }
        if (Boolean.parseBoolean(System.getenv().getOrDefault("VAULT_REQUIRE_POSTGRES_IT", "false"))) {
            return true;
        }
        return "true".equalsIgnoreCase(System.getenv("CI"));
    }

    /**
     * Used by @EnabledIf so local offline runs skip cleanly with Skipped counts reported,
     * while mandatory CI runs return true and fail loudly inside startOrFailIfRequired().
     */
    public static boolean shouldExecute() {
        if (isMandatory()) {
            return true;
        }
        String externalUrl = System.getProperty(
                "vault.it.postgres.url",
                System.getenv().getOrDefault("VAULT_IT_POSTGRES_URL", "")).trim();
        return !externalUrl.isEmpty() || isDockerAvailable();
    }

    public static PostgresTestTarget startOrFailIfRequired(String defaultDatabaseName) {
        String externalUrl = System.getProperty(
                "vault.it.postgres.url",
                System.getenv().getOrDefault("VAULT_IT_POSTGRES_URL", "")).trim();
        if (!externalUrl.isEmpty()) {
            String user = System.getProperty(
                    "vault.it.postgres.user",
                    System.getenv().getOrDefault("VAULT_IT_POSTGRES_USER", "vault"));
            String pass = System.getProperty(
                    "vault.it.postgres.password",
                    System.getenv().getOrDefault("VAULT_IT_POSTGRES_PASSWORD", TestEphemeralCredentials.dbCredential()));
            return new PostgresTestTarget(null, externalUrl, user, pass, false);
        }

        if (isDockerAvailable()) {
            PostgreSQLContainer<?> pg = new PostgreSQLContainer<>("postgres:15-alpine")
                    .withDatabaseName(defaultDatabaseName)
                    .withUsername("vault")
                    .withPassword(TestEphemeralCredentials.dbCredential());
            pg.start();
            return new PostgresTestTarget(pg, pg.getJdbcUrl(), pg.getUsername(), pg.getPassword(), true);
        }

        String reason = "Real PostgreSQL integration tests require Docker or VAULT_IT_POSTGRES_URL";
        if (isMandatory()) {
            throw new AssertionError("Mandatory PostgreSQL IT cannot run: " + reason);
        }
        Assumptions.abort("Skipping PostgreSQL IT on local environment without Docker/Postgres: " + reason);
        throw new IllegalStateException(reason);
    }

    private static boolean isDockerAvailable() {
        Boolean cached = cachedDockerAvailable;
        if (cached != null) {
            return cached;
        }
        boolean available;
        try {
            available = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable t) {
            available = false;
        }
        cachedDockerAvailable = available;
        return available;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public DataSource createDataSource() {
        return createDataSourceForUrl(jdbcUrl);
    }

    public DataSource createDataSourceForUrl(String url) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(url);
        ds.setUser(username);
        ds.setPassword(password);
        return ds;
    }

    /**
     * Executes work against a newly created, empty database on the active PostgreSQL instance,
     * dropping the temporary database afterwards.
     */
    public void withFreshDatabase(String prefix, ThrowingConsumer<DataSource> action) throws Exception {
        String dbName = (prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12))
                .toLowerCase();
        String derivedUrl = replaceDatabaseName(jdbcUrl, dbName);
        try (Connection conn = createDataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE DATABASE " + dbName);
        }
        try {
            action.accept(createDataSourceForUrl(derivedUrl));
        } finally {
            try (Connection conn = createDataSource().getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '" + dbName + "' AND pid <> pg_backend_pid()");
                stmt.execute("DROP DATABASE IF EXISTS " + dbName);
            } catch (Exception ignored) {
                // Best-effort cleanup inside ephemeral container
            }
        }
    }

    private static String replaceDatabaseName(String url, String newDb) {
        int queryIdx = url.indexOf('?');
        String base = queryIdx >= 0 ? url.substring(0, queryIdx) : url;
        String suffix = queryIdx >= 0 ? url.substring(queryIdx) : "";
        int lastSlash = base.lastIndexOf('/');
        if (lastSlash < 0) {
            throw new IllegalArgumentException("Unexpected JDBC URL format: " + url);
        }
        return base.substring(0, lastSlash + 1) + newDb + suffix;
    }

    @Override
    public void close() {
        if (ownsContainer && container != null) {
            container.stop();
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T value) throws Exception;
    }
}
