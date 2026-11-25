package com.vault.node.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Fails node startup if required credentials are missing or use known exposed
 * development defaults when running outside the explicit {@code dev} profile.
 */
@Slf4j
@Component
public class ProductionSafetyCheck implements InitializingBean {

    private static final Set<String> EXPOSED_DEFAULT_DB_PASSWORDS = Set.of(
            "vault_secret", "changeme", "password", "admin", "postgres", "secret"
    );

    private static final Set<String> EXPOSED_DEFAULT_NODE_SECRETS = Set.of(
            "changeme", "vault_secret", "secret", "secret123", "dev-only-node-secret"
    );

    private static final Set<String> EXPOSED_DEFAULT_API_KEYS = Set.of(
            "vault-dev-key", "test-key", "changeme", "vault-secret"
    );

    private final Environment environment;
    private final String dbPassword;
    private final String nodeSecret;
    private final String apiKeys;

    public ProductionSafetyCheck(
            Environment environment,
            @Value("${spring.datasource.password:}") String dbPassword,
            @Value("${vault.cluster.node-secret:}") String nodeSecret,
            @Value("${vault.security.api-keys:}") String apiKeys) {
        this.environment = environment;
        this.dbPassword = dbPassword;
        this.nodeSecret = nodeSecret;
        this.apiKeys = apiKeys;
    }

    @Override
    public void afterPropertiesSet() {
        boolean isDevProfile = environment.acceptsProfiles(Profiles.of("dev"));
        if (isDevProfile) {
            log.warn("========================================================================");
            log.warn("ACTIVE SPRING PROFILE 'dev' - Development credential defaults allowed.");
            log.warn("NEVER run the 'dev' profile against non-local or irreplaceable data.");
            log.warn("========================================================================");
            if (!StringUtils.hasText(dbPassword) || !StringUtils.hasText(nodeSecret) || !StringUtils.hasText(apiKeys)) {
                throw new IllegalStateException("Credentials must be non-empty even in dev profile.");
            }
            return;
        }

        if (!StringUtils.hasText(dbPassword)) {
            throw new IllegalStateException(
                    "Refusing to start outside dev profile: DB_PASSWORD (spring.datasource.password) is empty.");
        }
        if (EXPOSED_DEFAULT_DB_PASSWORDS.contains(dbPassword.trim())) {
            throw new IllegalStateException(
                    "Refusing to start outside dev profile: spring.datasource.password uses a known exposed default.");
        }

        if (!StringUtils.hasText(nodeSecret)) {
            throw new IllegalStateException(
                    "Refusing to start outside dev profile: VAULT_NODE_SECRET (vault.cluster.node-secret) is empty.");
        }
        if (EXPOSED_DEFAULT_NODE_SECRETS.contains(nodeSecret.trim())) {
            throw new IllegalStateException(
                    "Refusing to start outside dev profile: vault.cluster.node-secret uses a known exposed default.");
        }

        List<String> configuredKeys = Arrays.stream((apiKeys == null ? "" : apiKeys).split(","))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .toList();
        if (configuredKeys.isEmpty()) {
            throw new IllegalStateException(
                    "Refusing to start outside dev profile: VAULT_API_KEYS (vault.security.api-keys) is empty.");
        }
        for (String key : configuredKeys) {
            if (EXPOSED_DEFAULT_API_KEYS.contains(key)) {
                throw new IllegalStateException(
                        "Refusing to start outside dev profile: vault.security.api-keys contains a known exposed dev key.");
            }
        }
    }
}
