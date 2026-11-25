package com.vault.node.config;

import com.vault.node.TestEphemeralCredentials;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mock.env.MockEnvironment;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for P0-10, V-13, and V-18:
 * no default secrets outside the dev profile, fail-fast ProductionSafetyCheck,
 * restricted actuator health details, and aligned Flyway dependencies.
 */
class SecurityDefaultsRegressionTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(ProductionSafetyCheck.class);

    @Test
    void applicationYmlContainsNoDefaultSecretsAndRestrictsActuatorDetails() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        Properties props = yaml.getObject();
        assertNotNull(props, "application.yml must be readable");

        MockEnvironment env = new MockEnvironment();

        String nodeSecret = env.resolvePlaceholders(props.getProperty("vault.cluster.node-secret", ""));
        String apiKeys = env.resolvePlaceholders(props.getProperty("vault.security.api-keys", ""));
        String dbPassword = env.resolvePlaceholders(props.getProperty("spring.datasource.password", ""));
        String showDetails = env.resolvePlaceholders(props.getProperty("management.endpoint.health.show-details", ""));

        assertTrue(nodeSecret.isBlank(), "vault.cluster.node-secret must have no default outside dev profile");
        assertTrue(apiKeys.isBlank(), "vault.security.api-keys must have no default outside dev profile");
        assertTrue(dbPassword.isBlank(), "spring.datasource.password must have no default outside dev profile");
        assertNotEquals("always", showDetails.toLowerCase(),
                "management.endpoint.health.show-details must not default to always");
        assertEquals("when_authorized", showDetails.toLowerCase(),
                "management.endpoint.health.show-details should default to when_authorized");
    }

    @Test
    void productionSafetyCheckFailsWithoutSecretsOutsideDevProfile() {
        contextRunner.run(context -> {
            assertNotNull(context.getStartupFailure(),
                    "Node must fail fast at startup when credentials are omitted outside dev profile");
            Throwable root = context.getStartupFailure().getCause();
            assertNotNull(root);
            assertInstanceOf(IllegalStateException.class, root);
        });
    }

    @Test
    void productionSafetyCheckRejectsExposedDefaultSecretsOutsideDevProfile() {
        contextRunner
                .withPropertyValues(
                        "spring.datasource.password=vault_secret",
                        "vault.cluster.node-secret=changeme",
                        "vault.security.api-keys=vault-dev-key"
                )
                .run(context -> {
                    assertNotNull(context.getStartupFailure(),
                            "Node must reject exposed default secrets outside dev profile");
                    assertInstanceOf(IllegalStateException.class, context.getStartupFailure().getCause());
                });
    }

    @Test
    void productionSafetyCheckRejectsExposedDefaultApiKeyEvenWhenOtherSecretsSet() {
        contextRunner
                .withPropertyValues(
                        "spring.datasource.password=" + TestEphemeralCredentials.dbCredential(),
                        "vault.cluster.node-secret=" + TestEphemeralCredentials.nodeCredential(),
                        "vault.security.api-keys=" + TestEphemeralCredentials.clientKey() + ",test-key"
                )
                .run(context -> assertNotNull(context.getStartupFailure(),
                        "Node must reject exposed dev/test API keys outside dev profile"));
    }

    @Test
    void productionSafetyCheckStartsWithExplicitNonDefaultSecrets() {
        contextRunner
                .withPropertyValues(
                        "spring.datasource.password=" + TestEphemeralCredentials.dbCredential(),
                        "vault.cluster.node-secret=" + TestEphemeralCredentials.nodeCredential(),
                        "vault.security.api-keys=" + TestEphemeralCredentials.clientKey()
                )
                .run(context -> assertNull(context.getStartupFailure(),
                        "Node should start cleanly when strong non-default credentials are provided"));
    }

    @Test
    void devProfileSuppliesDevDefaultsAndAllowsLocalStartup() {
        contextRunner
                .withPropertyValues("spring.profiles.active=dev")
                .run(context -> {
                    assertNull(context.getStartupFailure(),
                            "Dev profile should load application-dev.yml and start locally");
                    assertEquals("vault-dev-key",
                            context.getEnvironment().getProperty("vault.security.api-keys"));
                });
    }

    @Test
    void flywayDependenciesAreAlignedAndExecutableRepackageConfigured() throws Exception {
        Path pom = Path.of("pom.xml");
        if (!Files.exists(pom)) {
            pom = Path.of("vault-node", "pom.xml");
        }
        String xml = Files.readString(pom, StandardCharsets.UTF_8);
        assertFalse(xml.contains("flyway-database-postgresql"),
                "Mixed Flyway 10 flyway-database-postgresql module must not be present with Boot 3.2.4 flyway-core 9.x");
        assertTrue(xml.contains("<goal>repackage</goal>"),
                "spring-boot-maven-plugin must configure repackage execution for runnable fat JAR");
    }
}
