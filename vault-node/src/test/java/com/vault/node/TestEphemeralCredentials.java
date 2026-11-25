package com.vault.node;

import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.StringUtils;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Generates ephemeral, per-JVM runtime test values so test sources contain no static credential literals.
 */
public final class TestEphemeralCredentials
        implements ApplicationContextInitializer<ConfigurableApplicationContext> {

    private static final SecureRandom RNG = new SecureRandom();
    private static final String RUNTIME_DB_CRED = freshToken();
    private static final String RUNTIME_NODE_CRED = freshToken();
    private static final String RUNTIME_CLIENT_KEY = freshToken();

    private static String freshToken() {
        byte[] bytes = new byte[18];
        RNG.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    public static String dbCredential() {
        return RUNTIME_DB_CRED;
    }

    public static String nodeCredential() {
        return RUNTIME_NODE_CRED;
    }

    public static String clientKey() {
        return RUNTIME_CLIENT_KEY;
    }

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        ConfigurableEnvironment env = context.getEnvironment();
        Map<String, Object> props = new LinkedHashMap<>();
        if (!StringUtils.hasText(env.getProperty("spring.datasource.password"))) {
            props.put("spring.datasource.password", RUNTIME_DB_CRED);
        }
        if (!StringUtils.hasText(env.getProperty("vault.cluster.node-secret"))) {
            props.put("vault.cluster.node-secret", RUNTIME_NODE_CRED);
        }
        if (!StringUtils.hasText(env.getProperty("vault.security.api-keys"))) {
            props.put("vault.security.api-keys", RUNTIME_CLIENT_KEY);
        }
        if (!props.isEmpty()) {
            env.getPropertySources().addFirst(new MapPropertySource("ephemeralTestProps", props));
        }
    }
}
