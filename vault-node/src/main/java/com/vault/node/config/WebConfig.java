package com.vault.node.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowedOriginPatterns("*") // allow all origins
                .allowedMethods("GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS")
                .allowedHeaders("*")
                .exposedHeaders("X-Vault-Version", "X-Vault-Checksum", "X-Vault-Consistency-Mode", "X-Vault-Leader-Address")
                .allowCredentials(true)
                .maxAge(3600);
    }
}
