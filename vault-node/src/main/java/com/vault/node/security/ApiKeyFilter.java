package com.vault.node.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ApiKeyFilter extends OncePerRequestFilter {

    private final List<String> validApiKeys;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();
        
        // Protect external API paths and S3 adapter
        if (!path.startsWith("/api/") && !path.startsWith("/s3/")) {
            filterChain.doFilter(request, response);
            return;
        }

        if (validApiKeys.isEmpty()) {
            log.warn("API Key validation rejected - no API keys configured (fail-closed)");
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("API access disabled: No valid API keys configured");
            return;
        }

        String apiKey = request.getHeader("X-API-Key");
        boolean matches = false;
        if (apiKey != null) {
            byte[] apiKeyBytes = apiKey.getBytes(StandardCharsets.UTF_8);
            for (String validKey : validApiKeys) {
                byte[] validKeyBytes = validKey.getBytes(StandardCharsets.UTF_8);
                if (MessageDigest.isEqual(validKeyBytes, apiKeyBytes)) {
                    matches = true;
                }
            }
        }

        if (!matches) {
            String maskedKey = apiKey == null ? "null" : (apiKey.length() > 4 ? apiKey.substring(0, 4) + "***" : "***");
            log.warn("Unauthorized API access attempt to {} with key {}", path, maskedKey);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("Invalid or missing X-API-Key");
            return;
        }

        filterChain.doFilter(request, response);
    }
}
