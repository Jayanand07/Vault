package com.vault.node.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ApiKeyFilter extends OncePerRequestFilter {

    private final List<String> validApiKeys;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();
        
        // Only protect external API paths
        if (!path.startsWith("/api/")) {
            filterChain.doFilter(request, response);
            return;
        }

        String apiKey = request.getHeader("X-API-Key");
        if (!validApiKeys.isEmpty() && (apiKey == null || !validApiKeys.contains(apiKey))) {
            log.warn("Unauthorized API access attempt to {} with key {}", path, apiKey);
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("Invalid or missing X-API-Key");
            return;
        }

        if (validApiKeys.isEmpty()) {
            log.trace("API Key validation skipped - no keys configured");
        }

        filterChain.doFilter(request, response);
    }
}
