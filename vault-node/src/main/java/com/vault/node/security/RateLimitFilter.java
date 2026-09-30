package com.vault.node.security;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RateLimitFilter extends OncePerRequestFilter {

    private final int maxRequestsPerSecond;
    private final Cache<String, AtomicInteger> requestCounts;

    public RateLimitFilter(int maxRequestsPerSecond) {
        this.maxRequestsPerSecond = maxRequestsPerSecond;
        this.requestCounts = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(1))
                .build();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();
        if (!path.startsWith("/api/")) {
            filterChain.doFilter(request, response);
            return;
        }

        String clientIp = request.getRemoteAddr();
        AtomicInteger count = requestCounts.get(clientIp, k -> new AtomicInteger(0));

        if (count.incrementAndGet() > maxRequestsPerSecond) {
            log.warn("Rate limit exceeded for IP: {}", clientIp);
            response.setStatus(429); // Too Many Requests
            response.getWriter().write("Rate limit exceeded");
            return;
        }

        filterChain.doFilter(request, response);
    }
}
