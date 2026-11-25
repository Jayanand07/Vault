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

    private final int maxWriteRequestsPerSecond;
    private final int maxReadRequestsPerSecond;
    private final Cache<String, AtomicInteger> requestCounts;

    public RateLimitFilter(int maxRequestsPerSecond) {
        this(maxRequestsPerSecond, maxRequestsPerSecond);
    }

    public RateLimitFilter(int maxWriteRequestsPerSecond, int maxReadRequestsPerSecond) {
        this.maxWriteRequestsPerSecond = maxWriteRequestsPerSecond;
        this.maxReadRequestsPerSecond = maxReadRequestsPerSecond;
        this.requestCounts = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofSeconds(1))
                .build();
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String path = request.getRequestURI();
        if (!path.startsWith("/api/") && !path.startsWith("/s3/")) {
            filterChain.doFilter(request, response);
            return;
        }

        boolean isRead = "GET".equalsIgnoreCase(request.getMethod()) || "HEAD".equalsIgnoreCase(request.getMethod());
        int limit = isRead ? maxReadRequestsPerSecond : maxWriteRequestsPerSecond;
        String bucketKey = request.getRemoteAddr() + (isRead ? ":R" : ":W");
        AtomicInteger count = requestCounts.get(bucketKey, k -> new AtomicInteger(0));

        if (count.incrementAndGet() > limit) {
            log.warn("Rate limit exceeded for IP {} ({}): limit={}/s", request.getRemoteAddr(), isRead ? "READ" : "WRITE", limit);
            response.setStatus(429); // Too Many Requests
            response.getWriter().write("Rate limit exceeded");
            return;
        }

        filterChain.doFilter(request, response);
    }
}
