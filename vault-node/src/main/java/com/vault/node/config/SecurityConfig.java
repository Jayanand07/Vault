package com.vault.node.config;

import com.vault.node.security.ApiKeyFilter;
import com.vault.node.security.RateLimitFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
public class SecurityConfig {

    @Value("${vault.security.api-keys:}")
    private String apiKeysString;

    @Value("${vault.security.rate-limit-write-per-sec:100}")
    private int rateLimitWrite;

    @Bean
    public FilterRegistrationBean<ApiKeyFilter> apiKeyFilter() {
        FilterRegistrationBean<ApiKeyFilter> registrationBean = new FilterRegistrationBean<>();
        
        List<String> keys = StringUtils.hasText(apiKeysString) 
            ? Arrays.stream(apiKeysString.split(",")).map(String::trim).collect(Collectors.toList())
            : Collections.emptyList();
            
        registrationBean.setFilter(new ApiKeyFilter(keys));
        registrationBean.addUrlPatterns("/api/*");
        registrationBean.setOrder(1); // Execute first
        
        return registrationBean;
    }

    @Bean
    public FilterRegistrationBean<RateLimitFilter> rateLimitFilter() {
        FilterRegistrationBean<RateLimitFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new RateLimitFilter(rateLimitWrite));
        registrationBean.addUrlPatterns("/api/*");
        registrationBean.setOrder(2); // Execute after auth
        
        return registrationBean;
    }
}
