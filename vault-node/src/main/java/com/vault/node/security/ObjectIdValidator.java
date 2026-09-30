package com.vault.node.security;

import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class ObjectIdValidator {
    
    // Allows: a-z, A-Z, 0-9, '.', '-', '_', '/'
    private static final Pattern VALID_PATTERN = Pattern.compile("^[a-zA-Z0-9.\\-_/]+$");

    public boolean isValid(String objectId) {
        if (objectId == null || objectId.isEmpty() || objectId.length() > 512) {
            return false;
        }
        if (objectId.contains("..")) {
            return false; // Prevent path traversal
        }
        if (objectId.startsWith("/") || objectId.endsWith("/")) {
            return false;
        }
        return VALID_PATTERN.matcher(objectId).matches();
    }
    
    public void validateOrThrow(String objectId) {
        if (!isValid(objectId)) {
            throw new IllegalArgumentException("Invalid object ID");
        }
    }
}
