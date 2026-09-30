package com.vault.node.controller;

import com.vault.common.model.DurabilityTier;
import com.vault.node.security.ObjectIdValidator;
import com.vault.node.service.ObjectService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * S3-Compatible API Adapter.
 * Allows developers to use standard AWS SDKs to interact with Vault.
 * Path style: /s3/{bucket}/{key}
 */
@RestController
@RequestMapping("/s3")
@RequiredArgsConstructor
@Slf4j
public class S3CompatibleController {

    private final ObjectController vaultController; // Reuse the robust core logic!
    private final ObjectIdValidator validator;

    /**
     * S3 PUT Object
     * Maps an S3 upload request to Vault's native ObjectController logic.
     */
    @PutMapping("/{bucket}/**")
    public ResponseEntity<?> putObject(
            @PathVariable String bucket,
            HttpServletRequest request) {
        
        try {
            // Extract the {key} from the URL since /** captures the rest of the path
            String path = request.getRequestURI();
            String key = path.substring(("/s3/" + bucket + "/").length());
            String objectId = bucket + "/" + key;
            
            validator.validateOrThrow(objectId);

            // S3 clients might not send idempotency keys, so we auto-generate one for the internal Vault pipeline
            String idempotencyKey = UUID.randomUUID().toString();
            
            // For now, default to STANDARD durability for S3 uploads
            DurabilityTier tier = DurabilityTier.STANDARD;
            
            // Spool the incoming S3 upload to a temporary file to avoid OOM on large files
            Path tempFile = Files.createTempFile("vault-s3-upload-", ".tmp");
            try {
                try (InputStream is = request.getInputStream()) {
                    Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
                }

                // Delegate to the battle-tested Vault controller
                ResponseEntity<?> response = vaultController.putObject(objectId, idempotencyKey, tier, tempFile);
                
                if (response.getStatusCode().is2xxSuccessful()) {
                    // S3 returns 200 OK for PUT (Vault returns 201 Created natively, so we map it)
                    return ResponseEntity.ok()
                            .header("ETag", "\"vault-s3-proxy\"") 
                            .build();
                }
                
                return response;
            } finally {
                Files.deleteIfExists(tempFile);
            }
            
        } catch (Exception e) {
            log.error("S3 PUT failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    /**
     * S3 GET Object
     */
    @GetMapping("/{bucket}/**")
    public ResponseEntity<?> getObject(
            @PathVariable String bucket,
            HttpServletRequest request,
            HttpServletResponse response) {
        
        try {
            String path = request.getRequestURI();
            String key = path.substring(("/s3/" + bucket + "/").length());
            String objectId = bucket + "/" + key;
            
            // Vault native controller already handles streaming and OOM protection!
            return vaultController.getObject(objectId, response);
            
        } catch (Exception e) {
            log.error("S3 GET failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * S3 DELETE Object
     */
    @DeleteMapping("/{bucket}/**")
    public ResponseEntity<?> deleteObject(
            @PathVariable String bucket,
            HttpServletRequest request) {
        
        try {
            String path = request.getRequestURI();
            String key = path.substring(("/s3/" + bucket + "/").length());
            String objectId = bucket + "/" + key;
            
            ResponseEntity<?> response = vaultController.deleteObject(objectId);
            
            if (response.getStatusCode().is2xxSuccessful()) {
                // S3 returns 204 No Content for successful deletes
                return ResponseEntity.noContent().build();
            }
            return response;
            
        } catch (Exception e) {
            log.error("S3 DELETE failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
