package com.vault.node.controller;

import com.vault.common.model.DurabilityTier;
import com.vault.node.security.ObjectIdValidator;
import com.vault.node.service.UploadSpooler;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;

/**
 * Experimental S3-Style API Adapter (requires X-API-Key; no AWS SigV4 or multipart upload).
 * Path style: /s3/{bucket}/{key}
 */
@RestController
@RequestMapping("/s3")
@RequiredArgsConstructor
@Slf4j
public class S3CompatibleController {

    private final ObjectController vaultController; // Reuse the robust core logic!
    private final ObjectIdValidator validator;
    private final UploadSpooler uploadSpooler;

    private String extractObjectId(String bucket, HttpServletRequest request) {
        String path = request.getRequestURI();
        String prefix = "/s3/" + bucket + "/";
        if (!path.startsWith(prefix) || path.length() <= prefix.length()) {
            throw new IllegalArgumentException("Object key cannot be empty");
        }
        return bucket + "/" + path.substring(prefix.length());
    }

    /**
     * S3 PUT Object
     * Maps an S3 upload request to Vault's native ObjectController logic.
     */
    @PutMapping("/{bucket}/**")
    public ResponseEntity<?> putObject(
            @PathVariable String bucket,
            HttpServletRequest request) {

        Path tempFile = null;
        try {
            String objectId = extractObjectId(bucket, request);
            validator.validateOrThrow(objectId);

            // Preserve caller-supplied X-Idempotency-Key; avoid minting synthetic keys on every S3 PUT
            String idempotencyKey = request.getHeader("X-Idempotency-Key");
            DurabilityTier tier = DurabilityTier.STANDARD;

            tempFile = uploadSpooler.spoolBounded(request, "vault-s3-upload-", false);

            ResponseEntity<?> response = vaultController.putObject(objectId, idempotencyKey, tier, tempFile);

            if (response.getStatusCode().is2xxSuccessful()) {
                return ResponseEntity.ok()
                        .header("ETag", "\"vault-s3-proxy\"")
                        .build();
            }

            return response;
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (UploadSpooler.UnsupportedUploadMediaTypeException e) {
            return ResponseEntity.status(HttpStatus.UNSUPPORTED_MEDIA_TYPE).body(Map.of("error", e.getMessage()));
        } catch (UploadSpooler.PayloadTooLargeException e) {
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("S3 PUT failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        } finally {
            if (tempFile != null) {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (Exception ignore) {}
            }
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
            String objectId = extractObjectId(bucket, request);
            validator.validateOrThrow(objectId);
            return vaultController.getObject(objectId, response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
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
            String objectId = extractObjectId(bucket, request);
            validator.validateOrThrow(objectId);
            ResponseEntity<?> response = vaultController.deleteObject(objectId);

            if (response.getStatusCode().is2xxSuccessful()) {
                return ResponseEntity.noContent().build();
            }
            return response;
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            log.error("S3 DELETE failed", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }
}
