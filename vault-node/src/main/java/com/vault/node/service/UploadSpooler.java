package com.vault.node.service;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

/**
 * Validates raw upload contracts and spools bounded request bodies to a temp file
 * without unbounded disk fills or buffering whole objects in heap memory.
 */
@Component
public class UploadSpooler {

    private final long maxObjectSizeBytes;

    public UploadSpooler(@Value("${vault.storage.max-object-size-bytes:134217728}") long maxObjectSizeBytes) {
        this.maxObjectSizeBytes = maxObjectSizeBytes;
    }

    public long getMaxObjectSizeBytes() {
        return maxObjectSizeBytes;
    }

    /**
     * Rejects form-encoded or multipart wrappers so callers send raw binary payloads.
     */
    public void validateRawUploadHeaders(HttpServletRequest request, boolean rejectFormUrlEncoded) {
        String contentType = request.getContentType();
        if (contentType != null) {
            String normalized = contentType.toLowerCase(Locale.ROOT);
            if (normalized.startsWith("multipart/")) {
                throw new UnsupportedUploadMediaTypeException(
                        "Multipart uploads are not supported; send raw binary bytes (e.g. curl --data-binary).");
            }
            if (rejectFormUrlEncoded && normalized.startsWith("application/x-www-form-urlencoded")) {
                throw new UnsupportedUploadMediaTypeException(
                        "Form-urlencoded uploads are not supported; send raw binary bytes with Content-Type: application/octet-stream.");
            }
        }
        long declaredLength = request.getContentLengthLong();
        if (declaredLength > maxObjectSizeBytes) {
            throw new PayloadTooLargeException(
                    "Declared Content-Length (" + declaredLength + ") exceeds limit of " + maxObjectSizeBytes + " bytes.");
        }
    }

    /**
     * Copies the request body to a newly created temp file while enforcing {@link #maxObjectSizeBytes}.
     * Caller owns deletion of the returned temp file when this method returns cleanly.
     */
    public Path spoolBounded(HttpServletRequest request, String tempPrefix, boolean rejectFormUrlEncoded) throws IOException {
        validateRawUploadHeaders(request, rejectFormUrlEncoded);
        return spoolStreamBounded(request.getInputStream(), tempPrefix);
    }

    /**
     * Spools a raw {@link InputStream} up to {@link #maxObjectSizeBytes} bytes, cleaning up on failure.
     */
    public Path spoolStreamBounded(InputStream input, String tempPrefix) throws IOException {
        Path tempFile = Files.createTempFile(tempPrefix, ".tmp");
        try {
            long total = 0L;
            byte[] buffer = new byte[8192];
            try (InputStream is = input;
                 OutputStream os = Files.newOutputStream(tempFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                int read;
                while ((read = is.read(buffer)) != -1) {
                    total += read;
                    if (total > maxObjectSizeBytes) {
                        throw new PayloadTooLargeException(
                                "Upload stream exceeds maximum allowed object size of " + maxObjectSizeBytes + " bytes.");
                    }
                    os.write(buffer, 0, read);
                }
                os.flush();
            }
            return tempFile;
        } catch (Throwable t) {
            Files.deleteIfExists(tempFile);
            if (t instanceof IOException io) {
                throw io;
            }
            if (t instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(t);
        }
    }

    public static class UnsupportedUploadMediaTypeException extends RuntimeException {
        public UnsupportedUploadMediaTypeException(String message) {
            super(message);
        }
    }

    public static class PayloadTooLargeException extends RuntimeException {
        public PayloadTooLargeException(String message) {
            super(message);
        }
    }
}
