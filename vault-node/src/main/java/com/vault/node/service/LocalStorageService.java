package com.vault.node.service;

import com.vault.common.dto.WriteResult;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Service
@Slf4j
public class LocalStorageService {

    @Value("${vault.node.data-dir}")
    private String dataDirStr;

    private Path objectsDir;
    private Path tmpDir;
    private Path tombstonesDir;

    @PostConstruct
    public void init() throws IOException {
        Path baseDir = Paths.get(dataDirStr);
        objectsDir = baseDir.resolve("objects");
        tmpDir = baseDir.resolve("tmp");
        tombstonesDir = baseDir.resolve("tombstones");

        Files.createDirectories(objectsDir);
        Files.createDirectories(tmpDir);
        Files.createDirectories(tombstonesDir);

        cleanTmpDir();
    }

    public void cleanTmpDir() {
        try {
            Files.list(tmpDir).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    log.warn("Failed to delete stale tmp file: {}", path);
                }
            });
            log.info("Cleaned temporary directory on startup.");
        } catch (IOException e) {
            log.error("Failed to clean tmp directory", e);
        }
    }

    public WriteResult store(String objectId, long version, InputStream data) throws IOException, NoSuchAlgorithmException {
        // e.g. cat.jpg.v1.partial
        String fileName = objectId.replace("/", "_") + ".v" + version;
        Path tempPath = tmpDir.resolve(fileName + ".partial");
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long sizeBytes = 0;
        
        // 1. Write to temp file AND compute SHA-256 in one pass
        try (DigestInputStream dis = new DigestInputStream(data, digest);
             OutputStream os = Files.newOutputStream(tempPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dis.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
                sizeBytes += bytesRead;
            }
        }
        
        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        String checksum = sb.toString();

        // 2. Determine final path using 2-level hash prefix to avoid million files in one dir
        // e.g., ab/cd/cat.jpg.v1.dat
        String hashPrefix = checksum.substring(0, 4);
        Path finalDir = objectsDir.resolve(hashPrefix.substring(0, 2)).resolve(hashPrefix.substring(2, 4));
        Files.createDirectories(finalDir);
        
        Path finalPath = finalDir.resolve(fileName + ".dat");

        // 3. Atomic rename (StandardCopyOption.ATOMIC_MOVE)
        try {
            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            // Fallback if filesystem doesn't support atomic moves
            Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING);
        }
        
        log.info("Stored file locally: {} (v{}), size: {} bytes, sha256: {}", objectId, version, sizeBytes, checksum);
        return new WriteResult(checksum, sizeBytes);
    }
    
    public Path getFilePath(String objectId, long version, String checksum) {
        String fileName = objectId.replace("/", "_") + ".v" + version + ".dat";
        String hashPrefix = checksum.substring(0, 4);
        return objectsDir.resolve(hashPrefix.substring(0, 2)).resolve(hashPrefix.substring(2, 4)).resolve(fileName);
    }

    public InputStream read(String objectId, long version, String checksum) throws IOException {
        Path path = getFilePath(objectId, version, checksum);
        if (!Files.exists(path)) {
            return null;
        }
        return Files.newInputStream(path);
    }
    
    public byte[] readFullyVerified(String objectId, long version, String expectedChecksum) throws IOException, NoSuchAlgorithmException {
        Path path = getFilePath(objectId, version, expectedChecksum);
        if (!Files.exists(path)) {
            return null;
        }
        
        byte[] data = Files.readAllBytes(path);
        String actualChecksum = com.vault.common.util.ChecksumUtil.sha256(data);
        
        if (!actualChecksum.equals(expectedChecksum)) {
            log.error("CORRUPTION DETECTED on read! Object {} v{}. Expected: {}, Actual: {}", objectId, version, expectedChecksum, actualChecksum);
            throw new RuntimeException("CORRUPT_FILE");
        }
        
        return data;
    }
    
    /**
     * Streams a file to the given OutputStream while computing SHA-256 on the fly.
     * Throws RuntimeException("CORRUPT_FILE") if the checksum does not match AFTER streaming.
     * This avoids loading the entire file into heap memory.
     */
    public void streamVerified(String objectId, long version, String expectedChecksum, java.io.OutputStream out) throws IOException, NoSuchAlgorithmException {
        Path path = getFilePath(objectId, version, expectedChecksum);
        if (!Files.exists(path)) {
            throw new java.io.FileNotFoundException("File not found: " + path);
        }
        
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream is = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
                out.write(buffer, 0, bytesRead);
            }
        }
        
        byte[] hashBytes = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        String actualChecksum = sb.toString();
        
        if (!actualChecksum.equals(expectedChecksum)) {
            log.error("CORRUPTION DETECTED during stream! Object {} v{}. Expected: {}, Actual: {}", objectId, version, expectedChecksum, actualChecksum);
            throw new RuntimeException("CORRUPT_FILE");
        }
    }
    
    public long getFileSize(String objectId, long version, String checksum) {
        Path path = getFilePath(objectId, version, checksum);
        try {
            return Files.exists(path) ? Files.size(path) : -1;
        } catch (IOException e) {
            return -1;
        }
    }

    public java.util.List<Path> listAllDataFiles() {
        try (java.util.stream.Stream<Path> stream = Files.walk(objectsDir)) {
            return stream.filter(Files::isRegularFile)
                         .filter(p -> p.toString().endsWith(".dat"))
                         .toList();
        } catch (IOException e) {
            log.error("Failed to list data files", e);
            return java.util.Collections.emptyList();
        }
    }
    
    public void delete(String objectId, long version) {
        // Delete all versions for this objectId on disk (or just place a tombstone marker)
        // For simplicity, we can just delete the .dat files that match objectId
        try {
            java.util.List<Path> files = listAllDataFiles();
            for (Path p : files) {
                String fileName = p.getFileName().toString();
                if (fileName.startsWith(objectId.replace("/", "_") + ".v")) {
                    Files.deleteIfExists(p);
                }
            }
            
            // Create a .tombstone marker
            Path marker = tombstonesDir.resolve(objectId.replace("/", "_") + ".v" + version + ".tombstone");
            Files.writeString(marker, "TOMBSTONE");
            
            log.info("Deleted local files and created tombstone for {}", objectId);
        } catch (Exception e) {
            log.error("Failed to delete local object {}", objectId, e);
        }
    }
}
