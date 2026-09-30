package com.vault.common.util;

import com.google.common.hash.Hashing;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class ChecksumUtil {

    private ChecksumUtil() {}

    public static String sha256(byte[] data) {
        return Hashing.sha256().hashBytes(data).toString();
    }

    public static String sha256(InputStream data) throws IOException {
        // We use Guava's ByteSource for this or Spring's DigestUtils, but since we have Guava:
        byte[] buffer = new byte[8192];
        int read;
        var hasher = Hashing.sha256().newHasher();
        while ((read = data.read(buffer)) != -1) {
            hasher.putBytes(buffer, 0, read);
        }
        return hasher.hash().toString();
    }

    public static String sha256File(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            return sha256(is);
        }
    }
}
