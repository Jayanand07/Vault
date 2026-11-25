package com.vault.common.util;

import java.nio.charset.StandardCharsets;

/**
 * Deterministic, collision-free disk locator and SQL LIKE escaping helpers for object keys.
 */
public final class ObjectKey {

    private ObjectKey() {
    }

    /**
     * Returns the 64-character lowercase SHA-256 hex digest of the UTF-8 object key.
     */
    public static String keyHash(String objectId) {
        if (objectId == null) {
            throw new IllegalArgumentException("objectId must not be null");
        }
        return ChecksumUtil.sha256(objectId.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns the collision-free data filename {@code <keySha256>.v<version>.dat}.
     */
    public static String storageFileName(String objectId, long version) {
        return keyHash(objectId) + ".v" + version + ".dat";
    }

    /**
     * Returns the two-level directory shard {@code <h[0..2)>/<h[2..4)>}.
     */
    public static String shardSubdirectory(String keyHash) {
        return keyHash.substring(0, 2) + "/" + keyHash.substring(2, 4);
    }

    /**
     * Returns the relative storage path beneath the node's data directory.
     */
    public static String relativeStoragePath(String objectId, long version) {
        String hash = keyHash(objectId);
        return "objects/" + shardSubdirectory(hash) + "/" + hash + ".v" + version + ".dat";
    }

    /**
     * Returns the collision-free tombstone marker filename.
     */
    public static String tombstoneFileName(String objectId, long version) {
        return keyHash(objectId) + ".v" + version + ".tombstone";
    }

    /**
     * Escapes SQL LIKE wildcards ({@code \}, {@code %}, {@code _}) using backslash {@code \}.
     */
    public static String escapeLikePrefix(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(prefix.length());
        for (int i = 0; i < prefix.length(); i++) {
            char c = prefix.charAt(i);
            if (c == '\\' || c == '%' || c == '_') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
