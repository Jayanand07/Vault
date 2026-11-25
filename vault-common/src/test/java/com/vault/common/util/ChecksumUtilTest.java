package com.vault.common.util;

import org.junit.jupiter.api.Test;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ChecksumUtilTest {

    @Test
    void testSha256ByteArray() {
        byte[] data = "hello world".getBytes();
        // SHA-256 of "hello world"
        String expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        
        String actual = ChecksumUtil.sha256(data);
        assertEquals(expected, actual);
    }

    @Test
    void testSha256InputStream() throws IOException {
        byte[] data = "hello world".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(data);
        
        String expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
        String actual = ChecksumUtil.sha256(is);
        
        assertEquals(expected, actual);
    }
}
