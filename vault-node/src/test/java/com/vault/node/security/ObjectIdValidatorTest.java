package com.vault.node.security;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ObjectIdValidatorTest {

    private final ObjectIdValidator validator = new ObjectIdValidator();

    @Test
    void testValidObjectIds() {
        assertTrue(validator.isValid("my-file.txt"));
        assertTrue(validator.isValid("user_avatar_123.png"));
        assertTrue(validator.isValid("folder/subfolder/file.dat"));
    }

    @Test
    void testInvalidObjectIds() {
        assertFalse(validator.isValid(null));
        assertFalse(validator.isValid(""));
        assertFalse(validator.isValid("../etc/passwd"));
        assertFalse(validator.isValid("/absolute/path"));
        assertFalse(validator.isValid("ends-with-slash/"));
        assertFalse(validator.isValid("has spaces.txt"));
        assertFalse(validator.isValid("special_chars_!@#.txt"));
    }
}
