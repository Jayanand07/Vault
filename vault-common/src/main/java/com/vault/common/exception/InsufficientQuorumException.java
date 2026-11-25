package com.vault.common.exception;

public class InsufficientQuorumException extends RuntimeException {
    public InsufficientQuorumException(String message) {
        super(message);
    }
}
