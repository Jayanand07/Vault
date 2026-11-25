package com.vault.common.exception;

/**
 * Thrown when a node attempts a leader-only metadata mutation without holding
 * an active, unexpired PostgreSQL-coordinated leader lease for the write term.
 */
public class StaleLeaderException extends RuntimeException {
    public StaleLeaderException(String message) {
        super(message);
    }
}
