package com.kafkalab.exception;

/** Signals that the required SHA-256 implementation is unavailable at runtime. */
public class ChecksumUnavailableException extends RuntimeException {
    public ChecksumUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
