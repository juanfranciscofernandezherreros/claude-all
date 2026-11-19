package com.kafkalab.exception;

/** Signals that a message ID cannot be formed from the supplied run ID and sequence. */
public class InvalidMessageIdException extends RuntimeException {
    public InvalidMessageIdException(String message) {
        super(message);
    }
}
