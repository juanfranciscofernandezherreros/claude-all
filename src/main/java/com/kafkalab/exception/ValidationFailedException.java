package com.kafkalab.exception;

/** Signals that independent benchmark validation did not satisfy its acceptance criteria. */
public class ValidationFailedException extends RuntimeException {
    public ValidationFailedException(String message) {
        super(message);
    }
}
