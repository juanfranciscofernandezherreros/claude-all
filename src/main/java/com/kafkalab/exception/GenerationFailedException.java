package com.kafkalab.exception;

/** Signals that the benchmark generator could not serialize or publish its assigned records. */
public class GenerationFailedException extends RuntimeException {
    public GenerationFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
