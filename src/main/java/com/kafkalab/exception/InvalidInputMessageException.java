package com.kafkalab.exception;

/**
 * Signals that an input message violates the Kafka lab message contract.
 * The consumer adapter may route this non-retryable input error to the DLT.
 */
public class InvalidInputMessageException extends RuntimeException {
    public InvalidInputMessageException(String message) {
        super(message);
    }
}
