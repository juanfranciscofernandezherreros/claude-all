package com.kafkalab.exception;

/** Signals that a message required by the processing contract could not be serialized. */
public class MessageSerializationException extends RuntimeException {
    public MessageSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
