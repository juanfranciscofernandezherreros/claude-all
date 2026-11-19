package com.kafkalab.exception;

/** Signals that a CLI option is missing, malformed, or outside the supported range. */
public class InvalidCommandLineException extends RuntimeException {
    public InvalidCommandLineException(String message) {
        super(message);
    }

    public InvalidCommandLineException(String message, Throwable cause) {
        super(message, cause);
    }
}
