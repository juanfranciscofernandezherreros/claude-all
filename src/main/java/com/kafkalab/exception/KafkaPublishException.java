package com.kafkalab.exception;

/** Signals that Kafka did not confirm a required publication or transactional recovery. */
public class KafkaPublishException extends RuntimeException {
    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
