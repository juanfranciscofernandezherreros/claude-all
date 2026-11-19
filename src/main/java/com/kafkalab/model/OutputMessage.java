package com.kafkalab.model;

import java.time.Instant;

public record OutputMessage(
        int schemaVersion,
        String messageId,
        Instant createdAt,
        Instant processedAt,
        String status,
        Result result) {
    public record Result(long sequence, String checksum) {}
}
