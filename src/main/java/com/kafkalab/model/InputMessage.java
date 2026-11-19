package com.kafkalab.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Instant;

public record InputMessage(
        @Positive int schemaVersion,
        @NotBlank String messageId,
        @NotNull Instant createdAt,
        @NotBlank String eventType,
        @NotNull @Valid Payload payload) {
    public record Payload(@Positive long sequence, @NotNull String data) {}
}
