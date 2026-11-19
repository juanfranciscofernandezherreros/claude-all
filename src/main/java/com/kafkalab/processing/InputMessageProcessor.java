package com.kafkalab.processing;

import com.kafkalab.config.LabProperties;
import com.kafkalab.exception.InvalidInputMessageException;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/**
 * Applies input-message policy and builds the deterministic business output.
 * Kafka transport, offset handling, serialization and publication stay in the listener/producer adapters.
 */
@Component
public class InputMessageProcessor {
    private final Validator validator;
    private final LabProperties properties;
    private final WorkloadProcessor workload;
    private final ChecksumService checksum;

    public InputMessageProcessor(Validator validator, LabProperties properties,
                                 WorkloadProcessor workload, ChecksumService checksum) {
        this.validator = validator;
        this.properties = properties;
        this.workload = workload;
        this.checksum = checksum;
    }

    public OutputMessage process(InputMessage input) {
        Set<ConstraintViolation<InputMessage>> violations = validator.validate(input);
        boolean hasBeanValidationErrors = !violations.isEmpty();
        boolean hasUnsupportedSchemaVersion = input.schemaVersion() != 1;
        boolean hasUnsupportedEventType = !"BENCHMARK".equals(input.eventType());
        boolean hasInvalidSequence = input.payload().sequence() < 1;
        boolean hasMessageIdSequenceMismatch =
                !input.messageId().endsWith("-" + input.payload().sequence());
        boolean hasUnexpectedPayloadSize =
                input.payload().data().getBytes(StandardCharsets.UTF_8).length != properties.payloadBytes();

        if (hasBeanValidationErrors || hasUnsupportedSchemaVersion || hasUnsupportedEventType
                || hasInvalidSequence || hasMessageIdSequenceMismatch || hasUnexpectedPayloadSize) {
            String reason = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining("; "));
            if (reason.isBlank()) {
                reason = "Unsupported schemaVersion or eventType";
            }
            throw new InvalidInputMessageException(reason);
        }

        Instant processedAt = Instant.now();
        workload.apply(input.payload().data(), properties.workload());
        return new OutputMessage(1, input.messageId(), input.createdAt(), processedAt,
                "OK", new OutputMessage.Result(input.payload().sequence(),
                checksum.sha256(input.payload().data())));
    }
}
