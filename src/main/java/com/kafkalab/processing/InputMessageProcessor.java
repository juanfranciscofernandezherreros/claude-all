package com.kafkalab.processing;

import com.kafkalab.config.LabProperties;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

/**
 * Applies input-message policy and builds the deterministic business output.
 * Kafka transport, offset handling, serialization and publication stay in the listener/producer adapters.
 */
@Service
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
        if (!violations.isEmpty() || input.schemaVersion() != 1 || !"BENCHMARK".equals(input.eventType())
                || input.payload().sequence() < 1
                || !input.messageId().endsWith("-" + input.payload().sequence())
                || input.payload().data().getBytes(StandardCharsets.UTF_8).length != properties.payloadBytes()) {
            String reason = violations.stream()
                    .map(v -> v.getPropertyPath() + " " + v.getMessage())
                    .collect(Collectors.joining("; "));
            if (reason.isBlank()) {
                reason = "Unsupported schemaVersion or eventType";
            }
            throw new IllegalArgumentException(reason);
        }

        Instant processedAt = Instant.now();
        workload.apply(input.payload().data(), properties.workload());
        return new OutputMessage(1, input.messageId(), input.createdAt(), processedAt,
                "OK", new OutputMessage.Result(input.payload().sequence(),
                checksum.sha256(input.payload().data())));
    }
}
