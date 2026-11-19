package com.kafkalab.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties(prefix = "lab")
@Validated
public record LabProperties(
        @Positive long messages,
        @Min(1) @Max(24) int partitions,
        @Min(1) @Max(20) int concurrency,
        @Min(1) @Max(10000) int batchSize,
        @Positive int producerBatchBytes,
        @Min(1) @Max(64) int generatorThreads,
        @Positive int payloadBytes,
        @NotNull DeliveryMode deliveryMode,
        @NotNull Workload workload,
        @NotBlank String inputTopic,
        @NotBlank String outputTopic,
        @NotBlank String dltTopic) {
    public enum DeliveryMode { AT_LEAST_ONCE, EXACTLY_ONCE }
    public enum Workload { LIGHT, MEDIUM, HEAVY }

    @AssertTrue(message = "CONCURRENCY must be one of 1, 2, 5, 10, 20")
    public boolean isConcurrencySupported() {
        return Set.of(1, 2, 5, 10, 20).contains(concurrency);
    }

    @AssertTrue(message = "PAYLOAD_BYTES must be 1024, 5120, 10240, 51200 or 102400")
    public boolean isPayloadSizeSupported() {
        return Set.of(1024, 5120, 10240, 51200, 102400).contains(payloadBytes);
    }

    @AssertTrue(message = "PARTITIONS is fixed at 24")
    public boolean isPartitionCountSupported() {
        return partitions == 24;
    }
}
