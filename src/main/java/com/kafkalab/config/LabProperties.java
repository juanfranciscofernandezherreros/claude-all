package com.kafkalab.config;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
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
        @Min(1) int payloadBytes,
        DeliveryMode deliveryMode,
        Workload workload,
        String inputTopic,
        String outputTopic,
        String dltTopic) {
    public enum DeliveryMode { AT_LEAST_ONCE, EXACTLY_ONCE }
    public enum Workload { LIGHT, MEDIUM, HEAVY }
}
