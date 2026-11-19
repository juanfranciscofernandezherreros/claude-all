package com.kafkalab.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.ChecksumService;
import com.kafkalab.processing.WorkloadProcessor;
import com.kafkalab.producer.OutputPublisher;
import com.kafkalab.metrics.LabMetrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "lab.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class InputMessageListener {
    private final ObjectMapper mapper;
    private final Validator validator;
    private final LabProperties properties;
    private final ChecksumService checksum;
    private final WorkloadProcessor workload;
    private final OutputPublisher publisher;
    private final LabMetrics metrics;
    private final MeterRegistry meterRegistry;

    public InputMessageListener(ObjectMapper mapper, Validator validator, LabProperties properties,
                                ChecksumService checksum, WorkloadProcessor workload, OutputPublisher publisher, LabMetrics metrics, MeterRegistry meterRegistry) {
        this.mapper = mapper; this.validator = validator; this.properties = properties;
        this.checksum = checksum; this.workload = workload; this.publisher = publisher;
        this.metrics = metrics; this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${lab.input-topic}", groupId = "kafka-lab")
    public void onMessage(String raw, Acknowledgment acknowledgment) {
        metrics.inputReceived();
        Timer.Sample processingSample = Timer.start(meterRegistry);
        final InputMessage input;
        try {
            input = mapper.readValue(raw, InputMessage.class);
        } catch (JsonProcessingException e) {
            publishDlt(raw, e);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            acknowledgment.acknowledge();
            return;
        }
        Set<ConstraintViolation<InputMessage>> violations = validator.validate(input);
        if (!violations.isEmpty() || input.schemaVersion() != 1 || !"BENCHMARK".equals(input.eventType())) {
            String reason = violations.stream().map(v -> v.getPropertyPath() + " " + v.getMessage()).collect(Collectors.joining("; "));
            if (reason.isBlank()) reason = "Unsupported schemaVersion or eventType";
            publishDlt(raw, new IllegalArgumentException(reason));
            acknowledgment.acknowledge();
            return;
        }
        Instant processedAt = Instant.now();
        workload.apply(input.payload().data(), properties.workload());
        OutputMessage output = new OutputMessage(1, input.messageId(), input.createdAt(), processedAt,
                "OK", new OutputMessage.Result(input.payload().sequence(), checksum.sha256(input.payload().data())));
        try {
            publisher.publish(properties.outputTopic(), input.messageId(), mapper.writeValueAsString(output));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize output", e);
        }
        metrics.outputPublished();
        metrics.stopProcessing(processingSample);
        acknowledgment.acknowledge();
    }

    private void publishDlt(String raw, Exception error) {
        try {
            DltMessage dlt = new DltMessage(raw, error.getClass().getSimpleName(),
                    String.valueOf(error.getMessage()), Instant.now());
            publisher.publish(properties.dltTopic(), "invalid-" + Integer.toHexString(raw.hashCode()),
                    mapper.writeValueAsString(dlt));
            metrics.deadLetter();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize dead-letter message", e);
        }
    }
}
