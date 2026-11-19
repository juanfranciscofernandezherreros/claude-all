package com.kafkalab.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.metrics.LabMetrics;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.ChecksumService;
import com.kafkalab.processing.WorkloadProcessor;
import com.kafkalab.producer.OutputPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final LabMetrics metrics;
    private final MeterRegistry meterRegistry;

    public InputMessageListener(ObjectMapper mapper, Validator validator, LabProperties properties,
                                ChecksumService checksum, WorkloadProcessor workload, OutputPublisher publisher,
                                KafkaTemplate<String, String> kafkaTemplate, LabMetrics metrics, MeterRegistry meterRegistry) {
        this.mapper = mapper; this.validator = validator; this.properties = properties;
        this.checksum = checksum; this.workload = workload; this.publisher = publisher;
        this.kafkaTemplate = kafkaTemplate; this.metrics = metrics; this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${lab.input-topic}", groupId = "kafka-lab")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        String raw = record.value();
        metrics.inputReceived();
        Timer.Sample processingSample = metrics.startProcessing(meterRegistry);
        final InputMessage input;
        try {
            input = mapper.readValue(raw, InputMessage.class);
        } catch (JsonProcessingException e) {
            publishDlt(raw, e, record, consumer, acknowledgment);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            return;
        }
        Set<ConstraintViolation<InputMessage>> violations = validator.validate(input);
        if (!violations.isEmpty() || input.schemaVersion() != 1 || !"BENCHMARK".equals(input.eventType())
                || input.payload().sequence() < 1
                || !input.messageId().endsWith("-" + input.payload().sequence())
                || input.payload().data().getBytes(java.nio.charset.StandardCharsets.UTF_8).length != properties.payloadBytes()) {
            String reason = violations.stream().map(v -> v.getPropertyPath() + " " + v.getMessage()).collect(Collectors.joining("; "));
            if (reason.isBlank()) reason = "Unsupported schemaVersion or eventType";
            publishDlt(raw, new IllegalArgumentException(reason), record, consumer, acknowledgment);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            return;
        }
        Instant processedAt = Instant.now();
        workload.apply(input.payload().data(), properties.workload());
        OutputMessage output = new OutputMessage(1, input.messageId(), input.createdAt(), processedAt,
                "OK", new OutputMessage.Result(input.payload().sequence(), checksum.sha256(input.payload().data())));
        try {
            publishAndCommit(properties.outputTopic(), input.messageId(), mapper.writeValueAsString(output),
                    record, consumer, acknowledgment);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize output", e);
        }
        metrics.outputPublished();
        metrics.recordEndToEnd(Duration.between(input.createdAt(), processedAt));
        metrics.stopProcessing(processingSample);
    }

    private void publishDlt(String raw, Exception error, ConsumerRecord<String, String> record,
                            Consumer<?, ?> consumer, Acknowledgment acknowledgment) {
        try {
            DltMessage dlt = new DltMessage(raw, error.getClass().getSimpleName(),
                    String.valueOf(error.getMessage()), Instant.now());
            publishAndCommit(properties.dltTopic(), "invalid-" + Integer.toHexString(raw.hashCode()),
                    mapper.writeValueAsString(dlt), record, consumer, acknowledgment);
            metrics.deadLetter();
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Could not serialize dead-letter message", e);
        }
    }

    private void publishAndCommit(String topic, String key, String value, ConsumerRecord<String, String> record,
                                  Consumer<?, ?> consumer, Acknowledgment acknowledgment) {
        if (properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE) {
            kafkaTemplate.executeInTransaction(operations -> {
                try {
                    operations.send(topic, key, value).get(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IllegalStateException("Transactional Kafka publish failed", e);
                }
                operations.sendOffsetsToTransaction(
                        Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)),
                        consumer.groupMetadata());
                return true;
            });
        } else {
            publisher.publish(topic, key, value);
            acknowledgment.acknowledge();
        }
    }
}
