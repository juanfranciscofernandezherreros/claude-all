package com.kafkalab.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.metrics.LabMetrics;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.InputMessageProcessor;
import com.kafkalab.producer.OutputPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    private final LabProperties properties;
    private final InputMessageProcessor processor;
    private final OutputPublisher publisher;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final LabMetrics metrics;
    private final MeterRegistry meterRegistry;

    public InputMessageListener(ObjectMapper mapper, LabProperties properties, InputMessageProcessor processor,
                                OutputPublisher publisher, KafkaTemplate<String, String> kafkaTemplate,
                                LabMetrics metrics, MeterRegistry meterRegistry) {
        this.mapper = mapper; this.properties = properties; this.processor = processor;
        this.publisher = publisher;
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
        final OutputMessage output;
        try {
            output = processor.process(input);
        } catch (IllegalArgumentException e) {
            publishDlt(raw, e, record, consumer, acknowledgment);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            return;
        }
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
