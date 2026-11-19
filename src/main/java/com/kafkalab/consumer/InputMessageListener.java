package com.kafkalab.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.exception.InvalidInputMessageException;
import com.kafkalab.exception.KafkaPublishException;
import com.kafkalab.exception.MessageSerializationException;
import com.kafkalab.metrics.LabMetrics;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.InputMessageProcessor;
import com.kafkalab.producer.OutputPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.KafkaException;
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
        } catch (InvalidInputMessageException e) {
            publishDlt(raw, e, record, consumer, acknowledgment);
            metrics.failed();
            metrics.stopProcessing(processingSample);
            return;
        }
        try {
            publishAndCommit(properties.outputTopic(), input.messageId(), mapper.writeValueAsString(output),
                    record, consumer, acknowledgment);
        } catch (JsonProcessingException e) {
            throw new MessageSerializationException("Could not serialize output message " + input.messageId(), e);
        }
        metrics.outputPublished();
        metrics.recordEndToEnd(Duration.between(input.createdAt(), output.processedAt()));
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
            throw new MessageSerializationException("Could not serialize dead-letter message", e);
        }
    }

    private void publishAndCommit(String topic, String key, String value, ConsumerRecord<String, String> record,
                                  Consumer<?, ?> consumer, Acknowledgment acknowledgment) {
        boolean isExactlyOnceDeliveryEnabled = properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE;
        if (isExactlyOnceDeliveryEnabled) {
            try {
                kafkaTemplate.executeInTransaction(operations -> {
                    try {
                        operations.send(topic, key, value).get(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new KafkaPublishException("Interrupted during transactional Kafka publish for topic " + topic, e);
                    } catch (ExecutionException | TimeoutException e) {
                        throw new KafkaPublishException("Transactional Kafka publish failed for topic " + topic, e);
                    }
                    operations.sendOffsetsToTransaction(
                            Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)),
                            consumer.groupMetadata());
                    return true;
                });
            } catch (KafkaPublishException publishFailure) {
                throw publishFailure;
            } catch (KafkaException kafkaFailure) {
                throw new KafkaPublishException("Transactional Kafka operation failed for topic " + topic, kafkaFailure);
            }
        } else {
            publisher.publish(topic, key, value);
            acknowledgment.acknowledge();
        }
    }
}
