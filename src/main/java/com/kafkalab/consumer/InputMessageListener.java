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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
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
        this.mapper = mapper;
        this.properties = properties;
        this.processor = processor;
        this.publisher = publisher;
        this.kafkaTemplate = kafkaTemplate;
        this.metrics = metrics;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(topics = "${lab.input-topic}", groupId = "kafka-lab")
    public void onMessages(List<ConsumerRecord<String, String>> records,
                           Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
        List<ProducerRecord<String, String>> publications = new ArrayList<>(records.size());
        List<ProcessedOutput> processedOutputs = new ArrayList<>(records.size());
        List<Timer.Sample> processingSamples = new ArrayList<>(records.size());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        int deadLetterRecords = 0;

        for (ConsumerRecord<String, String> record : records) {
            metrics.inputReceived();
            Timer.Sample processingSample = metrics.startProcessing(meterRegistry);
            processingSamples.add(processingSample);
            offsets.merge(new TopicPartition(record.topic(), record.partition()),
                    new OffsetAndMetadata(record.offset() + 1),
                    (current, next) -> current.offset() >= next.offset() ? current : next);

            String raw = record.value();
            final InputMessage input;
            try {
                input = mapper.readValue(raw, InputMessage.class);
            } catch (JsonProcessingException invalidJson) {
                publications.add(deadLetterRecord(raw, invalidJson));
                metrics.failed();
                deadLetterRecords++;
                continue;
            }

            final OutputMessage output;
            try {
                output = processor.process(input);
            } catch (InvalidInputMessageException invalidInput) {
                publications.add(deadLetterRecord(raw, invalidInput));
                metrics.failed();
                deadLetterRecords++;
                continue;
            }

            try {
                String json = mapper.writeValueAsString(output);
                publications.add(new ProducerRecord<>(properties.outputTopic(), input.messageId(), json));
                processedOutputs.add(new ProcessedOutput(input, output));
            } catch (JsonProcessingException serializationFailure) {
                throw new MessageSerializationException(
                        "Could not serialize output message " + input.messageId(), serializationFailure);
            }
        }

        try {
            if (properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE) {
                publishTransactionally(publications, offsets, consumer);
            } else {
                publisher.publishBatch(publications);
                acknowledgment.acknowledge();
            }
        } catch (RuntimeException publishFailure) {
            processingSamples.forEach(metrics::stopProcessing);
            throw publishFailure;
        }

        for (ProcessedOutput processed : processedOutputs) {
            metrics.outputPublished();
            metrics.recordEndToEnd(Duration.between(
                    processed.input().createdAt(), processed.output().processedAt()));
        }
        for (int i = 0; i < deadLetterRecords; i++) {
            metrics.deadLetter();
        }
        processingSamples.forEach(metrics::stopProcessing);
    }

    private ProducerRecord<String, String> deadLetterRecord(String raw, Exception error) {
        DltMessage dlt = new DltMessage(raw, error.getClass().getSimpleName(),
                String.valueOf(error.getMessage()), Instant.now());
        try {
            return new ProducerRecord<>(properties.dltTopic(),
                    "invalid-" + Integer.toHexString(raw.hashCode()), mapper.writeValueAsString(dlt));
        } catch (JsonProcessingException serializationFailure) {
            throw new MessageSerializationException("Could not serialize dead-letter message", serializationFailure);
        }
    }

    private void publishTransactionally(List<ProducerRecord<String, String>> publications,
                                        Map<TopicPartition, OffsetAndMetadata> offsets,
                                        Consumer<?, ?> consumer) {
        Timer.Sample publishingSample = metrics.startPublishing(meterRegistry);
        try {
            kafkaTemplate.executeInTransaction(operations -> {
                List<CompletableFuture<?>> acknowledgements = new ArrayList<>(publications.size());
                for (ProducerRecord<String, String> publication : publications) {
                    acknowledgements.add(operations.send(publication));
                }
                awaitAcknowledgements(acknowledgements);
                operations.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                return true;
            });
        } catch (KafkaPublishException publishFailure) {
            throw publishFailure;
        } catch (KafkaException kafkaFailure) {
            throw new KafkaPublishException("Transactional Kafka batch publication failed", kafkaFailure);
        } finally {
            metrics.stopPublishing(publishingSample);
        }
    }

    private void awaitAcknowledgements(List<CompletableFuture<?>> acknowledgements) {
        try {
            CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while waiting for transactional batch acknowledgements", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException("Transactional Kafka batch was not fully acknowledged", e);
        }
    }

    private record ProcessedOutput(InputMessage input, OutputMessage output) {}
}
