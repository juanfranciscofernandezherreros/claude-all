package com.kafkalab.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.exception.KafkaPublishException;
import com.kafkalab.metrics.LabMetrics;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.ChecksumService;
import com.kafkalab.processing.InputMessageProcessor;
import com.kafkalab.processing.WorkloadProcessor;
import com.kafkalab.producer.OutputPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

class InputMessageListenerTest {
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final LabMetrics metrics = new LabMetrics(registry);
    private final OutputPublisher publisher = mock(OutputPublisher.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final Consumer<?, ?> consumer = mock(Consumer.class);
    private InputMessageListener listener;
    private AutoCloseable validationFactory;

    @BeforeEach
    void setUp() {
        var factory = Validation.buildDefaultValidatorFactory();
        validationFactory = factory;
        LabProperties properties = new LabProperties(100, 24, 1, 500, 65536, 4, 1024,
                LabProperties.DeliveryMode.AT_LEAST_ONCE, LabProperties.Workload.LIGHT,
                "input-topic", "output-topic", "dlt-topic");
        InputMessageProcessor processor = new InputMessageProcessor(factory.getValidator(), properties,
                new WorkloadProcessor(), new ChecksumService());
        listener = new InputMessageListener(mapper, properties, processor, publisher,
                mock(KafkaTemplate.class), metrics, registry);
    }

    @AfterEach
    void tearDown() throws Exception {
        validationFactory.close();
        registry.close();
    }

    @Test
    void malformedJsonIsPublishedToDltBeforeAcknowledgingInput() throws Exception {
        String raw = "{not-json";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("input-topic", 0, 0L, "key-1", raw);

        listener.onMessages(List.of(record), acknowledgment, consumer);

        ArgumentCaptor<List> batch = ArgumentCaptor.forClass(List.class);
        verify(publisher).publishBatch(batch.capture());
        ProducerRecord<String, String> published = (ProducerRecord<String, String>) batch.getValue().get(0);
        assertEquals("dlt-topic", published.topic());
        DltMessage dlt = mapper.readValue(published.value(), DltMessage.class);
        assertEquals(raw, dlt.originalPayload());
        assertEquals("JsonParseException", dlt.errorType());
        verify(acknowledgment).acknowledge();
        assertEquals(1.0, registry.get("dlt_total").counter().count());
    }

    @Test
    void validInputPublishesMatchingOutputBeforeAcknowledging() throws Exception {
        String payload = "x".repeat(1024);
        InputMessage input = new InputMessage(1, "run-1", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(1, payload));
        ConsumerRecord<String, String> record = new ConsumerRecord<>("input-topic", 0, 0L,
                input.messageId(), mapper.writeValueAsString(input));

        listener.onMessages(List.of(record), acknowledgment, consumer);

        ArgumentCaptor<List> batch = ArgumentCaptor.forClass(List.class);
        verify(publisher).publishBatch(batch.capture());
        ProducerRecord<String, String> published = (ProducerRecord<String, String>) batch.getValue().get(0);
        assertEquals("output-topic", published.topic());
        assertEquals(input.messageId(), published.key());
        OutputMessage output = mapper.readValue(published.value(), OutputMessage.class);
        assertEquals(input.messageId(), output.messageId());
        assertEquals("OK", output.status());
        assertEquals(1L, output.result().sequence());
        assertEquals(new ChecksumService().sha256(payload), output.result().checksum());
        verify(acknowledgment).acknowledge();
    }

    @Test
    void publishesAllRecordsAsOneAcknowledgedBatch() throws Exception {
        List<ConsumerRecord<String, String>> records = List.of(
                inputRecord("run-batch-1", 1, 0),
                inputRecord("run-batch-2", 2, 1));

        listener.onMessages(records, acknowledgment, consumer);

        ArgumentCaptor<List> batch = ArgumentCaptor.forClass(List.class);
        verify(publisher).publishBatch(batch.capture());
        assertEquals(2, batch.getValue().size());
        verify(acknowledgment, times(1)).acknowledge();
        assertEquals(2.0, registry.get("output_published_total").counter().count());
    }

    @Test
    void exactlyOncePublishesBatchAndOffsetsWithinOneTransactionWithoutManualAck() throws Exception {
        var factory = Validation.buildDefaultValidatorFactory();
        try {
            LabProperties properties = new LabProperties(100, 24, 1, 500, 65536, 4, 1024,
                    LabProperties.DeliveryMode.EXACTLY_ONCE, LabProperties.Workload.LIGHT,
                    "input-topic", "output-topic", "dlt-topic");
            InputMessageProcessor processor = new InputMessageProcessor(factory.getValidator(), properties,
                    new WorkloadProcessor(), new ChecksumService());
            KafkaTemplate<String, String> transactionalTemplate = mock(KafkaTemplate.class);
            KafkaOperations<String, String> operations = mock(KafkaOperations.class);
            ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata("kafka-lab");
            when(consumer.groupMetadata()).thenReturn(groupMetadata);
            doReturn(CompletableFuture.completedFuture(null))
                    .when(operations).send(any(ProducerRecord.class));
            doAnswer(invocation -> {
                KafkaOperations.OperationsCallback<String, String, Boolean> callback =
                        invocation.getArgument(0);
                return callback.doInOperations(operations);
            }).when(transactionalTemplate).executeInTransaction(any());

            InputMessageListener transactionalListener = new InputMessageListener(mapper, properties, processor,
                    publisher, transactionalTemplate, metrics, registry);
            List<ConsumerRecord<String, String>> records = List.of(
                    inputRecord("run-eos-1", 1, 0),
                    inputRecord("run-eos-2", 2, 1));

            transactionalListener.onMessages(records, acknowledgment, consumer);

            verify(transactionalTemplate, times(1)).executeInTransaction(any());
            verify(operations, times(2)).send(any(ProducerRecord.class));
            verify(operations).sendOffsetsToTransaction(
                    argThat(offsets -> offsets.size() == 1
                            && offsets.get(new TopicPartition("input-topic", 0)).offset() == 2),
                    eq(groupMetadata));
            verify(acknowledgment, never()).acknowledge();
            verify(publisher, never()).publishBatch(anyList());
            assertEquals(2.0, registry.get("output_published_total").counter().count());
        } finally {
            factory.close();
        }
    }

    @Test
    void unsupportedSchemaAndEventTypeAreDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(2, "run-2", Instant.now(), "UNKNOWN",
                new InputMessage.Payload(2, "x".repeat(1024)));
        DltMessage dlt = publishAndCaptureDlt(invalid, 1L);
        assertEquals("InvalidInputMessageException", dlt.errorType());
        assertTrue(dlt.errorMessage().contains("Unsupported schemaVersion or eventType"));
    }

    @Test
    void wrongPayloadSizeIsDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(1, "run-3", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(3, "short"));
        DltMessage dlt = publishAndCaptureDlt(invalid, 2L);
        assertEquals("InvalidInputMessageException", dlt.errorType());
    }

    @Test
    void messageIdSequenceMismatchIsDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(1, "run-4-99", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(4, "x".repeat(1024)));
        DltMessage dlt = publishAndCaptureDlt(invalid, 3L);
        assertEquals("InvalidInputMessageException", dlt.errorType());
    }

    @Test
    void failedOutputPublishDoesNotAcknowledgeInputSoKafkaCanRetry() throws Exception {
        ConsumerRecord<String, String> record = inputRecord("run-retry-1", 1, 9L);
        doThrow(new KafkaPublishException("broker unavailable", new RuntimeException("broker unavailable")))
                .when(publisher).publishBatch(anyList());

        KafkaPublishException failure = assertThrows(KafkaPublishException.class,
                () -> listener.onMessages(List.of(record), acknowledgment, consumer));

        assertEquals("broker unavailable", failure.getMessage());
        assertEquals("broker unavailable", failure.getCause().getMessage());
        verify(acknowledgment, never()).acknowledge();
        verify(publisher).publishBatch(anyList());
    }

    private DltMessage publishAndCaptureDlt(InputMessage invalid, long offset) throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("input-topic", 0, offset,
                invalid.messageId(), mapper.writeValueAsString(invalid));
        listener.onMessages(List.of(record), acknowledgment, consumer);
        ArgumentCaptor<List> batch = ArgumentCaptor.forClass(List.class);
        verify(publisher).publishBatch(batch.capture());
        ProducerRecord<String, String> published = (ProducerRecord<String, String>) batch.getValue().get(0);
        assertEquals("dlt-topic", published.topic());
        verify(acknowledgment).acknowledge();
        return mapper.readValue(published.value(), DltMessage.class);
    }

    private ConsumerRecord<String, String> inputRecord(String messageId, long sequence, long offset)
            throws Exception {
        InputMessage input = new InputMessage(1, messageId, Instant.now(), "BENCHMARK",
                new InputMessage.Payload(sequence, "x".repeat(1024)));
        return new ConsumerRecord<>("input-topic", 0, offset, messageId, mapper.writeValueAsString(input));
    }
}
