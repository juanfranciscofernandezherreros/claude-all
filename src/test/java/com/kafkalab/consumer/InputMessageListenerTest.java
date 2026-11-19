package com.kafkalab.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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

        listener.onMessage(record, acknowledgment, consumer);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(publisher).publish(eq("dlt-topic"), startsWith("invalid-"), json.capture());
        DltMessage dlt = mapper.readValue(json.getValue(), DltMessage.class);
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

        listener.onMessage(record, acknowledgment, consumer);

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(publisher).publish(eq("output-topic"), eq(input.messageId()), json.capture());
        OutputMessage output = mapper.readValue(json.getValue(), OutputMessage.class);
        assertEquals(input.messageId(), output.messageId());
        assertEquals("OK", output.status());
        assertEquals(1L, output.result().sequence());
        assertEquals(new ChecksumService().sha256(payload), output.result().checksum());
        verify(acknowledgment).acknowledge();
    }
    @Test
    void unsupportedSchemaAndEventTypeAreDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(2, "run-2", Instant.now(), "UNKNOWN",
                new InputMessage.Payload(2, "x".repeat(1024)));
        DltMessage dlt = publishAndCaptureDlt(invalid, 1L);
        assertEquals("IllegalArgumentException", dlt.errorType());
        assertTrue(dlt.errorMessage().contains("Unsupported schemaVersion or eventType"));
    }

    @Test
    void wrongPayloadSizeIsDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(1, "run-3", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(3, "short"));
        DltMessage dlt = publishAndCaptureDlt(invalid, 2L);
        assertEquals("IllegalArgumentException", dlt.errorType());
    }

    @Test
    void messageIdSequenceMismatchIsDeadLettered() throws Exception {
        InputMessage invalid = new InputMessage(1, "run-4-99", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(4, "x".repeat(1024)));
        DltMessage dlt = publishAndCaptureDlt(invalid, 3L);
        assertEquals("IllegalArgumentException", dlt.errorType());
    }

    @Test
    void failedOutputPublishDoesNotAcknowledgeInputSoKafkaCanRetry() throws Exception {
        String payload = "x".repeat(1024);
        InputMessage input = new InputMessage(1, "run-retry-1", Instant.now(), "BENCHMARK",
                new InputMessage.Payload(1, payload));
        ConsumerRecord<String, String> record = new ConsumerRecord<>("input-topic", 0, 9L,
                input.messageId(), mapper.writeValueAsString(input));
        doThrow(new IllegalStateException("broker unavailable"))
                .when(publisher).publish(eq("output-topic"), eq(input.messageId()), anyString());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> listener.onMessage(record, acknowledgment, consumer));

        assertEquals("broker unavailable", failure.getMessage());
        verify(acknowledgment, never()).acknowledge();
        verify(publisher).publish(eq("output-topic"), eq(input.messageId()), anyString());
    }

    private DltMessage publishAndCaptureDlt(InputMessage invalid, long offset) throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("input-topic", 0, offset,
                invalid.messageId(), mapper.writeValueAsString(invalid));
        listener.onMessage(record, acknowledgment, consumer);
        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(publisher).publish(eq("dlt-topic"), startsWith("invalid-"), json.capture());
        verify(acknowledgment).acknowledge();
        return mapper.readValue(json.getValue(), DltMessage.class);
    }

}
