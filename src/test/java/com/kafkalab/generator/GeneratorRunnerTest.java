package com.kafkalab.generator;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class GeneratorRunnerTest {
    @Test
    void acceptsAcknowledgedBatchBeforeDeadline() {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
        assertDoesNotThrow(() -> GeneratorRunner.awaitAcknowledgements(
                List.of((java.util.concurrent.Future<?>) CompletableFuture.completedFuture("ack")), deadline));
    }

    @Test
    void failsBatchWhenBrokerAcknowledgementDoesNotArriveBeforeDeadline() {
        CompletableFuture<String> pending = new CompletableFuture<>();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(25);

        CompletionException failure = assertThrows(CompletionException.class,
                () -> GeneratorRunner.awaitAcknowledgements(List.of((java.util.concurrent.Future<?>) pending), deadline));

        assertTrue(failure.getMessage().contains("global timeout"));
        assertInstanceOf(java.util.concurrent.TimeoutException.class, failure.getCause());
    }
    @Test
    void allocatesSingleMessageExactlyOnceAcrossManyWorkers() {
        assertEquals(List.of(1L), GeneratorRunner.blockStartsForWorker(1, 0, 20, 500));
        for (int worker = 1; worker < 20; worker++) {
            assertTrue(GeneratorRunner.blockStartsForWorker(1, worker, 20, 500).isEmpty());
        }
    }

    @Test
    void distributesTenThousandMessagesWithoutGapsOrOverlaps() {
        for (int workers : List.of(1, 2, 5, 10, 20, 64)) {
            for (int batchSize : List.of(1, 500, 1000)) {
                boolean[] seen = new boolean[10_001];
                for (int worker = 0; worker < workers; worker++) {
                    for (long blockStart : GeneratorRunner.blockStartsForWorker(10_000, worker, workers, batchSize)) {
                        long blockEnd = Math.min(10_000, blockStart + batchSize - 1L);
                        for (long sequence = blockStart; sequence <= blockEnd; sequence++) {
                            assertFalse(seen[(int) sequence], "duplicate sequence " + sequence);
                            seen[(int) sequence] = true;
                        }
                    }
                }
                for (int sequence = 1; sequence <= 10_000; sequence++) {
                    assertTrue(seen[sequence], "missing sequence " + sequence);
                }
            }
        }
    }

    @Test
    void createsEverySupportedPayloadSizeExactly() {
        for (int size : List.of(1024, 5120, 10240, 51200, 102400)) {
            String payload = GeneratorRunner.payloadFor(size);
            assertEquals(size, payload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
        }
    }

    @Test
    void failsWhenKafkaBrokerIsUnavailableBeforeGlobalDeadline() {
        java.util.Properties config = new java.util.Properties();
        config.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1");
        config.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        config.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 250);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 100);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 500);
        config.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 0);

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(config)) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
            RuntimeException failure = assertThrows(RuntimeException.class, () -> {
                var acknowledgement = producer.send(
                        new org.apache.kafka.clients.producer.ProducerRecord<>("unreachable-topic", "key", "value"));
                GeneratorRunner.awaitAcknowledgements(List.of(acknowledgement), deadline);
            });
            assertNotNull(failure.getMessage());
        }
    }

    @Test
    void rejectsInvalidBlockAllocationArguments() {
        assertThrows(IllegalArgumentException.class, () -> GeneratorRunner.blockStartsForWorker(-1, 0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> GeneratorRunner.blockStartsForWorker(10, 1, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> GeneratorRunner.blockStartsForWorker(10, 0, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> GeneratorRunner.blockStartsForWorker(10, 0, 1, 0));
    }

}
