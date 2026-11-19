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
}
