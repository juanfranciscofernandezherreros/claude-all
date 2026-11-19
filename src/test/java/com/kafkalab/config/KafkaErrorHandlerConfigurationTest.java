package com.kafkalab.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.util.backoff.BackOffExecution;

class KafkaErrorHandlerConfigurationTest {
    @Test
    void configuresThreeExponentialRetriesWithCappedIntervals() {
        BackOffExecution execution = KafkaErrorHandlerConfiguration.retryBackOff().start();
        assertEquals(1_000L, execution.nextBackOff());
        assertEquals(2_000L, execution.nextBackOff());
        assertEquals(4_000L, execution.nextBackOff());
        assertEquals(BackOffExecution.STOP, execution.nextBackOff());
    }
}
