package com.kafkalab.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class LabMetricsTest {
    @Test
    void recordsCountersForInputOutputFailuresRetriesAndDeadLetters() {
        try (SimpleMeterRegistry registry = new SimpleMeterRegistry()) {
            LabMetrics metrics = new LabMetrics(registry);
            metrics.inputReceived();
            metrics.inputReceived();
            metrics.outputPublished();
            metrics.failed();
            metrics.retry();
            metrics.retry();
            metrics.deadLetter();

            assertEquals(2.0, registry.get("input_received_total").counter().count());
            assertEquals(1.0, registry.get("output_published_total").counter().count());
            assertEquals(1.0, registry.get("processing_failed_total").counter().count());
            assertEquals(2.0, registry.get("retry_total").counter().count());
            assertEquals(1.0, registry.get("dlt_total").counter().count());
        }
    }

    @Test
    void recordsProcessingPublishingAndNonNegativeEndToEndDurations() {
        try (SimpleMeterRegistry registry = new SimpleMeterRegistry()) {
            LabMetrics metrics = new LabMetrics(registry);
            metrics.stopProcessing(metrics.startProcessing(registry));
            metrics.stopPublishing(metrics.startPublishing(registry));
            metrics.recordEndToEnd(Duration.ofMillis(5));
            metrics.recordEndToEnd(Duration.ofNanos(-1));

            assertEquals(1, registry.get("processing_duration").timer().count());
            assertEquals(1, registry.get("publish_duration").timer().count());
            assertEquals(1, registry.get("end_to_end_latency").timer().count());
            assertTrue(registry.get("end_to_end_latency").timer().totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) >= 5);
        }
    }
}
