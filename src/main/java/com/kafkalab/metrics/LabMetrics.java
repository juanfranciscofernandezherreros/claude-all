package com.kafkalab.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class LabMetrics {
    private final Counter input;
    private final Counter output;
    private final Counter failed;
    private final Counter retries;
    private final Counter dlt;
    private final Timer processing;
    private final Timer publishing;
    private final Timer endToEnd;

    public LabMetrics(MeterRegistry registry) {
        input = Counter.builder("input_received_total").description("Input records received").register(registry);
        output = Counter.builder("output_published_total").description("Output records acknowledged").register(registry);
        failed = Counter.builder("processing_failed_total").description("Processing failures").register(registry);
        retries = Counter.builder("retry_total").description("Kafka retry attempts").register(registry);
        dlt = Counter.builder("dlt_total").description("Dead-letter records published").register(registry);
        processing = timer(registry, "processing_duration");
        publishing = timer(registry, "publish_duration");
        endToEnd = timer(registry, "end_to_end_latency");
    }
    private Timer timer(MeterRegistry registry, String name) {
        return Timer.builder(name).publishPercentileHistogram().publishPercentiles(0.5, 0.95, 0.99).register(registry);
    }
    public void inputReceived() { input.increment(); }
    public void outputPublished() { output.increment(); }
    public void failed() { failed.increment(); }
    public void retry() { retries.increment(); }
    public void deadLetter() { dlt.increment(); }
    public Timer.Sample startProcessing(MeterRegistry registry) { return Timer.start(registry); }
    public void stopProcessing(Timer.Sample sample) { sample.stop(processing); }
    public Timer.Sample startPublishing(MeterRegistry registry) { return Timer.start(registry); }
    public void stopPublishing(Timer.Sample sample) { sample.stop(publishing); }
    public void recordEndToEnd(Duration duration) { if (!duration.isNegative()) endToEnd.record(duration); }
}
