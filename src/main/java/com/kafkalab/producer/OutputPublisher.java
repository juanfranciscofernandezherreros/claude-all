package com.kafkalab.producer;

import com.kafkalab.metrics.LabMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OutputPublisher {
    private final KafkaTemplate<String, String> template;
    private final LabMetrics metrics;
    private final MeterRegistry registry;

    public OutputPublisher(KafkaTemplate<String, String> template, LabMetrics metrics, MeterRegistry registry) {
        this.template = template; this.metrics = metrics; this.registry = registry;
    }

    public void publish(String topic, String key, String json) {
        Timer.Sample sample = metrics.startPublishing(registry);
        try {
            template.send(topic, key, json).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka publish was not acknowledged for key " + key, e);
        } finally {
            metrics.stopPublishing(sample);
        }
    }
}
