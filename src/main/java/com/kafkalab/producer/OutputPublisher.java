package com.kafkalab.producer;

import com.kafkalab.exception.KafkaPublishException;
import com.kafkalab.metrics.LabMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OutputPublisher {
    private final KafkaTemplate<String, String> template;
    private final LabMetrics metrics;
    private final MeterRegistry registry;

    public OutputPublisher(KafkaTemplate<String, String> template, LabMetrics metrics, MeterRegistry registry) {
        this.template = template;
        this.metrics = metrics;
        this.registry = registry;
    }

    public void publish(String topic, String key, String json) {
        publishBatch(List.of(new ProducerRecord<>(topic, key, json)));
    }

    /**
     * Publishes a consumer batch without blocking once per record. The caller
     * acknowledges the input batch only after every broker acknowledgement arrives.
     */
    public void publishBatch(List<ProducerRecord<String, String>> records) {
        if (records.isEmpty()) return;
        Timer.Sample sample = metrics.startPublishing(registry);
        try {
            List<CompletableFuture<?>> acknowledgements = new ArrayList<>(records.size());
            for (ProducerRecord<String, String> record : records) {
                acknowledgements.add(template.send(record));
            }
            CompletableFuture.allOf(acknowledgements.toArray(CompletableFuture[]::new))
                    .get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing Kafka batch", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new KafkaPublishException("Kafka did not acknowledge the complete output batch", e);
        } catch (KafkaException e) {
            throw new KafkaPublishException("Kafka rejected output batch publication", e);
        } finally {
            metrics.stopPublishing(sample);
        }
    }
}
