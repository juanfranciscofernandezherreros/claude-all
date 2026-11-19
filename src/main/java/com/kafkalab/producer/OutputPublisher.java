package com.kafkalab.producer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OutputPublisher {
    private final KafkaTemplate<String, String> template;
    public OutputPublisher(KafkaTemplate<String, String> template) { this.template = template; }

    public void publish(String topic, String key, String json) {
        try {
            template.send(topic, key, json).get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka publish was not acknowledged for key " + key, e);
        }
    }
}
