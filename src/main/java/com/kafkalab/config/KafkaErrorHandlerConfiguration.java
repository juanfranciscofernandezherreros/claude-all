package com.kafkalab.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.exception.KafkaPublishException;
import com.kafkalab.model.DltMessage;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlerConfiguration {
    static ExponentialBackOffWithMaxRetries retryBackOff() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000L);
        return backOff;
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, LabProperties properties,
                                          ObjectMapper mapper, com.kafkalab.metrics.LabMetrics metrics) {
        ExponentialBackOffWithMaxRetries backOff = retryBackOff();

        ConsumerAwareRecordRecoverer recoverer;
        boolean exactlyOnce = properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE;
        if (exactlyOnce) {
            recoverer = (record, consumer, exception) -> {
                try {
                    DltMessage dlt = new DltMessage(String.valueOf(record.value()), exception.getClass().getSimpleName(),
                            String.valueOf(exception.getMessage()), Instant.now());
                    String json = mapper.writeValueAsString(dlt);
                    template.executeInTransaction(operations -> {
                        try {
                            operations.send(properties.dltTopic(), String.valueOf(record.key()), json).get(30, TimeUnit.SECONDS);
                        } catch (Exception e) {
                            throw new KafkaPublishException("Transactional DLT publish failed for topic " + properties.dltTopic(), e);
                        }
                        operations.sendOffsetsToTransaction(
                                Map.of(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1)),
                                consumer.groupMetadata());
                        return true;
                    });
                } catch (Exception e) {
                    throw new KafkaPublishException("Could not recover failed record transactionally", e);
                }
            };
        } else {
            recoverer = new DeadLetterPublishingRecoverer(template,
                    (record, exception) -> new TopicPartition(properties.dltTopic(), record.partition()));
        }
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setRetryListeners((record, exception, deliveryAttempt) -> metrics.retry());
        if (exactlyOnce) handler.setAckAfterHandle(false);
        return handler;
    }
}
