package com.kafkalab.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfiguration {
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template, LabProperties properties) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template, (record, exception) -> new TopicPartition(properties.dltTopic(), record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 3L));
    }
}
