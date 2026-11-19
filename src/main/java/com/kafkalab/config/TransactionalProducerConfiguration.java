package com.kafkalab.config;

import java.util.UUID;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransactionalProducerConfiguration {
    @Bean
    DefaultKafkaProducerFactoryCustomizer transactionalProducerCustomizer(LabProperties properties) {
        return factory -> {
            boolean isExactlyOnceDeliveryEnabled = properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE;
            if (isExactlyOnceDeliveryEnabled) {
                factory.setTransactionIdPrefix("kafka-lab-" + UUID.randomUUID() + "-");
            }
        };
    }
}
