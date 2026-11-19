package com.kafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfiguration {
    @Bean NewTopic inputTopic(LabProperties p) {
        return TopicBuilder.name(p.inputTopic()).partitions(p.partitions()).replicas(1).build();
    }
    @Bean NewTopic outputTopic(LabProperties p) {
        return TopicBuilder.name(p.outputTopic()).partitions(p.partitions()).replicas(1).build();
    }
    @Bean NewTopic dltTopic(LabProperties p) {
        return TopicBuilder.name(p.dltTopic()).partitions(p.partitions()).replicas(1).build();
    }
}
