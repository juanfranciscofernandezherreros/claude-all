package com.kafkalab.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** Captures the initial Kafka offsets for one isolated benchmark run. */
@Component
public class GeneratorOffsetSnapshot {
    private final ObjectMapper mapper;
    private final LabProperties properties;
    private final Environment environment;

    public GeneratorOffsetSnapshot(ObjectMapper mapper, LabProperties properties, Environment environment) {
        this.mapper = mapper;
        this.properties = properties;
        this.environment = environment;
    }

    public void capture(String runId) throws Exception {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty("spring.kafka.bootstrap-servers", "localhost:9094"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-lab-offset-snapshot-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            List<TopicPartition> partitions = new ArrayList<>();
            for (String topic : List.of(properties.inputTopic(), properties.outputTopic(), properties.dltTopic())) {
                for (PartitionInfo info : consumer.partitionsFor(topic)) {
                    partitions.add(new TopicPartition(topic, info.partition()));
                }
            }
            consumer.assign(partitions);
            Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
            List<Map<String, Object>> snapshot = new ArrayList<>();
            offsets.forEach((partition, offset) -> snapshot.add(Map.of(
                    "topic", partition.topic(), "partition", partition.partition(), "offset", offset)));
            Path folder = Path.of("benchmark-results", runId);
            Files.createDirectories(folder);
            mapper.writerWithDefaultPrettyPrinter().writeValue(folder.resolve("offsets.json").toFile(), snapshot);
        }
    }
}
