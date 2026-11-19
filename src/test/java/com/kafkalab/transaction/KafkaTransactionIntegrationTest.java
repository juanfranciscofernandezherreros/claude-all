package com.kafkalab.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class KafkaTransactionIntegrationTest {
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"))
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1");

    @Test
    void outputAndConsumerOffsetCommitAtomically() throws Exception {
        String input = "eos-input-" + UUID.randomUUID();
        String output = "eos-output-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1), new NewTopic(output, 1, (short) 1))).all().get();
        }

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-" + UUID.randomUUID());
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");

        Properties inputProducerProperties = new Properties();
        inputProducerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        inputProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        inputProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> inputProducer = new KafkaProducer<>(inputProducerProperties)) {
            inputProducer.send(new ProducerRecord<>(input, "id-1", "input")).get();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-group");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
             KafkaProducer<String, String> transactionalProducer = new KafkaProducer<>(producerProperties)) {
            consumer.subscribe(List.of(input));
            var records = consumer.poll(Duration.ofSeconds(20));
            assertEquals(1, records.count());
            var consumed = records.iterator().next();
            transactionalProducer.initTransactions();
            transactionalProducer.beginTransaction();
            transactionalProducer.send(new ProducerRecord<>(output, consumed.key(), "output")).get();
            transactionalProducer.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(input, consumed.partition()), new OffsetAndMetadata(consumed.offset() + 1)),
                    consumer.groupMetadata());
            transactionalProducer.commitTransaction();
        }

        Properties readProperties = new Properties();
        readProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        readProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-read-" + UUID.randomUUID());
        readProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        readProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        readProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        readProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        try (KafkaConsumer<String, String> reader = new KafkaConsumer<>(readProperties)) {
            reader.subscribe(List.of(output));
            var result = reader.poll(Duration.ofSeconds(20));
            assertEquals(1, result.count());
            assertEquals("output", result.iterator().next().value());
        }
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var committed = admin.listConsumerGroupOffsets("eos-group").partitionsToOffsetAndMetadata().get();
            assertEquals(1L, committed.get(new TopicPartition(input, 0)).offset());
        }
    }
}
