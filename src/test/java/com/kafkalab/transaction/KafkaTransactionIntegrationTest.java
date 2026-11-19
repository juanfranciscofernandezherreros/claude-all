package com.kafkalab.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.nio.file.Path;
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
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"))
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
            var records = consumer.poll(Duration.ofSeconds(1));
            long waitUntil = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (records.isEmpty() && System.nanoTime() < waitUntil) records = consumer.poll(Duration.ofMillis(500));
            assertEquals(1, records.count());
            var consumed = records.iterator().next();
            transactionalProducer.initTransactions();
            transactionalProducer.beginTransaction();
            transactionalProducer.send(new ProducerRecord<>(output, consumed.key(), "aborted")).get();
            transactionalProducer.sendOffsetsToTransaction(
                    Map.of(new TopicPartition(input, consumed.partition()), new OffsetAndMetadata(consumed.offset() + 1)),
                    consumer.groupMetadata());
            transactionalProducer.abortTransaction();
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
                var afterAbort = admin.listConsumerGroupOffsets("eos-group").partitionsToOffsetAndMetadata().get();
                assertEquals(0L, afterAbort.getOrDefault(new TopicPartition(input, 0), new OffsetAndMetadata(0)).offset());
            }
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
            var result = reader.poll(Duration.ofSeconds(1));
            long waitUntil = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (result.isEmpty() && System.nanoTime() < waitUntil) result = reader.poll(Duration.ofMillis(500));
            assertEquals(1, result.count());
            assertEquals("output", result.iterator().next().value());
        }
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var committed = admin.listConsumerGroupOffsets("eos-group").partitionsToOffsetAndMetadata().get();
            assertEquals(1L, committed.get(new TopicPartition(input, 0)).offset());
        }

        Properties uncommittedProperties = new Properties();
        uncommittedProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        uncommittedProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-uncommitted-" + UUID.randomUUID());
        uncommittedProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        uncommittedProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        uncommittedProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        uncommittedProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        try (KafkaConsumer<String, String> reader = new KafkaConsumer<>(uncommittedProperties)) {
            reader.subscribe(List.of(output));
            var result = reader.poll(Duration.ofSeconds(2));
            assertEquals(2, result.count());
            boolean abortedVisible = false;
            boolean committedVisible = false;
            for (var record : result.records(output)) {
                abortedVisible |= "aborted".equals(record.value());
                committedVisible |= "output".equals(record.value());
            }
            assertTrue(abortedVisible, "read_uncommitted must expose the aborted transactional record");
            assertTrue(committedVisible, "read_uncommitted must expose the committed record");
        }

        Properties restartProperties = new Properties();
        restartProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        restartProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-group");
        restartProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        restartProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        restartProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        restartProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        restartProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        try (KafkaConsumer<String, String> restarted = new KafkaConsumer<>(restartProperties)) {
            restarted.subscribe(List.of(input));
            var replayed = restarted.poll(Duration.ofSeconds(2));
            assertTrue(replayed.isEmpty(), "restarted group must resume after the transactionally committed offset");
        }
    }
    @Test
    void publishesTenThousandRecordsWithStableKeyPartitioning() throws Exception {
        String topic = "generator-load-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 1))).all().get();
            long leaderDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            boolean leadersReady = false;
            while (!leadersReady && System.nanoTime() < leaderDeadline) {
                var description = admin.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                leadersReady = description.partitions().size() == 6
                        && description.partitions().stream().allMatch(partition -> partition.leader() != null);
                if (!leadersReady) Thread.sleep(100);
            }
            assertTrue(leadersReady, "all 6 partitions must have elected leaders before publishing");
        }

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
        Map<String, Integer> keyPartitions = new java.util.HashMap<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties)) {
            List<java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata>> acknowledgements =
                    new java.util.ArrayList<>(10_000);
            for (int sequence = 1; sequence <= 10_000; sequence++) {
                String key = "generator-key-" + (sequence % 10);
                acknowledgements.add(producer.send(new ProducerRecord<>(topic, key, "payload-" + sequence)));
            }
            long acknowledgementDeadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            for (var acknowledgement : acknowledgements) {
                long remaining = acknowledgementDeadline - System.nanoTime();
                assertTrue(remaining > 0, "all 10,000 producer ACKs must arrive before the global deadline");
                var metadata = acknowledgement.get(remaining, java.util.concurrent.TimeUnit.NANOSECONDS);
                assertTrue(metadata.partition() >= 0 && metadata.partition() < 6);
            }
            producer.flush();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "generator-load-reader-" + UUID.randomUUID());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            List<TopicPartition> partitions = java.util.stream.IntStream.range(0, 6)
                    .mapToObj(partition -> new TopicPartition(topic, partition)).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            int received = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (received < 10_000 && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                received += records.count();
                for (var record : records) {
                    Integer previousPartition = keyPartitions.putIfAbsent(record.key(), record.partition());
                    if (previousPartition != null) {
                        assertEquals(previousPartition.intValue(), record.partition(),
                                "same Kafka key must always map to the same partition");
                    }
                }
            }
            assertEquals(10_000, received, "all acknowledged generator records must be readable");
            assertEquals(10, keyPartitions.size());
        }
    }

    @Test
    void validatorClassifiesKnownMissingDuplicateUnexpectedCorruptAndDltFixtures() throws Exception {
        String runId = "validator-fixture-" + UUID.randomUUID();
        String inputTopic = "validator-input-" + UUID.randomUUID();
        String outputTopic = "validator-output-" + UUID.randomUUID();
        String dltTopic = "validator-dlt-" + UUID.randomUUID();
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(inputTopic, 1, (short) 1),
                    new NewTopic(outputTopic, 1, (short) 1), new NewTopic(dltTopic, 1, (short) 1))).all().get();
            awaitTopicLeaders(admin, inputTopic, 1);
            awaitTopicLeaders(admin, outputTopic, 1);
            awaitTopicLeaders(admin, dltTopic, 1);
        }

        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        com.kafkalab.processing.ChecksumService checksums = new com.kafkalab.processing.ChecksumService();
        String payload = "x".repeat(1024);
        java.time.Instant created = java.time.Instant.parse("2025-01-01T00:00:00Z");
        com.kafkalab.model.InputMessage input = new com.kafkalab.model.InputMessage(1, runId + "-1",
                created, "BENCHMARK", new com.kafkalab.model.InputMessage.Payload(1, payload));
        com.kafkalab.model.InputMessage deadLetterInput = new com.kafkalab.model.InputMessage(1, runId + "-3",
                created, "BENCHMARK", new com.kafkalab.model.InputMessage.Payload(3, payload));
        com.kafkalab.model.OutputMessage valid = new com.kafkalab.model.OutputMessage(1, runId + "-1",
                created, created.plusMillis(2), "OK",
                new com.kafkalab.model.OutputMessage.Result(1, checksums.sha256(payload)));
        com.kafkalab.model.OutputMessage unexpected = new com.kafkalab.model.OutputMessage(1, runId + "-4",
                created, created.plusMillis(3), "OK",
                new com.kafkalab.model.OutputMessage.Result(4, checksums.sha256(payload)));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties(KAFKA.getBootstrapServers()))) {
            producer.send(new ProducerRecord<>(inputTopic, input.messageId(), mapper.writeValueAsString(input))).get(10, java.util.concurrent.TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(outputTopic, valid.messageId(), mapper.writeValueAsString(valid))).get(10, java.util.concurrent.TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(outputTopic, valid.messageId(), mapper.writeValueAsString(valid))).get(10, java.util.concurrent.TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(outputTopic, unexpected.messageId(), mapper.writeValueAsString(unexpected))).get(10, java.util.concurrent.TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(outputTopic, "corrupt", "{not-json")).get(10, java.util.concurrent.TimeUnit.SECONDS);
            com.kafkalab.model.DltMessage dlt = new com.kafkalab.model.DltMessage(
                    mapper.writeValueAsString(deadLetterInput), "IllegalArgumentException", "fixture", created);
            producer.send(new ProducerRecord<>(dltTopic, runId + "-3", mapper.writeValueAsString(dlt))).get(10, java.util.concurrent.TimeUnit.SECONDS);
        }

        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);
        Properties groupProperties = consumerProperties(KAFKA.getBootstrapServers(), "kafka-lab");
        try (KafkaConsumer<String, String> groupConsumer = new KafkaConsumer<>(groupProperties)) {
            groupConsumer.assign(List.of(inputPartition));
            groupConsumer.seekToEnd(List.of(inputPartition));
            long committedEnd = groupConsumer.position(inputPartition);
            groupConsumer.commitSync(Map.of(inputPartition, new OffsetAndMetadata(committedEnd)));
        }

        Path runFolder = Path.of("benchmark-results", runId);
        java.nio.file.Files.createDirectories(runFolder);
        List<Map<String, Object>> snapshot = List.of(
                Map.of("topic", inputTopic, "partition", 0, "offset", 0L),
                Map.of("topic", outputTopic, "partition", 0, "offset", 0L),
                Map.of("topic", dltTopic, "partition", 0, "offset", 0L));
        mapper.writerWithDefaultPrettyPrinter().writeValue(runFolder.resolve("offsets.json").toFile(), snapshot);

        com.kafkalab.config.LabProperties labProperties = new com.kafkalab.config.LabProperties(
                3, 24, 1, 500, 65536, 4, 1024,
                com.kafkalab.config.LabProperties.DeliveryMode.AT_LEAST_ONCE,
                com.kafkalab.config.LabProperties.Workload.LIGHT, inputTopic, outputTopic, dltTopic);
        var environment = new org.springframework.mock.env.MockEnvironment()
                .withProperty("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        com.kafkalab.validation.ValidationRunner runner = new com.kafkalab.validation.ValidationRunner(
                environment, labProperties, mapper, checksums);

        try {
            runner.run(new org.springframework.boot.DefaultApplicationArguments(
                    "--validate", "--run-id=" + runId, "--messages=3", "--timeout-seconds=10"));
            fail("validator must fail for deliberately injected anomalies");
        } catch (IllegalStateException expectedFailure) {
            assertTrue(expectedFailure.getMessage().contains("Independent validation failed"));
        }

        Map<String, Object> report = mapper.readValue(runFolder.resolve("validation.json").toFile(),
                new com.fasterxml.jackson.core.type.TypeReference<>() {});
        assertEquals("FAIL", report.get("status"));
        assertEquals(Boolean.TRUE, report.get("processingComplete"));
        assertEquals(1L, ((Number) report.get("valid")).longValue());
        assertEquals(2L, ((Number) report.get("missing")).longValue());
        assertEquals(1L, ((Number) report.get("duplicate")).longValue());
        assertEquals(1L, ((Number) report.get("unexpected")).longValue());
        assertEquals(1L, ((Number) report.get("invalid")).longValue());
        assertEquals(1L, ((Number) report.get("dlt")).longValue());

        try (var paths = java.nio.file.Files.walk(runFolder)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.deleteIfExists(path);
            }
        }
    }

    private static void awaitTopicLeaders(AdminClient admin, String topic, int expectedPartitions) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            var description = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(5, java.util.concurrent.TimeUnit.SECONDS).get(topic);
            if (description.partitions().size() == expectedPartitions
                    && description.partitions().stream().allMatch(partition -> partition.leader() != null)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new java.util.concurrent.TimeoutException("Kafka leaders were not elected for " + topic);
    }

    private static Properties producerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private static Properties consumerProperties(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

}
