package com.kafkalab.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.model.InputMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class GeneratorRunner implements ApplicationRunner {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final LabProperties properties;
    private final MessageIdFactory ids;
    private final Environment environment;

    public GeneratorRunner(KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                           LabProperties properties, MessageIdFactory ids, Environment environment) {
        this.kafka = kafka; this.mapper = mapper; this.properties = properties; this.ids = ids; this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!args.containsOption("generate")) return;
        long count = args.containsOption("messages")
                ? Long.parseLong(args.getOptionValues("messages").get(0)) : properties.messages();
        if (count < 1) throw new IllegalArgumentException("--messages must be positive");
        String runId = args.containsOption("run-id") ? args.getOptionValues("run-id").get(0) : ids.newRunId();
        snapshotOffsets(runId);
        String payload = "x".repeat(properties.payloadBytes());
        ExecutorService pool = Executors.newFixedThreadPool(properties.generatorThreads());
        AtomicLong completed = new AtomicLong();
        long publishStartEpochMs = System.currentTimeMillis();
        long start = System.nanoTime();
        List<Future<?>> workers = new ArrayList<>();
        try {
            for (int worker = 0; worker < properties.generatorThreads(); worker++) {
                final int workerId = worker;
                workers.add(pool.submit(() -> {
                    long blockStride = (long) properties.generatorThreads() * properties.batchSize();
                    for (long blockStart = (long) workerId * properties.batchSize() + 1; blockStart <= count; blockStart += blockStride) {
                        long blockEnd = Math.min(count, blockStart + properties.batchSize() - 1);
                        List<String> records = new ArrayList<>((int) (blockEnd - blockStart + 1));
                        for (long sequence = blockStart; sequence <= blockEnd; sequence++) {
                            InputMessage message = new InputMessage(1, ids.create(runId, sequence), Instant.now(),
                                    "BENCHMARK", new InputMessage.Payload(sequence, payload));
                            records.add(mapper.writeValueAsString(message));
                        }
                        List<Future<?>> acknowledgements = new ArrayList<>(records.size());
                        if (properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE) {
                            kafka.executeInTransaction(operations -> {
                                List<Future<?>> transactionAcks = new ArrayList<>(records.size());
                                for (int i = 0; i < records.size(); i++) {
                                    long sequence = blockStart + i;
                                    transactionAcks.add(operations.send(properties.inputTopic(), ids.create(runId, sequence), records.get(i)));
                                }
                                awaitAcknowledgements(transactionAcks);
                                return true;
                            });
                        } else {
                            for (int i = 0; i < records.size(); i++) {
                                long sequence = blockStart + i;
                                acknowledgements.add(kafka.send(properties.inputTopic(), ids.create(runId, sequence), records.get(i)));
                            }
                            awaitAcknowledgements(acknowledgements);
                        }
                        long n = completed.addAndGet(records.size());
                        if (n / 10_000 > (n - records.size()) / 10_000) {
                            System.out.printf("published=%d/%d%n", n, count);
                        }
                    }
                }));
            }
            for (Future<?> worker : workers) worker.get();
            kafka.flush();
            double seconds = (System.nanoTime() - start) / 1_000_000_000d;
            System.out.printf("{\"runId\":\"%s\",\"published\":%d,\"publishStartEpochMs\":%d,\"publishSeconds\":%.6f}%n", runId, completed.get(), publishStartEpochMs, seconds);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void awaitAcknowledgements(List<Future<?>> acknowledgements) {
        for (Future<?> acknowledgement : acknowledgements) {
            try {
                acknowledgement.get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new CompletionException("Kafka did not acknowledge a generated batch", e);
            }
        }
    }

    private void snapshotOffsets(String runId) throws Exception {
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
