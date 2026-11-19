package com.kafkalab.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkalab.config.LabProperties;
import com.kafkalab.model.DltMessage;
import com.kafkalab.model.InputMessage;
import com.kafkalab.model.OutputMessage;
import com.kafkalab.processing.ChecksumService;
import java.nio.file.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class ValidationRunner implements ApplicationRunner {
    private final Environment environment;
    private final LabProperties properties;
    private final ObjectMapper mapper;
    private final ChecksumService checksums;

    public ValidationRunner(Environment environment, LabProperties properties, ObjectMapper mapper, ChecksumService checksums) {
        this.environment = environment; this.properties = properties; this.mapper = mapper; this.checksums = checksums;
    }

    @Override public void run(ApplicationArguments args) throws Exception {
        if (!args.containsOption("validate")) return;
        String runId = required(args, "run-id");
        long expected = Long.parseLong(required(args, "messages"));
        long timeoutSeconds = args.containsOption("timeout-seconds")
                ? Long.parseLong(args.getOptionValues("timeout-seconds").get(0)) : 600L;
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers", "localhost:9094"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-lab-validator-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        BitSet sequences = new BitSet(Math.toIntExact(Math.min(expected + 1, Integer.MAX_VALUE)));
        long duplicates = 0, unexpected = 0, invalid = 0, dlt = 0;
        List<TopicPartition> partitions = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            for (String topic : List.of(properties.outputTopic(), properties.dltTopic())) {
                for (PartitionInfo p : consumer.partitionsFor(topic, Duration.ofSeconds(20))) {
                    partitions.add(new TopicPartition(topic, p.partition()));
                }
            }
            consumer.assign(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions, Duration.ofSeconds(20));
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
            while (!atEnd(consumer, ends) && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (record.topic().equals(properties.outputTopic())) {
                        try {
                            OutputMessage output = mapper.readValue(record.value(), OutputMessage.class);
                            if (!output.messageId().startsWith(runId + "-")) continue;
                            long sequence = output.result().sequence();
                            if (output.schemaVersion() != 1 || !"OK".equals(output.status())
                                    || !output.messageId().equals(runId + "-" + sequence)
                                    || !checksums.sha256(extractExpectedPayload(record, output)).equals(output.result().checksum())) {
                                invalid++;
                            } else if (sequence < 1 || sequence > expected) {
                                unexpected++;
                            } else if (sequences.get((int) sequence)) {
                                duplicates++;
                            } else {
                                sequences.set((int) sequence);
                            }
                        } catch (Exception e) { invalid++; }
                    } else {
                        try {
                            DltMessage dead = mapper.readValue(record.value(), DltMessage.class);
                            InputMessage original = mapper.readValue(dead.originalPayload(), InputMessage.class);
                            if (original.messageId().startsWith(runId + "-")) dlt++;
                        } catch (Exception ignored) { }
                    }
                }
            }
        }
        long valid = sequences.cardinality();
        long missing = Math.max(0, expected - valid);
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("runId", runId); report.put("timestamp", Instant.now().toString());
        report.put("expected", expected); report.put("valid", valid); report.put("failed", 0);
        report.put("dlt", dlt); report.put("missing", missing); report.put("duplicate", duplicates);
        report.put("unexpected", unexpected); report.put("invalid", invalid);
        report.put("status", missing == 0 && duplicates == 0 && unexpected == 0 && invalid == 0 && dlt == 0 ? "PASS" : "FAIL");
        Path output = Path.of("benchmark-results", runId, "validation.json");
        Files.createDirectories(output.getParent());
        mapper.writerWithDefaultPrettyPrinter().writeValue(output.toFile(), report);
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(report));
        if (!"PASS".equals(report.get("status"))) throw new IllegalStateException("Independent validation failed; see " + output);
    }

    private boolean atEnd(KafkaConsumer<String, String> consumer, Map<TopicPartition, Long> ends) {
        for (var entry : ends.entrySet()) if (consumer.position(entry.getKey()) < entry.getValue()) return false;
        return true;
    }
    private String required(ApplicationArguments args, String name) {
        if (!args.containsOption(name) || args.getOptionValues(name).isEmpty()) throw new IllegalArgumentException("--" + name + " is required");
        return args.getOptionValues(name).get(0);
    }
    private String extractExpectedPayload(ConsumerRecord<String, String> record, OutputMessage output) {
        return output.result().checksum();
    }
}
