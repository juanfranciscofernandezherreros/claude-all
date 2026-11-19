package com.kafkalab.generator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkalab.config.LabProperties;
import com.kafkalab.exception.GenerationFailedException;
import com.kafkalab.model.InputMessage;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class GeneratorRunner implements ApplicationRunner {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final LabProperties properties;
    private final MessageIdFactory ids;
    private final GeneratorOffsetSnapshot offsetSnapshot;

    public GeneratorRunner(KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                           LabProperties properties, MessageIdFactory ids, GeneratorOffsetSnapshot offsetSnapshot) {
        this.kafka = kafka; this.mapper = mapper; this.properties = properties; this.ids = ids;
        this.offsetSnapshot = offsetSnapshot;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        if (!args.containsOption("generate")) return;
        long count = args.containsOption("messages")
                ? Long.parseLong(args.getOptionValues("messages").get(0)) : properties.messages();
        if (count < 1) throw new IllegalArgumentException("--messages must be positive");
        long timeoutSeconds = args.containsOption("timeout-seconds")
                ? Long.parseLong(args.getOptionValues("timeout-seconds").get(0)) : 600L;
        if (timeoutSeconds < 1) throw new IllegalArgumentException("--timeout-seconds must be positive");
        String runId = args.containsOption("run-id") ? args.getOptionValues("run-id").get(0) : ids.newRunId();
        try {
            offsetSnapshot.capture(runId);
        } catch (Exception snapshotFailure) {
            throw new GenerationFailedException("Could not capture initial Kafka offsets for run " + runId, snapshotFailure);
        }
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        String payload = payloadFor(properties.payloadBytes());
        ExecutorService pool = Executors.newFixedThreadPool(properties.generatorThreads());
        AtomicLong completed = new AtomicLong();
        long publishStartEpochMs = System.currentTimeMillis();
        long start = System.nanoTime();
        List<Future<?>> workers = new ArrayList<>();
        try {
            for (int worker = 0; worker < properties.generatorThreads(); worker++) {
                final int workerId = worker;
                workers.add(pool.submit(() -> {
                    for (long blockStart : blockStartsForWorker(count, workerId,
                            properties.generatorThreads(), properties.batchSize())) {
                        final long currentBlockStart = blockStart;
                        long blockEnd = Math.min(count, blockStart + properties.batchSize() - 1);
                        List<String> records = new ArrayList<>((int) (blockEnd - blockStart + 1));
                        for (long sequence = blockStart; sequence <= blockEnd; sequence++) {
                            InputMessage message = new InputMessage(1, ids.create(runId, sequence), Instant.now(),
                                    "BENCHMARK", new InputMessage.Payload(sequence, payload));
                            records.add(serialize(message));
                        }
                        List<Future<?>> acknowledgements = new ArrayList<>(records.size());
                        boolean isExactlyOnceDeliveryEnabled = properties.deliveryMode() == LabProperties.DeliveryMode.EXACTLY_ONCE;
                        if (isExactlyOnceDeliveryEnabled) {
                            kafka.executeInTransaction(operations -> {
                                List<Future<?>> transactionAcks = new ArrayList<>(records.size());
                                for (int i = 0; i < records.size(); i++) {
                                    long sequence = currentBlockStart + i;
                                    transactionAcks.add(operations.send(properties.inputTopic(), ids.create(runId, sequence), records.get(i)));
                                }
                                awaitAcknowledgements(transactionAcks, deadlineNanos);
                                return true;
                            });
                        } else {
                            for (int i = 0; i < records.size(); i++) {
                                long sequence = blockStart + i;
                                acknowledgements.add(kafka.send(properties.inputTopic(), ids.create(runId, sequence), records.get(i)));
                            }
                            awaitAcknowledgements(acknowledgements, deadlineNanos);
                        }
                        long n = completed.addAndGet(records.size());
                        if (n / 10_000 > (n - records.size()) / 10_000) {
                            System.out.printf("published=%d/%d%n", n, count);
                        }
                    }
                }));
            }
            try {
                for (Future<?> worker : workers) {
                    worker.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GenerationFailedException("Interrupted while waiting for generator workers", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new GenerationFailedException("Generator workers did not complete successfully", e);
            }
            double seconds = (System.nanoTime() - start) / 1_000_000_000d;
            System.out.printf("{\"runId\":\"%s\",\"published\":%d,\"publishStartEpochMs\":%d,\"publishSeconds\":%.6f}%n", runId, completed.get(), publishStartEpochMs, seconds);
        } finally {
            pool.shutdownNow();
        }
    }

    static List<Long> blockStartsForWorker(long count, int workerId, int workerCount, int batchSize) {
        if (count < 0) throw new IllegalArgumentException("count must not be negative");
        if (workerCount < 1) throw new IllegalArgumentException("workerCount must be positive");
        if (workerId < 0 || workerId >= workerCount) throw new IllegalArgumentException("workerId is out of range");
        if (batchSize < 1) throw new IllegalArgumentException("batchSize must be positive");

        long stride = Math.multiplyExact((long) workerCount, batchSize);
        List<Long> starts = new ArrayList<>();
        long blockStart = Math.addExact(Math.multiplyExact((long) workerId, batchSize), 1L);
        while (blockStart <= count) {
            starts.add(blockStart);
            boolean hasNoNextBlock = count - blockStart < stride;
            if (hasNoNextBlock) break;
            blockStart += stride;
        }
        return starts;
    }

    static String payloadFor(int bytes) {
        if (bytes < 1) throw new IllegalArgumentException("payload size must be positive");
        return "x".repeat(bytes);
    }

    private String serialize(InputMessage message) {
        try {
            return mapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new GenerationFailedException("Could not serialize generated message", e);
        }
    }

    static void awaitAcknowledgements(List<Future<?>> acknowledgements, long deadlineNanos) {
        for (Future<?> acknowledgement : acknowledgements) {
            try {
                acknowledgement.get(remainingNanos(deadlineNanos), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GenerationFailedException("Interrupted while waiting for Kafka acknowledgements", e);
            } catch (ExecutionException | TimeoutException e) {
                throw new GenerationFailedException("Kafka did not acknowledge the generated batch before the global timeout", e);
            }
        }
    }

    private static long remainingNanos(long deadlineNanos) throws TimeoutException {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) throw new TimeoutException("Generation exceeded --timeout-seconds");
        return remaining;
    }


}
