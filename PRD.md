# Kafka Performance Lab — Autonomous Execution Spec

## Mission
Implement, run, validate and document a complete Kafka-to-Kafka message processing benchmark. Do not only provide a plan or code snippets.

## Autonomy
- Ask ZERO questions. Requirements are defined; choose the simplest reliable default for unspecified details.
- Do not request routine approvals. Create/edit files, install dependencies where permitted, run commands, build, start/stop Docker, test, benchmark, diagnose and fix errors autonomously.
- After failures inspect logs, fix root cause and rerun affected tests.
- Never invent metrics or mark unexecuted tests PASS.
- Stop only for unavoidable external blockers; document exact blocker and continue independent tasks.
- Inspect repository first and preserve unrelated files.

## Fixed stack
Java 21, Spring Boot + Spring Kafka, Apache Kafka KRaft, Maven, JSON, Docker Compose, Micrometer, Prometheus, Grafana, JUnit 5, Testcontainers. Pin compatible stable dependency/image versions; never use floating `latest`.

## Goal and architecture
Consume 1,000,000 Kafka messages, transform each valid input and publish exactly one logical output per valid input. Measure duration, throughput, latency, resource use, scalability and integrity.

Flow: Java input generator -> `input-topic` -> Java Consumer -> deterministic processing -> Kafka Producer -> `output-topic` -> independent validator -> reports. Also create `input-dlt-topic`.

Local single-broker Kafka in Docker Compose, KRaft mode (no ZooKeeper), 24 partitions, replication factor 1. Configure Docker-internal and localhost advertised listeners. Health-check broker and wait for readiness.

## Message contract
Input JSON: unique `messageId`, timestamp, eventType, payload.
Output JSON: original `messageId`, processing timestamp, status, result.
Preserve IDs. One logical output per valid input. Deterministic transformation. Validate JSON and required fields. Invalid/permanently failed records go to DLT and remain accounted for.

## Delivery modes
Implement and benchmark both:
- AT_LEAST_ONCE: manual offset commit only after output broker acknowledgement; idempotent producer; retries; document possible duplicates on retry.
- EXACTLY_ONCE: Kafka transactions atomically publish output and commit consumed offsets; unique transactional IDs per producer instance; consumers use read_committed; rollback/recovery tests. No exactly-once claims outside Kafka's transaction boundary.

## Configuration
Environment variables: bootstrap servers, topics, group ID, concurrency, max poll records, batch size, linger, compression, workload, delivery mode, message count, payload size, generator threads.
Defaults: 1,000,000 messages; 24 partitions; concurrency 1; batch size 500; generator threads 4; payload 1 KB.
Concurrency: 1, 2, 5, 10, 20. Payload sizes: 1, 5, 10, 50, 100 KB. Workloads: LIGHT, MEDIUM, HEAVY (deterministic CPU work; no artificial delay by default).

## Generator
Independent Java generator with unique IDs, configurable count/size/rate/batch/threads, progress and error reporting. Measure input publishing separately. Ensure generator is not silently the benchmark bottleneck.

## Metrics
Micrometer + Prometheus. Measure received, processed, published, failed, retried, DLT, processing/publish/end-to-end latency, throughput, consumer lag, JVM heap/non-heap, GC and CPU where available. Report p50/p95/p99. Avoid high-cardinality labels such as messageId.

## Benchmark engine
Automated controller/scripts. Isolate/reset topics and state per run. Verify broker and consumer readiness.
For each run:
1. Use monotonic timers.
2. Publish input workload.
3. Wait for all expected output records to be confirmed and independently validated (not merely generator completion).
4. Record input publishing, consumer processing and end-to-end durations.
5. Validate expected/actual IDs, missing, duplicate, unexpected, failed and DLT counts.
6. Save raw data and report.
Primary metric: end-to-end throughput = validated outputs / end-to-end seconds. Consumer throughput uses successfully published/validated outputs, never merely consumed messages.
Perform warm-up and three measured runs per principal scenario; report median and preserve each run.

### Suites
A. Concurrency: 1M messages at 1, 2, 5, 10, 20 consumers.
B. Payload: 1, 5, 10, 50, 100 KB, same count and reference concurrency.
C. Workload: LIGHT, MEDIUM, HEAVY.
D. Delivery: AT_LEAST_ONCE vs EXACTLY_ONCE.
E. Stress: increase rate until throughput plateaus, lag grows, resources saturate, errors rise or service destabilizes.
F. Recovery: consumer/broker restart, invalid JSON, processing/publishing failure, transaction rollback.

## Calculations
Throughput = validated outputs / elapsed seconds.
Error rate = failed inputs / total inputs * 100.
Latency = output confirmation time - input creation time; use consistent clocks and monotonic timers for durations.
Report min, mean, p50, p95, p99 and max latency.

## Acceptance
Functional: all valid inputs accounted for; one logical output per valid input; zero missing/duplicate logical outputs in successful run; IDs preserved; failures/DLT accounted; offsets correct; restart recovery tested.
Initial performance target (not guarantee): 1M messages end-to-end under 60 seconds (~16,667 validated msg/s). Report actual results and hardware limits honestly.

## Validation
Independent validator with memory-bounded/disk-backed strategy (sorted files or embedded DB), not unbounded in-memory ID sets. Integrity JSON: expected, received, processed, published, unique, missing, duplicate, unexpected, failed, DLT. PASS requires actual validation.

## Docker/local operations
Create Dockerfile, Compose, Kafka KRaft broker, Java app, generator, Prometheus and Grafana. Health checks, persistent volumes where useful and configurable CPU/memory limits. Support local app execution for development.
User must NOT manually install/start Kafka. Automatically check Docker, build images, start broker, wait for readiness, create/verify topics and start services. If Docker absent/stopped or permissions block startup, attempt permitted remedies and report exact blocker; never claim success.

## Tests
JUnit unit tests: serialization, validation, transformation, IDs, error handling, metrics.
Integration/e2e: Kafka consume/publish, transactions, retry, DLT, offsets and recovery; Testcontainers where appropriate. Execute tests and report failures accurately.

## Deliverables
Complete Maven project, Consumer, Producer, generator, Dockerfile/Compose, Prometheus/Grafana configs/dashboard, scripts, tests and docs.
Suggested structure: `src/main/java` (config, consumer, producer, service, model, metrics, benchmark, validation, exception), `src/test/java`, `generator/`, `docker/`, `grafana/`, `scripts/`, `benchmark-results/`, `docs/`, `pom.xml`, `docker-compose.yml`, `.env.example`, `README.md`, `CLAUDE.md`.

Each run saves: summary.json, results.csv, validation.json, environment.json, execution.log, report.md. Never invent unavailable metrics.

## Commands to implement and test
- `scripts/start-environment.sh`
- `scripts/stop-environment.sh`
- `scripts/run-benchmark.sh --messages 1000000`
- `scripts/run-full-suite.sh --scenario concurrency|payload|reliability`
- `mvn clean verify`
Provide Windows .bat launchers.

## Execution order
1. Inspect repo, OS, Java, Maven, Docker and resources.
2. Create/complete project without overwriting unrelated data.
3. Implement app and message contract.
4. Configure/start local Kafka KRaft and verify broker/topics.
5. Implement generator, metrics, validator, benchmark.
6. Implement and execute tests.
7. Run warm-up and full benchmark suites (three measured runs per main scenario).
8. Analyze bottlenecks; optimize one variable at a time; preserve baseline.
9. Rerun and compare baseline vs optimized.
10. Generate docs and reproducible report.

## Definition of done
Runnable source; successful build; local Docker Kafka operational; topics created; consumer/producer/generator functional; tests executed; 1M benchmark executed or exact blocker documented; independent integrity validated; reports saved; README reproduces everything.

## Final response
Report files, commands, tests, actual counts, durations, throughput, p50/p95/p99, CPU/memory, missing/duplicates, best configuration, baseline vs optimized, limitations. Distinguish PASS, FAIL, BLOCKED and NOT TESTED.

## Final command
Begin immediately. Inspect environment, create files, start local Kafka broker, implement everything, execute tests and benchmarks, fix failures and report. Do not ask questions, return only a plan, or mark unexecuted tests successful.
