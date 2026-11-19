# Kafka Performance Lab — Autonomous Execution Spec

## Mission
Implement, run, validate and document a complete Kafka-to-Kafka message processing benchmark. Do not only provide a plan or code snippets.

## Autonomy (mandatory)
- Ask the user ZERO questions. All decisions are defined here; for unspecified details choose the simplest reliable compatible default.
- Do not request routine approvals. Create/edit project files, install dependencies where permitted, run shell commands, build, start/stop Docker services, test, benchmark, diagnose and fix errors.
- Continue after failures: inspect logs, identify root cause, fix and rerun affected tests.
- Never invent metrics or mark unexecuted tests PASS.
- Stop only for unavoidable external blockers (permissions, unavailable Docker/service/network); document exact blocker and continue independent work.
- Do not overwrite unrelated existing files. Inspect repository before changes.

## Fixed stack
Java 21, Spring Boot + Spring Kafka, Apache Kafka (KRaft), Maven, JSON, Docker Compose, Micrometer, Prometheus, Grafana, JUnit 5, Testcontainers. Pin compatible stable dependency and image versions; never use floating `latest`.

## Goal
Consume 1,000,000 messages from Kafka, validate/transform each valid message and publish exactly one logical output per valid input. Measure actual duration, throughput, latency, resource use, scaling and integrity.

## Architecture
Java input generator -> Kafka `input-topic` -> Java Consumer -> deterministic processing service -> Kafka Producer -> `output-topic` -> independent validator -> reports.
Also create `input-dlt-topic`.
Local single-broker Kafka in Docker Compose, KRaft mode (no ZooKeeper), 24 partitions, replication factor 1. Configure internal Docker listener and localhost advertised listener. Health-check broker and wait for readiness before dependent services.

## Message contract
Input JSON: unique `messageId`, timestamp, eventType, payload.
Output JSON: original `messageId`, processing timestamp, status and result.
Preserve IDs. One logical output per valid input. Deterministic transformation. Validate JSON and required fields. Invalid/permanently failed records must be accounted for and routed to DLT; never silently discard.

## Delivery modes
Implement and benchmark both:
1. AT_LEAST_ONCE: manual offset commit only after broker acknowledgement of output; idempotent producer; retries; document duplicate possibility on retry.
2. EXACTLY_ONCE: Kafka transactions atomically publish output and commit consumed offsets; unique transactional IDs per producer instance; consumers use read_committed; rollback/recovery tests. Do not claim exactly-once outside Kafka's transaction boundary.

## Configurable settings
Environment variables for bootstrap servers, topics, group ID, concurrency, max poll records, batch size, linger, compression, workload, delivery mode, message count, payload size and generator threads.
Defaults: 1,000,000 messages; 24 partitions; concurrency 1; batch size 500; generator threads 4; payload 1 KB.
Concurrency values: 1, 2, 5, 10, 20.
Payload sizes: 1, 5, 10, 50, 100 KB.
Workloads: LIGHT, MEDIUM, HEAVY (configurable deterministic CPU work; no artificial delay by default).

## Generator
Independent Java generator; unique IDs; configurable count, payload size, rate, batch size and threads; progress/error reporting. Measure input publish duration separately. Ensure generator is not silently the benchmark bottleneck.

## Metrics
Micrometer + Prometheus endpoint. Record received, processed, published, failed, retried, DLT, processing/publish/end-to-end latency, throughput, consumer lag, JVM heap/non-heap, GC and CPU where available. Report p50, p95, p99. Avoid high-cardinality labels such as messageId.

## Benchmark engine
Provide automated scripts/controller. Before each run, isolate/reset topics and state. Verify broker and consumer readiness.
For every run:
1. Start timers with monotonic clocks.
2. Publish input workload.
3. Wait until all expected output records are confirmed and independently validated (not merely until generator exits).
4. Record input publishing, consumer processing and end-to-end durations separately.
5. Validate expected IDs, actual IDs, missing, duplicate, unexpected, failed and DLT counts.
6. Save raw data and report.
Primary metric: end-to-end throughput = successfully validated output count / end-to-end seconds. Also report consumer throughput based on successfully published/validated output, never merely consumed records.
Warm-up before measurements; perform three measured runs per principal scenario and report median. Record each run independently.

### Suites
A. Concurrency: 1M messages at 1, 2, 5, 10, 20 consumers.
B. Payload: 1, 5, 10, 50, 100 KB, same count and reference concurrency.
C. Workload: LIGHT, MEDIUM, HEAVY.
D. Delivery: AT_LEAST_ONCE vs EXACTLY_ONCE.
E. Stress: increase rate until throughput plateaus, lag grows, resources saturate, errors rise or service destabilizes.
F. Recovery: consumer and broker restart, invalid JSON, processing/publishing failure, transaction rollback.

## Calculations
Throughput = validated output messages / elapsed seconds.
Error rate = failed input messages / total input messages * 100.
Latency = output confirmation time - input creation time; use consistent timestamps/clocks and monotonic timers for durations.
Report min, mean, p50, p95, p99 and max latency.

## Acceptance
Functional: all valid inputs accounted for; one logical output per valid input; zero missing/duplicate logical outputs in successful run; IDs preserved; failures/DLT accounted; offsets correct; restart recovery tested.
Initial performance target (not a guarantee): 1M messages end-to-end under 60 seconds (~16,667 validated msg/s). Report actual outcome and hardware limits honestly.

## Validation
Use a separate validator and memory-bounded/disk-backed approach (e.g. sorted files or embedded DB); do not keep all IDs in unbounded application memory. Produce integrity JSON with expected, received, processed, published, unique, missing, duplicate, unexpected, failed and DLT counts. A PASS requires actual successful validation.

## Docker/local operations
Provide Dockerfile, Compose, Kafka KRaft broker, Java app, generator, Prometheus and Grafana. Add health checks, persistent volumes where relevant and configurable CPU/memory limits. Support local app execution when useful.
The user must not need to install/start Kafka manually. Automatically check Docker, build images, start broker, wait for readiness, create/verify topics and start dependent services. If Docker is absent/not running or permissions block startup, attempt permitted remedies and report exact blocker; do not pretend success.

## Tests
JUnit unit tests for serialization, validation, transformation, IDs, error handling and metrics.
Integration/e2e tests for Kafka consume/publish, transactions, retry, DLT, offsets and recovery; use Testcontainers where appropriate.
Execute tests and report failures accurately.

## Deliverables
Complete Maven project, Consumer, Producer, generator, Dockerfile/Compose, Prometheus/Grafana configs/dashboard, scripts, tests and docs.
Suggested structure:
- src/main/java/... (config, consumer, producer, service, model, metrics, benchmark, validation, exception)
- src/test/java/...
- generator/
- docker/
- grafana/
- scripts/
- benchmark-results/
- docs/
- pom.xml, docker-compose.yml, .env.example, README.md, CLAUDE.md

Each benchmark run saves: summary.json, results.csv, validation.json, environment.json, execution.log, report.md. Do not invent unavailable metrics.

## Required commands
Implement and test:
- `scripts/start-environment.sh`
- `scripts/stop-environment.sh`
- `scripts/run-benchmark.sh --messages 1000000`
- `scripts/run-full-suite.sh --scenario concurrency|payload|reliability`
- `mvn clean verify`
Also provide Windows .bat launchers if practical.

## Execution order
1. Inspect repo, OS, Java, Maven, Docker and available resources.
2. Create/complete project structure without overwriting unrelated data.
3. Implement application and message contract.
4. Configure/start Kafka KRaft locally and verify broker/topics.
5. Implement generator, metrics, validator and benchmark engine.
6. Implement tests and run them.
7. Run warm-up and full benchmark suites (three measured runs per main scenario).
8. Analyze bottlenecks; optimize one important variable at a time; preserve baseline.
9. Rerun and compare baseline vs optimized.
10. Generate docs and reproducible final report.

## Definition of done
Runnable source; successful build; local Docker Kafka operational; topics created; consumer/producer/generator functional; tests executed; 1M benchmark executed or exact blocker documented; independent output integrity validated; reports saved; README reproduces everything.

## Final response
Summarize files, commands, tests, actual message counts, elapsed times, throughput, p50/p95/p99, CPU/memory, missing/duplicates, best configuration, baseline vs optimized and limitations. Clearly distinguish PASS, FAIL, BLOCKED and NOT TESTED.

## Final command
Begin now. Inspect the repository and environment, create real files, start local Kafka broker, implement everything, execute all possible tests and benchmarks, fix failures and produce the final report. Do not ask questions, do not return only a plan, and do not mark unexecuted tests successful.
