# Kafka Performance Lab — Specification

## Goal
Build and run a complete local Java/Kafka system that consumes, transforms and publishes 1,000,000 messages; independently validate outputs and report real performance. Deliver working code, tests, Docker environment and reproducible reports—not just a plan.

## Fixed decisions
- Java 21, Spring Boot, Spring Kafka, Maven, JSON.
- Apache Kafka, single local broker, KRaft (no ZooKeeper), Docker Compose.
- 24 partitions; replication factor 1 locally.
- Micrometer, Prometheus, Grafana, JUnit 5, Testcontainers.
- Pin compatible dependency/image versions; never use `latest`.

## Flow
Java generator -> `input-topic` -> Consumer -> deterministic transform -> Producer -> `output-topic` -> independent validator.
Also create `input-dlt-topic`.
Configure Docker-internal and localhost listeners. Compose must health-check Kafka, wait for readiness and create topics automatically. User must not install/start Kafka manually.

## Message rules
Input: unique `messageId`, timestamp, event type, JSON payload.
Output: same ID, processing timestamp, status and result.
One logical output per valid input. Invalid/permanently failed records go to DLT. Preserve IDs; never silently discard failures.

## Delivery
Implement and compare:
- `AT_LEAST_ONCE`: manual commit after output acknowledgement, idempotent producer, retries; document duplicate possibility.
- `EXACTLY_ONCE`: Kafka transaction atomically publishes output and commits input offsets; unique transactional IDs; `read_committed`; rollback/recovery tests.
Do not claim guarantees beyond Kafka's transactional boundary.

## Configuration/defaults
All tunables via environment: broker/topics/group, concurrency, poll/batch/linger/compression, delivery mode, workload, message count, payload size, generator threads.
Defaults: 1,000,000 messages, 24 partitions, concurrency 1, batch 500, 4 generator threads, payload 1 KB.
Concurrency: 1/2/5/10/20. Payload: 1/5/10/50/100 KB. Workload: LIGHT/MEDIUM/HEAVY, deterministic CPU work, no artificial delay by default.

## Generator and observability
Independent Java generator: unique IDs, configurable count/size/rate/batch/threads, progress/errors. Measure input publishing separately.
Expose Micrometer/Prometheus metrics: received, processed, published, failed, retried, DLT, processing/publish/end-to-end latency, throughput, consumer lag, JVM/GC, CPU/memory where available. Report p50/p95/p99; no message IDs as metric labels.

## Benchmark
Automate warm-up plus 3 measured runs per principal scenario; preserve each run and report median.
Suites:
1. 1M messages at concurrency 1/2/5/10/20.
2. Payload sizes 1/5/10/50/100 KB.
3. LIGHT/MEDIUM/HEAVY processing.
4. AT_LEAST_ONCE vs EXACTLY_ONCE.
5. Stress until throughput plateaus, lag/resources/errors become limiting.
6. Recovery: consumer/broker restart, invalid JSON, processing/publish failure, transaction rollback.

Measure separately:
- Input publish duration.
- Consumer processing duration.
- End-to-end duration (input publish start until every expected output is independently validated).

Use monotonic clocks for durations. Throughput = validated outputs / elapsed seconds. Error rate = failed inputs / total inputs. Latency = output confirmation minus input creation; report min/mean/p50/p95/p99/max. Never use consumed count as successful throughput.

## Validation and acceptance
Independent memory-bounded/disk-backed validator compares expected and actual IDs; report missing, duplicate, unexpected, failed and DLT records.
Functional pass: all valid inputs accounted for, exactly one logical output each, zero missing/duplicate outputs, IDs preserved, failures accounted for, offsets/recovery correct.
Initial target (not guaranteed): 1M end-to-end under 60s (~16,667 validated msg/s). Report actual result and hardware constraints honestly.

## Tests
Unit: JSON, validation, transform, IDs, errors, metrics.
Integration/e2e: Kafka consume/publish, transactions, offsets, retry, DLT and recovery. Use JUnit 5/Testcontainers.

## Deliverables
Complete app, generator, Dockerfile/Compose, Kafka KRaft, Prometheus/Grafana dashboard, tests, scripts and docs.
Each run saves `summary.json`, `results.csv`, `validation.json`, `environment.json`, `execution.log`, `report.md` under `benchmark-results/`.
Implement: `scripts/start-environment.sh`, `stop-environment.sh`, `run-benchmark.sh --messages 1000000`, `run-full-suite.sh --scenario concurrency|payload|reliability`, Windows .bat launchers, `mvn clean verify`.

## Autonomous execution / Definition of Done
Ask ZERO questions; all decisions are defined. Choose sensible defaults for unspecified details. Do not request routine approvals. Inspect repo/environment; preserve unrelated files. Build everything, start local Kafka, wait for broker, create topics, run tests, execute benchmarks, diagnose/fix failures, optimize one variable at a time, rerun and document baseline vs optimized. If blocked by permissions/dependency/environment, state exact blocker and continue independent work. Never fabricate results or mark unexecuted tests PASS.
Done = runnable source, successful build, local Kafka, working flow/generator, tests executed, 1M benchmark or exact blocker, independent integrity validation, reports and reproducible README.

Final response: commands, tests, actual counts/times/throughput/latency/resources, integrity, best config, baseline vs optimized, limitations. Start now; do not return only a plan.
