# Kafka Performance Lab

Windows: run `RUN-KAFKA-LAB.bat`.

Requires Docker Desktop and authenticated Claude Code CLI. The launcher starts Claude in this repository. Claude reads `PRD.md`, builds the Java app, provisions local Kafka KRaft with Docker Compose, tests, benchmarks 1M messages and saves real reports under `benchmark-results/`.

See `PRD.md` for all requirements.