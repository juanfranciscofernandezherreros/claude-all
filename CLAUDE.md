# Claude Code instructions

Read `PRD.md` completely before work; it is authoritative.

- Zero questions: make reasonable technical decisions autonomously.
- Implement, build, run, test, benchmark and document; do not return only a plan.
- Provision Kafka locally with Docker Compose in KRaft mode, including broker startup/readiness and topic creation. User must not manually start Kafka.
- Never claim tests or benchmarks passed unless actually executed.
- Never fabricate metrics. Document blockers precisely and continue independent work.
- Preserve unrelated files; inspect repository before changing existing content.
- Store benchmark outputs under `benchmark-results/`; document reproducible commands in `README.md`.
