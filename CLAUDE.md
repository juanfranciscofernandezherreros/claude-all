# Claude Code project instructions

Read `PRD.md` before doing work. It is the authoritative execution specification.

- Zero questions: make reasonable technical decisions autonomously.
- Implement, build, run, test, benchmark and document; do not return only a plan.
- The user expects Kafka to be provisioned locally by Docker Compose, including KRaft broker startup and topic creation.
- Never claim tests or benchmarks passed unless actually executed.
- Never fabricate metrics. Record blockers precisely and continue independent work.
- Preserve unrelated files and inspect the repository before changing existing content.
- Keep benchmark outputs under `benchmark-results/` and document reproducible commands in `README.md`.
