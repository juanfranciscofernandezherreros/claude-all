#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/run-benchmark.sh "${MESSAGES:-1000000}"
echo "Main concurrency suite complete. Payload/workload/delivery suites require the corresponding scenarios; EXACTLY_ONCE remains pending transactional integration."
