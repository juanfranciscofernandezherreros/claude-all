#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MESSAGES="${1:-1000000}"
SECONDARY_MESSAGES="${SECONDARY_MESSAGES:-100000}"
bash scripts/run-benchmark.sh "$MESSAGES"
for payload in 1024 5120 10240 51200 102400; do
  bash scripts/run-scenario.sh "payload-${payload}" "$SECONDARY_MESSAGES" 5 "$payload" LIGHT AT_LEAST_ONCE
done
for workload in LIGHT MEDIUM HEAVY; do
  bash scripts/run-scenario.sh "workload-${workload}" "$SECONDARY_MESSAGES" 5 1024 "$workload" AT_LEAST_ONCE
done
for delivery in AT_LEAST_ONCE EXACTLY_ONCE; do
  bash scripts/run-scenario.sh "delivery-${delivery}" "$SECONDARY_MESSAGES" 5 1024 LIGHT "$delivery"
done
echo "Full benchmark suite complete. See benchmark-results/results.csv and per-run reports."
