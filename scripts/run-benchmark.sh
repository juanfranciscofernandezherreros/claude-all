#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MESSAGES="${1:-1000000}"
mkdir -p benchmark-results
chmod 777 benchmark-results
printf 'scenario,runId,concurrency,messages,payloadBytes,workload,deliveryMode,publishSeconds,throughputPerSecond,status\n' > benchmark-results/results.csv
docker compose up -d --build
for concurrency in 1 2 5 10 20; do
  bash scripts/run-scenario.sh "main-c${concurrency}" "$MESSAGES" "$concurrency" 1024 LIGHT AT_LEAST_ONCE
done
echo "Main benchmark suite complete. See benchmark-results/results.csv."
