#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--dry-run" ]]; then
  MAIN_MESSAGES="${2:-1000000}"
  SECONDARY_MESSAGES="${SECONDARY_MESSAGES:-100000}"
  WARMUP_MESSAGES="${WARMUP_MESSAGES:-10000}"
  python3 - "$MAIN_MESSAGES" "$SECONDARY_MESSAGES" "$WARMUP_MESSAGES" <<'PY'
import json, sys
main_messages, secondary_messages, warmup_messages = map(int, sys.argv[1:])
matrix = {
    "main": {
        "messages": main_messages,
        "concurrencies": [1, 2, 5, 10, 20],
        "payloadBytes": 1024,
        "workload": "LIGHT",
        "deliveryMode": "AT_LEAST_ONCE",
        "warmupMessages": warmup_messages,
        "repetitions": 3,
    },
    "payload": [
        {"messages": secondary_messages, "concurrency": 5, "payloadBytes": size,
         "workload": "LIGHT", "deliveryMode": "AT_LEAST_ONCE",
         "warmupMessages": warmup_messages, "repetitions": 3}
        for size in (1024, 5120, 10240, 51200, 102400)
    ],
    "workload": [
        {"messages": secondary_messages, "concurrency": 5, "payloadBytes": 1024,
         "workload": workload, "deliveryMode": "AT_LEAST_ONCE",
         "warmupMessages": warmup_messages, "repetitions": 3}
        for workload in ("LIGHT", "MEDIUM", "HEAVY")
    ],
    "delivery": [
        {"messages": secondary_messages, "concurrency": 5, "payloadBytes": 1024,
         "workload": "LIGHT", "deliveryMode": delivery,
         "warmupMessages": warmup_messages, "repetitions": 3}
        for delivery in ("AT_LEAST_ONCE", "EXACTLY_ONCE")
    ],
}
print(json.dumps(matrix, indent=2))
PY
  exit 0
fi

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
