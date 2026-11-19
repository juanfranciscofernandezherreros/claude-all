#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--dry-run" ]]; then
  messages="${2:-${STRESS_MESSAGES:-5000000}}"
  concurrency="${3:-${STRESS_CONCURRENCY:-20}}"
  payload="${4:-${STRESS_PAYLOAD_BYTES:-102400}}"
  workload="${5:-${STRESS_WORKLOAD:-HEAVY}}"
  delivery="${6:-${STRESS_DELIVERY_MODE:-EXACTLY_ONCE}}"
  step="${STRESS_STEP_MESSAGES:-100000}"
  repetitions="${STRESS_REPETITIONS:-1}"
  python3 - "$messages" "$step" "$concurrency" "$payload" "$workload" "$delivery" "$repetitions" "${WARMUP_MESSAGES:-10000}" <<'PY'
import json, sys
messages, step, concurrency, payload, repetitions, warmup = map(
    int, (sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[7], sys.argv[8])
)
workload, delivery = sys.argv[5:7]
assert messages > 0 and step > 0 and repetitions > 0 and warmup > 0
assert concurrency in (1, 2, 5, 10, 20)
assert payload in (1024, 5120, 10240, 51200, 102400)
assert workload in ("LIGHT", "MEDIUM", "HEAVY")
assert delivery in ("AT_LEAST_ONCE", "EXACTLY_ONCE")
print(json.dumps({
    "maxMessages": messages, "stepMessages": step, "concurrency": concurrency,
    "payloadBytes": payload, "workload": workload, "deliveryMode": delivery,
    "warmupMessages": warmup, "repetitionsPerStep": repetitions,
    "stopConditions": {
        "validationErrors": "any non-zero error count",
        "memoryPercent": ">85",
        "consumerLag": "increasing for 3 consecutive steps"
    }
}, indent=2))
PY
  exit 0
fi

messages="${1:-${STRESS_MESSAGES:-5000000}}"
concurrency="${2:-${STRESS_CONCURRENCY:-20}}"
payload="${3:-${STRESS_PAYLOAD_BYTES:-102400}}"
workload="${4:-${STRESS_WORKLOAD:-HEAVY}}"
delivery="${5:-${STRESS_DELIVERY_MODE:-EXACTLY_ONCE}}"
export SCENARIO_REPETITIONS="${STRESS_REPETITIONS:-1}"
echo "Starting incremental stress run: max=$messages step=${STRESS_STEP_MESSAGES:-100000} concurrency=$concurrency payload=$payload workload=$workload delivery=$delivery repetitions=$SCENARIO_REPETITIONS"
python3 scripts/stress_runner.py "$messages" "${STRESS_STEP_MESSAGES:-100000}" "$concurrency" "$payload" "$workload" "$delivery"
