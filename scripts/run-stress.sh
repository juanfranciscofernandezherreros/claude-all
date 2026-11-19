#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${1:-}" == "--dry-run" ]]; then
  messages="${2:-${STRESS_MESSAGES:-5000000}}"
  concurrency="${3:-${STRESS_CONCURRENCY:-20}}"
  payload="${4:-${STRESS_PAYLOAD_BYTES:-102400}}"
  workload="${5:-${STRESS_WORKLOAD:-HEAVY}}"
  delivery="${6:-${STRESS_DELIVERY_MODE:-EXACTLY_ONCE}}"
  python3 - "$messages" "$concurrency" "$payload" "$workload" "$delivery" "${WARMUP_MESSAGES:-10000}" <<'PY'
import json, sys
messages, concurrency, payload, warmup = map(int, (sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[6]))
workload, delivery = sys.argv[4:6]
assert messages > 0
assert concurrency in (1, 2, 5, 10, 20)
assert payload in (1024, 5120, 10240, 51200, 102400)
assert workload in ("LIGHT", "MEDIUM", "HEAVY")
assert delivery in ("AT_LEAST_ONCE", "EXACTLY_ONCE")
print(json.dumps({
    "messages": messages, "concurrency": concurrency, "payloadBytes": payload,
    "workload": workload, "deliveryMode": delivery,
    "warmupMessages": warmup, "repetitions": 3
}, indent=2))
PY
  exit 0
fi

messages="${1:-${STRESS_MESSAGES:-5000000}}"
concurrency="${2:-${STRESS_CONCURRENCY:-20}}"
payload="${3:-${STRESS_PAYLOAD_BYTES:-102400}}"
workload="${4:-${STRESS_WORKLOAD:-HEAVY}}"
delivery="${5:-${STRESS_DELIVERY_MODE:-EXACTLY_ONCE}}"
name="stress-${concurrency}c-${payload}b-${workload}-${delivery}-$(date -u +%Y%m%dT%H%M%SZ)"
echo "Starting stress scenario: messages=$messages concurrency=$concurrency payload=$payload workload=$workload delivery=$delivery"
bash scripts/run-scenario.sh "$name" "$messages" "$concurrency" "$payload" "$workload" "$delivery"
