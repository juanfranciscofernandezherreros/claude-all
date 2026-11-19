#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MESSAGES="${1:-1000000}"
RESULTS="benchmark-results"
mkdir -p "$RESULTS"
printf 'concurrency,runId,messages,publishSeconds,status\n' > "$RESULTS/results.csv"
docker compose up -d --build
for concurrency in 1 2 5 10 20; do
  CONCURRENCY="$concurrency" docker compose up -d --force-recreate app
  warmup="warmup-c${concurrency}-$(date +%s)-$$"
  echo "Warm-up concurrency=$concurrency"
  docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
    --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
    --messages=10000 --run-id="$warmup" > "$RESULTS/$warmup.log" 2>&1
  for repetition in 1 2 3; do
    run_id="c${concurrency}-r${repetition}-$(date -u +%Y%m%dT%H%M%S)-$$"
    mkdir -p "$RESULTS/$run_id"
    echo "Run $run_id"
    docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
      --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
      --messages="$MESSAGES" --run-id="$run_id" | tee "$RESULTS/$run_id/execution.log"
    docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
      --spring.main.web-application-type=none --lab.consumer.enabled=false --validate \
      --run-id="$run_id" --messages="$MESSAGES" --timeout-seconds=1800 \
      | tee "$RESULTS/$run_id/validation-console.json"
    python3 - "$RESULTS/$run_id/execution.log" "$RESULTS/results.csv" "$concurrency" "$run_id" "$MESSAGES" <<'PY'
import json,sys
line=[x for x in open(sys.argv[1],encoding="utf-8") if x.startswith('{"runId"')][-1]
data=json.loads(line)
with open(sys.argv[2],"a",encoding="utf-8") as f:
    f.write(f"{sys.argv[3]},{sys.argv[4]},{sys.argv[5]},{data['publishSeconds']},VALIDATED\n")
PY
  done
done
echo "Benchmark runs complete. See $RESULTS/results.csv and per-run validation.json."
