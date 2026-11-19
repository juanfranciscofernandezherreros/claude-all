#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MESSAGES="${1:-1000000}"
RESULTS="benchmark-results"
mkdir -p "$RESULTS"
printf 'concurrency,runId,messages,publishSeconds,throughputPerSecond,status\n' > "$RESULTS/results.csv"
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
    start_ns=$(date +%s%N)
    docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
      --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
      --messages="$MESSAGES" --run-id="$run_id" | tee "$RESULTS/$run_id/execution.log"
    docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
      --spring.main.web-application-type=none --lab.consumer.enabled=false --validate \
      --run-id="$run_id" --messages="$MESSAGES" --timeout-seconds=1800 \
      | tee "$RESULTS/$run_id/validation-console.json"
    end_ns=$(date +%s%N)
    elapsed=$(python3 -c "print(($end_ns-$start_ns)/1e9)")
    python3 - "$RESULTS/$run_id" "$RESULTS/results.csv" "$concurrency" "$run_id" "$MESSAGES" "$elapsed" <<'PY'
import json,sys,platform,os
folder,csv_path,concurrency,run_id,messages,elapsed=sys.argv[1:]
execution=[x for x in open(folder+"/execution.log",encoding="utf-8") if x.startswith('{"runId"')][-1]
generated=json.loads(execution)
validation=json.load(open(folder+"/validation.json",encoding="utf-8"))
environment={"os":platform.platform(),"architecture":platform.machine(),"processors":os.cpu_count(),"java":"21 (configured; runtime capture pending)","docker":"not captured by script"}
summary={"runId":run_id,"concurrency":int(concurrency),"messages":int(messages),"publishSeconds":generated["publishSeconds"],"endToEndSeconds":float(elapsed),"validatedUniqueOutputs":validation["valid"],"throughputPerSecond":validation["valid"]/float(elapsed),"validation":validation["status"],"latency":validation["latency"]}
with open(folder+"/summary.json","w",encoding="utf-8") as f: json.dump(summary,f,indent=2)
with open(folder+"/environment.json","w",encoding="utf-8") as f: json.dump(environment,f,indent=2)
with open(folder+"/report.md","w",encoding="utf-8") as f:
    f.write("# Benchmark run\n\n")
    f.write(f"- Run: {run_id}\n- Concurrency: {concurrency}\n- Messages: {messages}\n")
    f.write(f"- Publish: {generated['publishSeconds']:.3f}s\n- End-to-end: {float(elapsed):.3f}s\n")
    f.write(f"- Validated throughput: {summary['throughputPerSecond']:.1f} messages/s\n- Integrity: {validation['status']}\n")
with open(csv_path,"a",encoding="utf-8") as f:
    f.write(f"{concurrency},{run_id},{messages},{generated['publishSeconds']},{summary['throughputPerSecond']},{validation['status']}\n")
PY
  done
done
echo "Benchmark runs complete. See benchmark-results/results.csv and per-run reports."
