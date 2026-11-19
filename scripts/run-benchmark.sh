#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MESSAGES="${1:-1000000}"
RESULTS="benchmark-results"
PYTHON=$(command -v python3 || command -v python || true)
if [ -z "$PYTHON" ]; then echo "ERROR: Python 3 is required to assemble reports." >&2; exit 1; fi
mkdir -p "$RESULTS"
printf 'concurrency,runId,messages,publishSeconds,throughputPerSecond,status\n' > "$RESULTS/results.csv"
docker compose up -d --build
JAVA_VERSION=$(docker compose exec -T app java -version 2>&1 | head -1)
DOCKER_VERSION=$(docker version --format '{{.Server.Version}}')
export JAVA_VERSION DOCKER_VERSION
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
    "$PYTHON" - "$RESULTS/$run_id" "$RESULTS/results.csv" "$concurrency" "$run_id" "$MESSAGES" "$JAVA_VERSION" "$DOCKER_VERSION" <<'PY'
import json,sys,platform,os,datetime
folder,csv_path,concurrency,run_id,messages,java_version,docker_version=sys.argv[1:]
execution=[x for x in open(folder+"/execution.log",encoding="utf-8") if x.startswith('{"runId"')][-1]
generated=json.loads(execution)
validation=json.load(open(folder+"/validation.json",encoding="utf-8"))
validated_at=datetime.datetime.fromisoformat(validation["timestamp"].replace("Z","+00:00")).timestamp()
end_to_end=max(0.0,validated_at-generated["publishStartEpochMs"]/1000.0)
throughput=validation["valid"]/end_to_end if end_to_end else 0
environment={"os":platform.platform(),"architecture":platform.machine(),"processor":platform.processor() or None,"logicalProcessors":os.cpu_count(),"java":java_version,"dockerEngine":docker_version,"memoryBytes":None,"memoryStatus":"not captured by portable launcher"}
summary={"runId":run_id,"concurrency":int(concurrency),"messages":int(messages),"publishSeconds":generated["publishSeconds"],"endToEndSeconds":end_to_end,"validatedUniqueOutputs":validation["valid"],"throughputPerSecond":throughput,"validation":validation["status"],"latency":validation["latency"]}
with open(folder+"/summary.json","w",encoding="utf-8") as f: json.dump(summary,f,indent=2)
with open(folder+"/environment.json","w",encoding="utf-8") as f: json.dump(environment,f,indent=2)
with open(folder+"/report.md","w",encoding="utf-8") as f:
    f.write("# Benchmark run\n\n")
    f.write(f"- Run: {run_id}\n- Concurrency: {concurrency}\n- Messages: {messages}\n")
    f.write(f"- Publish: {generated['publishSeconds']:.3f}s\n- End-to-end: {end_to_end:.3f}s\n")
    f.write(f"- Validated throughput: {throughput:.1f} messages/s\n- Integrity: {validation['status']}\n")
with open(csv_path,"a",encoding="utf-8") as f:
    f.write(f"{concurrency},{run_id},{messages},{generated['publishSeconds']},{throughput},{validation['status']}\n")
PY
  done
done
echo "Benchmark runs complete. See benchmark-results/results.csv and per-run reports."
