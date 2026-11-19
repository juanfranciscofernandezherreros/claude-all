#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <name> <messages> <concurrency> <payloadBytes> <workload> <deliveryMode>" >&2
  exit 2
fi
name="$1"; messages="$2"; concurrency="$3"; payload="$4"; workload="$5"; delivery="$6"
results="benchmark-results"
python_bin=$(command -v python3 || command -v python || true)
if [ -z "$python_bin" ]; then echo "ERROR: Python 3 is required to assemble benchmark reports." >&2; exit 1; fi
mkdir -p "$results"
chmod 777 "$results"
export CONCURRENCY="$concurrency" PAYLOAD_BYTES="$payload" WORKLOAD="$workload" DELIVERY_MODE="$delivery"
docker compose up -d --force-recreate app
for attempt in $(seq 1 60); do
  if docker compose exec -T app java -version >/dev/null 2>&1; then break; fi
  sleep 2
done
docker compose exec -T app java -version >/dev/null 2>&1 || { echo "ERROR: application container did not start." >&2; exit 1; }
java_version=$(docker compose exec -T app java -version 2>&1 | head -1)
docker_version=$(docker version --format '{{.Server.Version}}')
warmup="warmup-${name}-$(date +%s)-$$"
mkdir -p "$results/$warmup"
echo "Warm-up: $name"
docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
  --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
  --messages=10000 --run-id="$warmup" --timeout-seconds="${GENERATOR_TIMEOUT_SECONDS:-600}" > "$results/$warmup/execution.log" 2>&1
for repetition in 1 2 3; do
  run_id="${name}-r${repetition}-$(date -u +%Y%m%dT%H%M%S)-$$"
  mkdir -p "$results/$run_id"
  echo "Run $run_id"
  docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
    --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
    --messages="$messages" --run-id="$run_id" --timeout-seconds="${GENERATOR_TIMEOUT_SECONDS:-600}" | tee "$results/$run_id/execution.log"
  docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
    --spring.main.web-application-type=none --lab.consumer.enabled=false --validate \
    --run-id="$run_id" --messages="$messages" --timeout-seconds=1800 \
    | tee "$results/$run_id/validation-console.json"
docker_memory_usage=$(docker stats --no-stream --format '{{.MemUsage}}' kafka-lab-app 2>/dev/null || true)
prometheus_metrics=$(curl -fsS http://localhost:8080/actuator/prometheus 2>/dev/null || true)
consumer_lag=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group kafka-lab 2>/dev/null | awk 'NR>1 && $6 ~ /^[0-9]+$/ {sum += $6; found=1} END {if (found) print sum; else print ""}' || true)
  "$python_bin" - "$results/$run_id" "$results/results.csv" "$name" "$concurrency" "$messages" "$payload" "$workload" "$delivery" "$java_version" "$docker_version" "$docker_memory_usage" "$prometheus_metrics" "$consumer_lag" <<'PY'
import json,sys,platform,os,datetime,re
folder,csv_path,name,concurrency,messages,payload,workload,delivery,java_version,docker_version,docker_memory_usage,prometheus_metrics,consumer_lag=sys.argv[1:]
execution=[x for x in open(folder+"/execution.log",encoding="utf-8") if x.startswith('{"runId"')][-1]
generated=json.loads(execution)
validation=json.load(open(folder+"/validation.json",encoding="utf-8"))
validated_at=datetime.datetime.fromisoformat(validation["timestamp"].replace("Z","+00:00")).timestamp()
end_to_end=max(0.0,validated_at-generated["publishStartEpochMs"]/1000.0)
throughput=validation["valid"]/end_to_end if end_to_end else 0
def bytes_from_docker_stats(value):
    match=re.match(r"\s*([0-9.]+)\s*([KMGT]?i?B)",value)
    if not match: return None
    unit=match.group(2)
    factors={"B":1,"kB":1000,"MB":1000**2,"GB":1000**3,"TB":1000**4,
             "KiB":1024,"MiB":1024**2,"GiB":1024**3,"TiB":1024**4}
    return int(float(match.group(1))*factors[unit])
def metric_sum(name, selector=None):
    total=0.0
    for line in prometheus_metrics.splitlines():
        if line.startswith(name) and (selector is None or selector in line):
            try: total += float(line.rsplit(" ",1)[1])
            except (ValueError,IndexError): pass
    return total if prometheus_metrics else None
try: lag_value=int(consumer_lag.strip())
except (ValueError,AttributeError): lag_value=None
environment={"os":platform.platform(),"architecture":platform.machine(),"processor":platform.processor() or None,
             "logicalProcessors":os.cpu_count(),"java":java_version,"dockerEngine":docker_version,
             "dockerMemoryUsage":docker_memory_usage or None,"memoryBytes":bytes_from_docker_stats(docker_memory_usage),
             "heapUsedBytes":metric_sum("jvm_memory_used_bytes", 'area="heap"'),
             "gcPauseCount":metric_sum("jvm_gc_pause_seconds_count"),
             "gcPauseSeconds":metric_sum("jvm_gc_pause_seconds_sum"),
             "kafkaConsumerLag":lag_value,
             "resourceSample":"single snapshot after validation; null means metric unavailable"}
summary={"scenario":name,"runId":generated["runId"],"concurrency":int(concurrency),"messages":int(messages),"payloadBytes":int(payload),"workload":workload,"deliveryMode":delivery,"publishSeconds":generated["publishSeconds"],"endToEndSeconds":end_to_end,"validatedUniqueOutputs":validation["valid"],"throughputPerSecond":throughput,"validation":validation["status"],"latency":validation["latency"]}
with open(folder+"/summary.json","w",encoding="utf-8") as f: json.dump(summary,f,indent=2)
with open(folder+"/environment.json","w",encoding="utf-8") as f: json.dump(environment,f,indent=2)
with open(folder+"/report.md","w",encoding="utf-8") as f:
    f.write("# Benchmark run\n\n")
    f.write(f"- Scenario: {name}\n- Run: {generated['runId']}\n- Concurrency: {concurrency}\n- Messages: {messages}\n")
    f.write(f"- Payload: {payload} bytes\n- Workload: {workload}\n- Delivery: {delivery}\n")
    f.write(f"- Publish: {generated['publishSeconds']:.3f}s\n- End-to-end: {end_to_end:.3f}s\n")
    f.write(f"- Validated throughput: {throughput:.1f} messages/s\n- Integrity: {validation['status']}\n")
header="scenario,runId,concurrency,messages,payloadBytes,workload,deliveryMode,publishSeconds,throughputPerSecond,status\n"
if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
    with open(csv_path,"w",encoding="utf-8",newline="") as f:
        f.write(header)
with open(csv_path,"a",encoding="utf-8",newline="") as f:
    f.write(f"{name},{generated['runId']},{concurrency},{messages},{payload},{workload},{delivery},{generated['publishSeconds']},{throughput},{validation['status']}\n")
PY
done
