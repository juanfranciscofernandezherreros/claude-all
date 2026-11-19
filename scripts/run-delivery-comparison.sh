#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

messages="\${1:-1000000}"
concurrency="\${2:-\${BENCHMARK_CONCURRENCY:-10}}"
repetitions="\${3:-\${SCENARIO_REPETITIONS:-3}}"
warmup_messages="\${WARMUP_MESSAGES:-10000}"

for value in "$messages" "$concurrency" "$repetitions" "$warmup_messages"; do
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: messages, concurrency, repetitions and warm-up must be positive integers." >&2
    exit 2
  fi
done
case "$concurrency" in
  1|2|5|10|20) ;;
  *) echo "ERROR: concurrency must be one of 1, 2, 5, 10 or 20." >&2; exit 2 ;;
esac

# Fixed workload and producer tuning for a reproducible apples-to-apples comparison.
export CONCURRENCY="$concurrency"
export BATCH_SIZE="\${BATCH_SIZE:-1000}"
export PRODUCER_BATCH_BYTES="\${PRODUCER_BATCH_BYTES:-131072}"
export GENERATOR_THREADS="\${GENERATOR_THREADS:-4}"
export PAYLOAD_BYTES=1024
export WORKLOAD=LIGHT
export COMPRESSION_TYPE="\${COMPRESSION_TYPE:-lz4}"
export LINGER_MS="\${LINGER_MS:-5}"
export WARMUP_MESSAGES="$warmup_messages"
export SCENARIO_REPETITIONS=1

mkdir -p benchmark-results
chmod 777 benchmark-results
if [[ ! -s benchmark-results/results.csv ]]; then
  printf 'scenario,runId,concurrency,messages,payloadBytes,workload,deliveryMode,publishSeconds,throughputPerSecond,status\n' > benchmark-results/results.csv
fi

comparison_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
scenario_prefix="delivery-1m-$comparison_id"
echo "Starting delivery comparison: messages=$messages concurrency=$concurrency repetitions=$repetitions"
echo "Fixed settings: payload=1024 workload=LIGHT batch=$BATCH_SIZE producerBatchBytes=$PRODUCER_BATCH_BYTES compression=$COMPRESSION_TYPE linger=$LINGER_MS"

docker compose up -d --build

# Alternate modes in each pair to reduce ordering/thermal bias.
for repetition in $(seq 1 "$repetitions"); do
  for delivery in AT_LEAST_ONCE EXACTLY_ONCE; do
    bash scripts/run-scenario.sh "$scenario_prefix-$delivery-pair$repetition" \
      "$messages" "$concurrency" 1024 LIGHT "$delivery"
  done
done

python3 - "$scenario_prefix" "$messages" "$concurrency" "$repetitions" <<'PY'
import csv
import json
import os
import statistics
import sys
from pathlib import Path

prefix, messages, concurrency, repetitions = sys.argv[1:]
messages, concurrency, repetitions = int(messages), int(concurrency), int(repetitions)
root = Path("benchmark-results")
with (root / "results.csv").open(newline="", encoding="utf-8") as stream:
    rows = [row for row in csv.DictReader(stream) if row["scenario"].startswith(prefix)]

comparison = {"scenarioPrefix": prefix, "messagesPerRun": messages,
              "concurrency": concurrency, "repetitionsPerMode": repetitions,
              "fixedWorkload": {"payloadBytes": 1024, "workload": "LIGHT",
                                "batchSize": int(os.environ["BATCH_SIZE"]),
                                "producerBatchBytes": int(os.environ["PRODUCER_BATCH_BYTES"]),
                                "compression": os.environ["COMPRESSION_TYPE"],
                                "lingerMs": int(os.environ["LINGER_MS"])},
              "modes": {}, "comparisonStatus": "FAIL"}
for mode in ("AT_LEAST_ONCE", "EXACTLY_ONCE"):
    selected = [row for row in rows if row["deliveryMode"] == mode]
    if len(selected) != repetitions:
        raise SystemExit(f"Expected {repetitions} {mode} runs, found {len(selected)}")
    throughputs, publish_times, e2e_times = [], [], []
    validations = []
    for row in selected:
        folder = root / row["runId"]
        summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
        validation = json.loads((folder / "validation.json").read_text(encoding="utf-8"))
        if (validation["status"] != "PASS" or validation["valid"] != messages
                or validation["missing"] != 0 or validation["duplicate"] != 0
                or validation["unexpected"] != 0 or validation["invalid"] != 0
                or validation["dlt"] != 0):
            raise SystemExit(f"Integrity validation failed for {row['runId']}: {validation}")
        throughputs.append(float(summary["throughputPerSecond"]))
        publish_times.append(float(summary["publishSeconds"]))
        e2e_times.append(float(summary["endToEndSeconds"]))
        validations.append({"runId": row["runId"], "status": validation["status"],
                            "valid": validation["valid"], "missing": validation["missing"],
                            "duplicate": validation["duplicate"], "unexpected": validation["unexpected"],
                            "invalid": validation["invalid"], "dlt": validation["dlt"]})
    comparison["modes"][mode] = {
        "runs": validations,
        "medianThroughputPerSecond": statistics.median(throughputs),
        "medianPublishSeconds": statistics.median(publish_times),
        "medianEndToEndSeconds": statistics.median(e2e_times),
        "minThroughputPerSecond": min(throughputs),
        "maxThroughputPerSecond": max(throughputs)
    }

at_least = comparison["modes"]["AT_LEAST_ONCE"]["medianThroughputPerSecond"]
exactly = comparison["modes"]["EXACTLY_ONCE"]["medianThroughputPerSecond"]
comparison["exactlyOnceThroughputVsAtLeastOncePercent"] = ((exactly / at_least) - 1) * 100 if at_least else None
comparison["comparisonStatus"] = "PASS"
json_path = root / f"{prefix}-comparison.json"
md_path = root / f"{prefix}-comparison.md"
json_path.write_text(json.dumps(comparison, indent=2), encoding="utf-8")
with md_path.open("w", encoding="utf-8") as report:
    report.write("# AT_LEAST_ONCE vs EXACTLY_ONCE — one-million-message comparison\n\n")
    report.write(f"- Messages per run: {messages}\n- Concurrency: {concurrency}\n")
    report.write(f"- Repetitions per mode: {repetitions}\n- Integrity: PASS for every run\n\n")
    report.write("| Mode | Median publish (s) | Median end-to-end (s) | Median validated msg/s |\n")
    report.write("|---|---:|---:|---:|\n")
    for mode in ("AT_LEAST_ONCE", "EXACTLY_ONCE"):
        values = comparison["modes"][mode]
        report.write(f"| {mode} | {values['medianPublishSeconds']:.3f} | "
                     f"{values['medianEndToEndSeconds']:.3f} | {values['medianThroughputPerSecond']:.1f} |\n")
    report.write(f"\nEXACTLY_ONCE throughput vs AT_LEAST_ONCE: "
                 f"{comparison['exactlyOnceThroughputVsAtLeastOncePercent']:+.2f}%\n")
    report.write("\nNo result is considered valid unless the independent validator reports "
                 "zero missing, duplicate, unexpected, invalid and DLT records.\n")
print(json.dumps({"status": "PASS", "json": str(json_path), "report": str(md_path),
                  "modes": comparison["modes"],
                  "exactlyOnceThroughputVsAtLeastOncePercent":
                      comparison["exactlyOnceThroughputVsAtLeastOncePercent"]}, indent=2))
PY

echo "Delivery comparison complete. See benchmark-results/$scenario_prefix-comparison.md"
