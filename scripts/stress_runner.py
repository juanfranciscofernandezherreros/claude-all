#!/usr/bin/env python3
"""Run validated Kafka stress scenarios in cumulative 100k-message steps."""

import csv
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import sys

from stress_policy import evaluate_step


def read_step_artifacts(results: Path, scenario: str, repetitions: int):
    folders = sorted(results.glob(f"{scenario}-r*"))
    reports = []
    for folder in folders:
        validation_path = folder / "validation.json"
        environment_path = folder / "environment.json"
        if not validation_path.is_file() or not environment_path.is_file():
            continue
        validation = json.loads(validation_path.read_text(encoding="utf-8"))
        environment = json.loads(environment_path.read_text(encoding="utf-8"))
        errors = sum(int(validation.get(key, 0) or 0)
                     for key in ("missing", "duplicate", "unexpected", "invalid", "dlt"))
        reports.append({
            "runId": folder.name,
            "status": validation.get("status", "FAIL"),
            "errors": errors,
            "memoryPercentMax": environment.get("memoryPercentMax"),
            "kafkaConsumerLagMax": environment.get("kafkaConsumerLagMax"),
        })
    if len(reports) != repetitions:
        raise RuntimeError(
            f"{scenario}: expected {repetitions} validation/environment pairs, found {len(reports)}"
        )
    return reports


def main():
    if len(sys.argv) != 7:
        raise SystemExit(
            "Usage: stress_runner.py <maxMessages> <stepMessages> <concurrency> "
            "<payloadBytes> <workload> <deliveryMode>"
        )

    max_messages, step_messages, concurrency, payload = map(int, sys.argv[1:5])
    workload, delivery = sys.argv[5:7]
    if min(max_messages, step_messages) < 1:
        raise SystemExit("maxMessages and stepMessages must be positive")
    if concurrency not in (1, 2, 5, 10, 20):
        raise SystemExit("unsupported concurrency")
    if payload not in (1024, 5120, 10240, 51200, 102400):
        raise SystemExit("unsupported payload size")
    if workload not in ("LIGHT", "MEDIUM", "HEAVY"):
        raise SystemExit("unsupported workload")
    if delivery not in ("AT_LEAST_ONCE", "EXACTLY_ONCE"):
        raise SystemExit("unsupported delivery mode")

    repetitions = int(os.environ.get("SCENARIO_REPETITIONS", "1"))
    if repetitions < 1:
        raise SystemExit("SCENARIO_REPETITIONS must be positive")

    results = Path("benchmark-results")
    results.mkdir(parents=True, exist_ok=True)
    history_path = results / "stress-history.jsonl"
    run_tag = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    steps = list(range(step_messages, max_messages + 1, step_messages))
    if not steps or steps[-1] != max_messages:
        steps.append(max_messages)

    previous_lag = None
    rising_steps = 0
    environment = os.environ.copy()
    environment["SCENARIO_REPETITIONS"] = str(repetitions)

    for total_messages in steps:
        scenario = f"stress-{run_tag}-{total_messages}"
        command = [
            "bash", "scripts/run-scenario.sh", scenario, str(total_messages),
            str(concurrency), str(payload), workload, delivery,
        ]
        print(f"STRESS_STEP messages={total_messages} scenario={scenario}", flush=True)
        completed = subprocess.run(command, env=environment, check=False)
        try:
            reports = read_step_artifacts(results, scenario, repetitions)
        except Exception as error:
            record = {
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "messages": total_messages,
                "scenario": scenario,
                "status": "FAIL",
                "reason": str(error),
                "exitCode": completed.returncode,
            }
            with history_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record) + "\n")
            print(json.dumps(record), flush=True)
            return 1

        statuses = [item["status"] for item in reports]
        error_count = sum(item["errors"] for item in reports)
        memory_values = [item["memoryPercentMax"] for item in reports
                         if item["memoryPercentMax"] is not None]
        lag_values = [item["kafkaConsumerLagMax"] for item in reports
                      if item["kafkaConsumerLagMax"] is not None]
        memory_percent = max(memory_values) if memory_values else None
        current_lag = max(lag_values) if lag_values else None
        status = "PASS" if completed.returncode == 0 and all(s == "PASS" for s in statuses) else "FAIL"
        reason, rising_steps = evaluate_step(
            status, error_count, memory_percent, current_lag, previous_lag, rising_steps
        )
        record = {
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "messages": total_messages,
            "scenario": scenario,
            "status": status,
            "errors": error_count,
            "memoryPercentMax": memory_percent,
            "kafkaConsumerLagMax": current_lag,
            "consecutiveLagIncreases": rising_steps,
            "runs": reports,
            "stopReason": reason,
        }
        with history_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")
        print(json.dumps(record), flush=True)

        if reason:
            if reason == "validation-error":
                return 1
            print(f"STRESS_STOP reason={reason} messages={total_messages}", flush=True)
            return 0
        previous_lag = current_lag

    print(f"STRESS_COMPLETE messages={max_messages}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
