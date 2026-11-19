#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
N="${1:-10000}"
RUN_ID="${2:-manual-$(date -u +%Y%m%dT%H%M%SZ)-$}"
TIMEOUT_SECONDS="${3:-600}"
docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
  --spring.main.web-application-type=none --lab.consumer.enabled=false --generate \
  --messages="$N" --run-id="$RUN_ID" --timeout-seconds="$TIMEOUT_SECONDS"
