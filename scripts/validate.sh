#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -lt 2 ]; then echo "Usage: $0 <runId> <expectedMessages> [timeoutSeconds]" >&2; exit 2; fi
docker compose run --rm --no-deps -e LAB_CONSUMER_ENABLED=false app \
  --spring.main.web-application-type=none --lab.consumer.enabled=false --validate \
  --run-id="$1" --messages="$2" --timeout-seconds="${3:-600}"
