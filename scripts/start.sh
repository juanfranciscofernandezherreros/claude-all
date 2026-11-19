#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p benchmark-results
chmod 777 benchmark-results
docker compose up -d --build
docker compose ps
