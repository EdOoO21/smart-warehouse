#!/usr/bin/env sh
set -eu

curl -fsS -X POST http://localhost:8082/events \
  -H 'Content-Type: application/json' \
  -d "$1"
