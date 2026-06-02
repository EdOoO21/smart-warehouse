#!/usr/bin/env sh
set -eu

./scripts/wait-http.sh http://127.0.0.1:9090/-/ready 60
curl -fsS http://127.0.0.1:9090/api/v1/rules | grep -q "WarehouseHighHTTPErrorRate"
curl -fsS http://127.0.0.1:9093/-/ready >/dev/null
echo "alert rules are loaded and Alertmanager is ready"
