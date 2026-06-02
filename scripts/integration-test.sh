#!/usr/bin/env sh
set -eu

suffix="$(date +%s)-$$"
product="INT-SKU-$suffix"
event_id="int-received-$suffix"
occurred_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

./scripts/wait-http.sh http://localhost:8080/health 90
./scripts/wait-http.sh http://localhost:8082/health 90

./scripts/send-event.sh '{"event_id":"'"$event_id"'","event_type":"PRODUCT_RECEIVED","occurred_at":"'"$occurred_at"'","product_id":"'"$product"'","zone_id":"ZONE-A","quantity":25,"schema_version":1}'

i=1
while [ "$i" -le 60 ]; do
  output="$(docker compose exec -T cassandra-1 cqlsh -e "SELECT available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id = '$product' AND zone_id = 'ZONE-A';" 2>/dev/null || true)"
  if printf '%s\n' "$output" | grep -q "25"; then
    echo "integration test passed"
    exit 0
  fi
  sleep 2
  i=$((i + 1))
done

echo "integration test failed: inventory was not written to Cassandra" >&2
exit 1
