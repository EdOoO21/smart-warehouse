#!/usr/bin/env sh
set -eu

suffix="$(date +%s)-$$"
product="E2E-SKU-$suffix"
order="E2E-ORDER-$suffix"
base_time="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

./scripts/wait-http.sh http://localhost:8080/health 90
./scripts/wait-http.sh http://localhost:8082/health 90

./scripts/send-event.sh '{"event_id":"e2e-received-'"$suffix"'","event_type":"PRODUCT_RECEIVED","occurred_at":"'"$base_time"'","product_id":"'"$product"'","zone_id":"ZONE-A","quantity":100,"schema_version":1}'
sleep 2
./scripts/send-event.sh '{"event_id":"e2e-order-created-'"$suffix"'","event_type":"ORDER_CREATED","occurred_at":"'"$(date -u -v+1S +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")"'","order_id":"'"$order"'","items":[{"product_id":"'"$product"'","zone_id":"ZONE-A","quantity":15}],"schema_version":1}'
sleep 2
./scripts/send-event.sh '{"event_id":"e2e-order-completed-'"$suffix"'","event_type":"ORDER_COMPLETED","occurred_at":"'"$(date -u -v+2S +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || date -u +"%Y-%m-%dT%H:%M:%SZ")"'","order_id":"'"$order"'","schema_version":1}'

i=1
while [ "$i" -le 60 ]; do
  order_output="$(docker compose exec -T cassandra-1 cqlsh -e "SELECT status FROM warehouse.orders_by_id WHERE order_id = '$order';" 2>/dev/null || true)"
  inventory_output="$(docker compose exec -T cassandra-1 cqlsh -e "SELECT available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id = '$product' AND zone_id = 'ZONE-A';" 2>/dev/null || true)"
  if printf '%s\n' "$order_output" | grep -q "COMPLETED" && printf '%s\n' "$inventory_output" | grep -q "85.*0"; then
    echo "e2e test passed"
    exit 0
  fi
  sleep 2
  i=$((i + 1))
done

echo "e2e test failed: order or inventory state is invalid" >&2
exit 1
