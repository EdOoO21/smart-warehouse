#!/usr/bin/env sh
set -eu

now() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

./scripts/send-event.sh '{"event_id":"evt-001","event_type":"PRODUCT_RECEIVED","occurred_at":"'"$(now)"'","product_id":"SKU-001","zone_id":"ZONE-A","quantity":100,"schema_version":1}'
./scripts/send-event.sh '{"event_id":"evt-002","event_type":"PRODUCT_RESERVED","occurred_at":"'"$(now)"'","product_id":"SKU-001","zone_id":"ZONE-A","quantity":30}'
./scripts/send-event.sh '{"event_id":"evt-003","event_type":"PRODUCT_MOVED","occurred_at":"'"$(now)"'","product_id":"SKU-001","from_zone_id":"ZONE-A","to_zone_id":"ZONE-B","quantity":20}'
./scripts/send-event.sh '{"event_id":"evt-004","event_type":"PRODUCT_SHIPPED","occurred_at":"'"$(now)"'","product_id":"SKU-001","zone_id":"ZONE-A","quantity":10}'
./scripts/send-event.sh '{"event_id":"evt-005","event_type":"ORDER_CREATED","occurred_at":"'"$(now)"'","order_id":"ORD-001","items":[{"product_id":"SKU-001","zone_id":"ZONE-A","quantity":15}]}'
./scripts/send-event.sh '{"event_id":"evt-006","event_type":"ORDER_COMPLETED","occurred_at":"'"$(now)"'","order_id":"ORD-001"}'
