CREATE KEYSPACE IF NOT EXISTS warehouse
WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}
AND durable_writes = true;

USE warehouse;

CREATE TABLE IF NOT EXISTS inventory_by_product_zone (
    product_id text,
    zone_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    last_event_id text,
    last_event_timestamp timestamp,
    PRIMARY KEY ((product_id), zone_id)
);

CREATE TABLE IF NOT EXISTS inventory_by_product (
    product_id text PRIMARY KEY,
    total_available_quantity int,
    total_reserved_quantity int,
    last_event_id text,
    last_event_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS inventory_by_zone (
    zone_id text,
    product_id text,
    available_quantity int,
    reserved_quantity int,
    supplier_id text,
    last_event_id text,
    last_event_timestamp timestamp,
    PRIMARY KEY ((zone_id), product_id)
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id text PRIMARY KEY,
    event_type text,
    processed_at timestamp
);

CREATE TABLE IF NOT EXISTS entity_versions (
    entity_key text PRIMARY KEY,
    last_event_id text,
    last_event_timestamp timestamp
);

CREATE TABLE IF NOT EXISTS event_history (
    bucket text,
    occurred_at timestamp,
    event_id text,
    event_type text,
    event_json text,
    PRIMARY KEY ((bucket), occurred_at, event_id)
) WITH CLUSTERING ORDER BY (occurred_at DESC);

CREATE TABLE IF NOT EXISTS orders_by_id (
    order_id text PRIMARY KEY,
    status text,
    items_json text,
    updated_at timestamp
);
