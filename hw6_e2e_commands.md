# HW6 E2E команды без скриптов

## 0. Поднять систему

### 0.1 Поднять все сервисы

```bash
docker compose up --build -d
```

### 0.2 Проверить контейнеры

```bash
docker compose ps
```

### 0.3 Проверить health consumer

```bash
curl -i http://localhost:8080/health
```

### 0.4 Проверить Cassandra cluster

```bash
docker exec cassandra-1 nodetool status
```

### 0.5 Проверить версии схем в Schema Registry

```bash
curl -s http://localhost:8081/subjects/warehouse-events-value/versions
```

## 1. Базовый цикл склада

### 1.1 Отправить PRODUCT_RECEIVED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T10:00:00Z","product_id":"SKU-E2E-001","zone_id":"ZONE-A","quantity":100,"schema_version":2}'
```

### 1.2 Проверить остаток в конкретной зоне

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001' AND zone_id='ZONE-A';"
```

### 1.3 Проверить агрегат по товару

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT * FROM warehouse.inventory_by_product WHERE product_id='SKU-E2E-001';"
```

### 1.4 Отправить PRODUCT_RESERVED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-002","event_type":"PRODUCT_RESERVED","occurred_at":"2026-05-21T10:01:00Z","product_id":"SKU-E2E-001","zone_id":"ZONE-A","quantity":30,"schema_version":2}'
```

### 1.5 Проверить available=70, reserved=30

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001' AND zone_id='ZONE-A';"
```

### 1.6 Отправить PRODUCT_MOVED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-003","event_type":"PRODUCT_MOVED","occurred_at":"2026-05-21T10:02:00Z","product_id":"SKU-E2E-001","from_zone_id":"ZONE-A","to_zone_id":"ZONE-B","quantity":20,"schema_version":2}'
```

### 1.7 Проверить остатки по зонам товара

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001';"
```

### 1.8 Отправить PRODUCT_SHIPPED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-004","event_type":"PRODUCT_SHIPPED","occurred_at":"2026-05-21T10:03:00Z","product_id":"SKU-E2E-001","zone_id":"ZONE-A","quantity":10,"schema_version":2}'
```

### 1.9 Проверить остаток после отгрузки

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001' AND zone_id='ZONE-A';"
```

### 1.10 Отправить ORDER_CREATED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-005","event_type":"ORDER_CREATED","occurred_at":"2026-05-21T10:04:00Z","order_id":"ORDER-E2E-001","items":[{"product_id":"SKU-E2E-001","zone_id":"ZONE-A","quantity":15}],"schema_version":2}'
```

### 1.11 Проверить резерв и заказ CREATED

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT * FROM warehouse.orders_by_id WHERE order_id='ORDER-E2E-001'; SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001' AND zone_id='ZONE-A';"
```

### 1.12 Отправить ORDER_COMPLETED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-basic-006","event_type":"ORDER_COMPLETED","occurred_at":"2026-05-21T10:05:00Z","order_id":"ORDER-E2E-001","schema_version":2}'
```

### 1.13 Проверить reserved уменьшился и заказ COMPLETED

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT * FROM warehouse.orders_by_id WHERE order_id='ORDER-E2E-001'; SELECT product_id, zone_id, available_quantity, reserved_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-001' AND zone_id='ZONE-A';"
```

## 2. Идемпотентность

### 2.1 Отправить первое событие

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-idem-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T11:00:00Z","product_id":"SKU-E2E-002","zone_id":"ZONE-A","quantity":50,"schema_version":2}'
```

### 2.2 Проверить available=50

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-002';"
```

### 2.3 Повторно отправить тот же event_id

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-idem-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T11:00:00Z","product_id":"SKU-E2E-002","zone_id":"ZONE-A","quantity":50,"schema_version":2}'
```

### 2.4 Проверить, что available всё ещё 50

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-002';"
```

### 2.5 Проверить лог duplicate

```bash
docker compose logs --tail=80 consumer | grep "duplicate event"
```

## 3. Консистентность денормализованных таблиц

### 3.1 Отправить PRODUCT_RECEIVED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-consistency-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T12:00:00Z","product_id":"SKU-E2E-003","zone_id":"ZONE-A","quantity":100,"schema_version":2}'
```

### 3.2 Проверить inventory_by_product_zone

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-003' AND zone_id='ZONE-A';"
```

### 3.3 Проверить inventory_by_product

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, total_available_quantity FROM warehouse.inventory_by_product WHERE product_id='SKU-E2E-003';"
```

### 3.4 Проверить inventory_by_zone

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT zone_id, product_id, available_quantity FROM warehouse.inventory_by_zone WHERE zone_id='ZONE-A';"
```

## 4. Out-of-order события

### 4.1 Отправить PRODUCT_RECEIVED на 13:00

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-ooo-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T13:00:00Z","product_id":"SKU-E2E-004","zone_id":"ZONE-A","quantity":100,"schema_version":2}'
```

### 4.2 Отправить PRODUCT_SHIPPED на 13:05

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-ooo-002","event_type":"PRODUCT_SHIPPED","occurred_at":"2026-05-21T13:05:00Z","product_id":"SKU-E2E-004","zone_id":"ZONE-A","quantity":20,"schema_version":2}'
```

### 4.3 Проверить available=80

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-004' AND zone_id='ZONE-A';"
```

### 4.4 Отправить старое событие на 13:02

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-ooo-003","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T13:02:00Z","product_id":"SKU-E2E-004","zone_id":"ZONE-A","quantity":50,"schema_version":2}'
```

### 4.5 Проверить, что available всё ещё 80

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, last_event_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-004' AND zone_id='ZONE-A';"
```

### 4.6 Проверить лог stale

```bash
docker compose logs --tail=100 consumer | grep "stale event"
```

## 5. Dead Letter Queue

### 5.1 Отправить невалидное событие

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-dlq-001","event_type":"PRODUCT_SHIPPED","occurred_at":"2026-05-21T14:00:00Z","product_id":"SKU-E2E-005","zone_id":"ZONE-A","quantity":-5,"schema_version":2}'
```

### 5.2 Проверить лог отправки в DLQ

```bash
docker compose logs --tail=100 consumer | grep "sending to dlq"
```

### 5.3 Прочитать сообщение из DLQ topic

```bash
docker exec -it smart-warehouse-kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic warehouse-events-dlq --from-beginning --max-messages 1
```

### 5.4 Отправить валидное событие после ошибки

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-dlq-002","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T14:01:00Z","product_id":"SKU-E2E-005","zone_id":"ZONE-A","quantity":20,"schema_version":2}'
```

### 5.5 Проверить, что consumer продолжил работу

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-005';"
```

## 6. Cassandra cluster и отказоустойчивость

### 6.1 Проверить 3 Cassandra-ноды

```bash
docker exec cassandra-1 nodetool status
```

### 6.2 Отправить начальный PRODUCT_RECEIVED

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-cluster-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T15:00:00Z","product_id":"SKU-E2E-006","zone_id":"ZONE-A","quantity":200,"schema_version":2}'
```

### 6.3 Проверить чтение с QUORUM

```bash
docker exec -it cassandra-1 cqlsh -e "CONSISTENCY QUORUM; SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-006' AND zone_id='ZONE-A';"
```

### 6.4 Остановить одну ноду

```bash
docker stop cassandra-2
```

### 6.5 Отправить событие при одной остановленной ноде

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-cluster-002","event_type":"PRODUCT_SHIPPED","occurred_at":"2026-05-21T15:01:00Z","product_id":"SKU-E2E-006","zone_id":"ZONE-A","quantity":50,"schema_version":2}'
```

### 6.6 Проверить, что QUORUM всё ещё работает

```bash
docker exec -it cassandra-1 cqlsh -e "CONSISTENCY QUORUM; SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-006' AND zone_id='ZONE-A';"
```

### 6.7 Показать, что ALL не работает без одной ноды

```bash
docker exec -it cassandra-1 cqlsh -e "CONSISTENCY ALL; SELECT product_id, zone_id, available_quantity FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-006' AND zone_id='ZONE-A';"
```

### 6.8 Вернуть ноду обратно

```bash
docker start cassandra-2
```

### 6.9 Проверить, что нода вернулась

```bash
docker exec cassandra-1 nodetool status
```

## 7. Monitoring и consumer lag

### 7.1 Проверить health

```bash
curl -i http://localhost:8080/health
```

### 7.2 Проверить metrics

```bash
curl -s http://localhost:8080/metrics
```

### 7.3 Открыть Prometheus

```bash
open http://localhost:9090
```

### 7.4 Открыть Grafana

```bash
open http://localhost:3000
```

Логин/пароль: `admin / admin`.

### 7.5 Остановить consumer для проверки lag

```bash
docker stop smart-warehouse-consumer
```

### 7.6 Отправить событие, пока consumer остановлен

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-lag-001","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T16:00:00Z","product_id":"SKU-E2E-LAG","zone_id":"ZONE-A","quantity":10,"schema_version":2}'
```

### 7.7 Отправить ещё одно событие для роста lag

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-lag-002","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T16:01:00Z","product_id":"SKU-E2E-LAG","zone_id":"ZONE-B","quantity":20,"schema_version":2}'
```

### 7.8 Запустить consumer обратно

```bash
docker start smart-warehouse-consumer
```

## 8. Schema Evolution

### 8.1 Проверить версии схем

```bash
curl -s http://localhost:8081/subjects/warehouse-events-value/versions
```

### 8.2 Отправить V1 без supplier_id

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-schema-v1","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T17:00:00Z","product_id":"SKU-E2E-V1","zone_id":"ZONE-A","quantity":10,"schema_version":1}'
```

### 8.3 Проверить V1: supplier_id=null

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-V1';"
```

### 8.4 Отправить V2 с supplier_id

```bash
curl -fsS -X POST http://localhost:8082/events -H 'Content-Type: application/json' -d '{"event_id":"e2e-schema-v2","event_type":"PRODUCT_RECEIVED","occurred_at":"2026-05-21T17:01:00Z","product_id":"SKU-E2E-V2","zone_id":"ZONE-A","quantity":10,"supplier_id":"SUP-001","schema_version":2}'
```

### 8.5 Проверить V2: supplier_id=SUP-001

```bash
docker exec -it cassandra-1 cqlsh -e "SELECT product_id, zone_id, available_quantity, supplier_id FROM warehouse.inventory_by_product_zone WHERE product_id='SKU-E2E-V2';"
```
