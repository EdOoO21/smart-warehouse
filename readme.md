# Smart Warehouse HW7

Система состоит из двух сервисов и инфраструктуры:

- `wms-producer` принимает события склада по HTTP `POST /events` и публикует их в Kafka.
- `consumer` читает события из Kafka, применяет складскую логику и сохраняет состояние в Cassandra.
- Cassandra хранит остатки, заказы, историю событий и idempotency keys.
- Prometheus, Grafana, Alertmanager и Kafka exporter поднимаются вместе с системой.

## Запуск

```sh
docker compose up -d --build
```

UI:

- Producer: `http://localhost:8082/health`, `http://localhost:8082/metrics`
- Consumer: `http://localhost:8080/health`, `http://localhost:8080/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)
- Alertmanager: `http://localhost:9093`

## CI/CD

Pipeline хранится в `.github/workflows/ci.yml` и запускается на `push` и `pull_request`.

Шаги:

1. build: `go build ./cmd/smart-warehouse`
2. unit tests: `go test ./...`
3. compose build: `docker compose build`
4. test environment: `docker compose up -d`
5. integration test: `./scripts/integration-test.sh`
6. E2E test: `./scripts/e2e-test.sh`
7. load test: `k6`, 10 VU, 30 секунд
8. Prometheus SLI validation: `./scripts/check-metrics.py`
9. alert rules validation: `./scripts/check-alerts.sh`

Артефакты CI: compose logs, k6 summary, SLI report.

## Тесты

```sh
go test ./...
./scripts/integration-test.sh
./scripts/e2e-test.sh
```

Integration test проверяет связку `producer -> Kafka -> consumer -> Cassandra`.

E2E сценарий:

1. `PRODUCT_RECEIVED` создает остаток 100.
2. `ORDER_CREATED` резервирует 15 единиц.
3. `ORDER_COMPLETED` завершает заказ.
4. Cassandra проверяется на `status = COMPLETED`, `available_quantity = 85`, `reserved_quantity = 0`.

## Метрики

Оба сервиса экспортируют `/metrics` в Prometheus-формате.

Базовые HTTP-метрики:

- `http_requests_total{method,endpoint,status}`
- `http_request_errors_total{method,endpoint,error_type}`
- `http_request_duration_seconds{method,endpoint}`

Доменные метрики:

- `events_processed_total{event_type}`
- `events_failed_total{error_code}`
- `event_processing_duration_seconds`
- `consumer_lag{partition}`
- `cassandra_write_errors_total`

## Grafana

Provisioning включен автоматически.

Dashboards:

- `deployments/grafana/dashboards/smart-warehouse.json`: latency p50/p95/p99, errors, throughput, domain processing.
- `deployments/grafana/dashboards/infrastructure.json`: Kafka brokers, consumer lag, partitions, Cassandra write errors, scrape health.

## Load test

```sh
docker run --rm -e BASE_URL=http://host.docker.internal:8082 -v "$PWD:/workspace" -w /workspace grafana/k6:0.55.0 run load/k6.js
```

Порог отказа k6:

- `http_req_failed < 1%`
- `p95(http_req_duration) < 500ms`

## Alerts

Alert rules хранятся в `deployments/alerts.yml`.

Покрыты ситуации:

- service target down
- HTTP error rate > 5%
- HTTP p95 latency > 1s
- consumer lag > 5

Alertmanager поднимается в `docker-compose.yml`.

## SLI/SLO

SLI считаются из Prometheus в `scripts/check-metrics.py`.

| SLI | PromQL | SLO | Порог отказа |
| --- | --- | --- | --- |
| API availability | `sum(rate(http_requests_total{status=~"2..|3.."}[1m])) / clamp_min(sum(rate(http_requests_total[1m])), 0.000001)` | `>= 99.5%` | `< 99.5%` |
| HTTP p95 latency | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket[1m])))` | `< 500ms` | `> 500ms` |
| Event processing p95 | `histogram_quantile(0.95, sum by (le) (rate(event_processing_duration_seconds_bucket[1m])))` | `< 1s` | `> 1s` |
| HTTP error rate | `sum(rate(http_request_errors_total[1m])) / clamp_min(sum(rate(http_requests_total[1m])), 0.000001)` | `< 1%` | `> 1%` |

Пороги выбраны для локального test environment: сценарий короткий, без внешних сетевых вызовов, поэтому p95 выше 500ms для HTTP и выше 1s для обработки события считается деградацией.
