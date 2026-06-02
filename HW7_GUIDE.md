# HW7 Smart Warehouse Guide

## Что было до доработки

Проект `smart-warehouse` уже содержал складскую event-driven систему:

- `wms-producer` принимает складские события через HTTP `POST /events`.
- `consumer` читает события из Kafka.
- Cassandra хранит остатки, заказы, историю событий и processed events.
- Kafka используется как брокер событий.
- Prometheus/Grafana частично уже были подключены.

## Что добавлено в HW7

### CI pipeline

Добавлен GitHub Actions workflow:

- файл: `.github/workflows/ci.yml`
- запускается на `push` и `pull_request`
- шаги:
  - `go build`
  - `go test ./...`
  - `docker compose build`
  - `docker compose up -d`
  - integration test
  - E2E test
  - k6 load test
  - проверка SLI через Prometheus
  - проверка alert rules
  - сохранение артефактов

Что сказать на защите:

> CI проверяет не только unit tests, но и реальный docker-compose стенд: Kafka, Cassandra, producer, consumer, Prometheus, Grafana и Alertmanager.

## Тесты

### Unit tests

Добавлен тест процессора складской логики:

- файл: `internal/application/warehouse/processor_test.go`
- проверяет сценарий:
  - товар принят на склад
  - заказ создан
  - заказ завершен
  - остаток стал `available=85`, `reserved=0`

### Integration test

Файл:

- `scripts/integration-test.sh`

Проверяет цепочку:

> HTTP producer -> Kafka -> consumer -> Cassandra

Сценарий:

- отправляется `PRODUCT_RECEIVED`
- затем Cassandra проверяется на появление остатка

### E2E test

Файл:

- `scripts/e2e-test.sh`

Проверяет полный бизнес-сценарий:

- `PRODUCT_RECEIVED`: пришло 100 единиц товара
- `ORDER_CREATED`: заказ резервирует 15
- `ORDER_COMPLETED`: заказ завершен
- Cassandra проверяется:
  - заказ `COMPLETED`
  - `available_quantity = 85`
  - `reserved_quantity = 0`

Что сказать:

> E2E идет через публичный API producer и проверяет конечное состояние в Cassandra, то есть не мокает сервисы.

### Чем integration test отличается от E2E

Коротко:

- integration test проверяет, что сервисы и инфраструктура связаны между собой;
- E2E test проверяет полный бизнес-сценарий из предметной области.

Конкретно в этом проекте:

| Тест | Файл | Что отправляет | Что проверяет | Смысл |
| --- | --- | --- | --- | --- |
| Integration | `scripts/integration-test.sh` | 1 событие `PRODUCT_RECEIVED` | остаток появился в Cassandra | producer, Kafka, consumer и Cassandra работают вместе |
| E2E | `scripts/e2e-test.sh` | 3 события: `PRODUCT_RECEIVED`, `ORDER_CREATED`, `ORDER_COMPLETED` | заказ `COMPLETED`, остаток `85`, резерв `0` | полный сценарий склада работает правильно |

Integration test:

```text
POST /events
-> wms-producer
-> Kafka
-> consumer
-> Cassandra
```

Он отвечает на вопрос:

> Проходит ли одно событие через всю техническую цепочку?

E2E test:

```text
товар пришел
-> заказ создан
-> товар зарезервирован
-> заказ завершен
-> проверка итогового состояния склада
```

Он отвечает на вопрос:

> Работает ли реальный складской сценарий от начала до конца?

Важно: оба теста идут через реальные сервисы, без моков. Разница не в инструментах, а в цели проверки: integration проверяет связку компонентов, E2E проверяет бизнес-флоу.

## Метрики

Добавлены HTTP-метрики для обоих сервисов:

- `http_requests_total{method,endpoint,status}`
- `http_request_errors_total{method,endpoint,error_type}`
- `http_request_duration_seconds{method,endpoint}`

Файлы:

- `internal/infrastructure/metrics/metrics.go`
- `internal/infrastructure/httpserver/server.go`

Что сказать:

> Метрики собираются middleware вокруг HTTP handler. Он фиксирует endpoint, method, status и duration.

Доменные метрики уже используются:

- `events_processed_total`
- `events_failed_total`
- `event_processing_duration_seconds`
- `consumer_lag`
- `cassandra_write_errors_total`

## Prometheus

Файл:

- `deployments/prometheus.yml`

Prometheus собирает:

- `consumer:8080`
- `wms-producer:8082`
- `kafka-exporter:9308`
- сам Prometheus

Что показать:

- `http://localhost:9090/targets`
- все targets должны быть `UP`

## Grafana

Добавлены dashboards:

- `deployments/grafana/dashboards/smart-warehouse.json`
- `deployments/grafana/dashboards/infrastructure.json`

Service dashboard показывает:

- p50/p95/p99 latency
- throughput
- HTTP errors
- domain events/sec
- event processing p95

Infrastructure dashboard показывает:

- Kafka brokers
- Kafka consumer lag
- Kafka partitions
- Cassandra write errors
- Prometheus scrape health

Что сказать:

> Dashboards provisioned automatically, руками в Grafana ничего создавать не нужно.

## Load testing

Файл:

- `load/k6.js`

Что делает:

- запускает поток HTTP-запросов в `wms-producer`;
- каждый запрос отправляет `POST /events`;
- producer публикует событие в Kafka;
- k6 проверяет, что API отвечает `202 Accepted`.

Параметры:

- 10 virtual users
- 30 секунд
- thresholds:
  - error rate `< 1%`
  - p95 latency `< 500ms`

Где запускается в CI:

- `.github/workflows/ci.yml`
- step `Load test`

Команда в CI:

```sh
docker run --rm --network host -e BASE_URL=http://127.0.0.1:8082 -v "$PWD:/workspace" -w /workspace grafana/k6:0.55.0 run --summary-export artifacts/k6-summary.json load/k6.js
```

Что важно:

- если threshold нарушен, k6 возвращает exit code `1`;
- GitHub Actions сразу помечает pipeline failed;
- результат сохраняется в `artifacts/k6-summary.json`;
- потом весь каталог `artifacts/` загружается в GitHub Actions artifacts.

Health check под нагрузкой:

- k6 сам проверяет доступность producer через успешные `202 Accepted`;
- после нагрузки CI запускает `scripts/check-metrics.py`, который проверяет Prometheus SLI;
- если нужен прямой health check для защиты, можно показать:

```sh
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8082/health
```

Важная доработка:

- в Kafka producer добавлены `BatchSize: 1` и `BatchTimeout: 10ms`
- иначе HTTP `/events` отвечал около 1 секунды из-за batching

Что сказать:

> Нагрузочный тест запускается в CI через k6: 10 VU, 30 секунд, thresholds на error rate и p95 latency. Если сервис начинает ошибаться или тормозить, k6 возвращает ошибку и pipeline падает. Результаты сохраняются как artifact.

## Alertmanager и alerts

Файлы:

- `deployments/alerts.yml`
- `deployments/alertmanager.yml`

Алерты:

- service target down
- HTTP error rate > 5%
- HTTP p95 latency > 1s
- consumer lag > 5

Что показать:

- `http://localhost:9090/alerts`
- `http://localhost:9093`

## SLI/SLO

Файл:

- `scripts/check-metrics.py`

SLI считаются из Prometheus:

- API availability
- HTTP p95 latency
- Event processing p95
- HTTP error rate

Пороги:

- availability >= 99.5%
- HTTP p95 < 500ms
- event processing p95 < 1s
- error rate < 1%

Что сказать:

> CI после нагрузки делает PromQL-запросы в Prometheus и падает, если SLI нарушены.

## Что показывать на защите

1. Запустить:

```sh
docker compose up -d --build
```

2. Показать контейнеры:

```sh
docker compose ps
```

3. Запустить тесты:

```sh
go test ./...
./scripts/integration-test.sh
./scripts/e2e-test.sh
```

4. Запустить load test:

```sh
docker run --rm -e BASE_URL=http://host.docker.internal:8082 -v "$PWD:/workspace" -w /workspace grafana/k6:0.55.0 run load/k6.js
```

5. Проверить SLI:

```sh
./scripts/check-metrics.py
```

6. Проверить alerts:

```sh
./scripts/check-alerts.sh
```

7. Открыть:

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Alertmanager: `http://localhost:9093`

## Частые вопросы

### Зачем Kafka?

Kafka отделяет прием событий от обработки. Producer быстро принимает событие, consumer независимо применяет складскую логику.

### Зачем Cassandra?

Cassandra хранит состояние склада и хорошо подходит для write-heavy event-driven сценария.

### Чем integration отличается от E2E?

Integration проверяет взаимодействие сервисов и инфраструктуры. E2E проверяет полный бизнес-сценарий от API до конечного состояния в БД.

### Почему CI может долго идти?

Потому что поднимаются тяжелые компоненты: Kafka, Schema Registry и 3 Cassandra ноды.

### Почему pipeline падает при ошибках?

Все шаги возвращают non-zero exit code: Go tests, shell tests, k6 thresholds, SLI checker.

## Короткий рассказ на 1 минуту

Я доработал `smart-warehouse` до полноценного CI/CD и observability стенда. Система состоит из producer и consumer: producer принимает складские события по HTTP, кладет их в Kafka, consumer читает Kafka и сохраняет итоговое состояние в Cassandra. В CI поднимается полный docker-compose стенд, запускаются unit, integration, E2E, k6 load test, затем Prometheus проверяется на SLI. Для наблюдаемости добавлены HTTP-метрики, Prometheus scrape config, Grafana dashboards, Alertmanager и alert rules. Все конфиги лежат в репозитории как код.

## Команды при уже поднятом compose

- Проверить контейнеры:

```sh
docker compose ps
```

- Unit tests:

```sh
go test ./...
```

- Integration test:

```sh
./scripts/integration-test.sh
```

- E2E test:

```sh
./scripts/e2e-test.sh
```

- Load test:

```sh
docker run --rm -e BASE_URL=http://host.docker.internal:8082 -v "$PWD:/workspace" -w /workspace grafana/k6:0.55.0 run --summary-export artifacts/k6-summary.json load/k6.js
```

- SLI / metrics check:

```sh
./scripts/check-metrics.py
```

- Alert rules check:

```sh
./scripts/check-alerts.sh
```

- Health checks:

```sh
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8082/health
```

- Посмотреть метрики:

```sh
curl -fsS http://localhost:8080/metrics
curl -fsS http://localhost:8082/metrics
```

- Остановить все:

```sh
docker compose down -v
```
