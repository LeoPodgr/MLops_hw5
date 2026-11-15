# Real-time Fraud Monitoring: Kafka + Prometheus + Grafana

Учебный проект мониторинга ML-сервиса детекции фрода. Включает непрерывный поток транзакций через Kafka, онлайн-скоринг, запись результатов в PostgreSQL, hot-reload модели без перезапуска скорера, Prometheus, Grafana, Node Exporter, четыре дашборда и два provisioned alert rules.

## 1. Архитектура

```text
transaction-simulator
        |
        v
 Kafka / topic: transactions
        |
        v
 fraud-detector -----> Prometheus :8000/metrics
        |     ^
        |     | читает /models/latest.txt при каждом сообщении
        |     +------ model_registry/*.json (bind mount)
        |
        +----------> Kafka / topic: scoring
                            |
                            v
                      scoring-writer -----> PostgreSQL / table: scores
                            |                         |
                            +--> Prometheus :8001    +--> Grafana PostgreSQL datasource

 Node Exporter :9100 ---------> Prometheus :9090 -----> Grafana :3000
```

Все контейнеры находятся в одной Docker Compose-сети `fraud-net`. Redpanda используется как Kafka-compatible broker, чтобы локальный учебный стек запускался без отдельного ZooKeeper. Снаружи broker доступен на `localhost:19092`, внутри сети — `redpanda:9092`.

### Сервисы Compose

| Сервис | Назначение | Порт на хосте |
|---|---|---:|
| `redpanda` | Kafka-compatible брокер | `19092` |
| `transaction-simulator` | Непрерывно публикует новые транзакции | — |
| `fraud-detector` | Читает `transactions`, считает score/fraud_flag, пишет в `scoring` | `8000` — `/metrics` |
| `scoring-writer` | Читает `scoring`, сохраняет результаты в PostgreSQL | `8001` — `/metrics` |
| `postgres` | Хранит таблицу `scores` | `5432` |
| `prometheus` | Снимает метрики раз в секунду | `9090` |
| `grafana` | Дашборды, PostgreSQL datasource и алерты | `3000` |
| `node-exporter` | CPU, RAM, сеть и файловые системы | `9100` |

## 2. Быстрый запуск

Требуются Docker Engine и Docker Compose plugin

```bash
cp .env.example .env
# При необходимости поменяйте пароли в .env

docker compose config -q
docker compose up -d --build
```

Проверить статус:

```bash
docker compose ps
```

Открыть интерфейсы:

| Интерфейс | URL | Логин / пароль |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | без авторизации |
| Метрики скорера | http://localhost:8000/metrics | без авторизации |
| Метрики writer | http://localhost:8001/metrics | без авторизации |
| Node Exporter | http://localhost:9100/metrics | без авторизации |
| PostgreSQL | `localhost:5432` | `fraud_user` / `fraud_pass_change_me` |

Логин и пароль Grafana и PostgreSQL задаются через `.env`.

```bash
docker compose up -d --no-build
```

## 3. Дашборды Grafana

Источник Prometheus и источник PostgreSQL создаются автоматически через provisioning. Дашборды и правила алертов также лежат в репозитории и загружаются при старте Grafana. Ручное создание datasource или импорт JSON не нужен.

После входа откройте папку **Fraud Monitoring**:

| Дашборд | Что смотреть | Прямая ссылка |
|---|---|---|
| **Fraud Scoring — поток и модель** | TPS скоринга, доля фрода, p50/p95/p99 времени обработки, распределение скоров по histogram buckets, ошибки входных данных, версии модели и отметки hot-reload | http://localhost:3000/d/fraud-scoring/fraud-scoring |
| **Scoring Writer — запись в PostgreSQL** | Скорость записи, доля fraud-записей, p95 записи, распределение сохранённых скоров; SQL-таблица с числом скоров и fraud share по `model_version` | http://localhost:3000/d/fraud-writer/fraud-writer |
| **System Metrics — Node Exporter** | CPU, память, RX/TX сети и занятость файловых систем | http://localhost:3000/d/fraud-system/fraud-system |
| **Fraud Business Analysis — фильтры и категории** | Плотность score и TPS с фильтрами `US state`/`Merchant`, fraud share по `cat_id` на последних 1000 строках и сумма помеченных транзакций по мерчанту | http://localhost:3000/d/fraud-business/fraud-business |

Дашборды хранятся в `grafana/dashboards/*.json`. Настройки источников данных и автозагрузки — в `grafana/provisioning/`.

### Фильтры бизнес-дашборда

Вверху `Fraud Business Analysis` находятся два multi-select фильтра:

- `US state` — поле `scores.us_state`;
- `Merchant` — поле `scores.merch`.

Оба фильтра применяются к SQL-панелям плотности score и TPS. Фильтры также применяются к барплоту fraud share по категориям и к панели суммы помеченных транзакций. Для быстрой проверки отправьте тестовую партию, как описано ниже: там будут `NY` и `demo_high_risk_merchant`.

### Список метрик

**Сервис скоринга (`fraud-detector:8000`)**

| Метрика | Тип | Назначение |
|---|---|---|
| `fraud_scorer_transactions_processed_total` | Counter | Число успешно обработанных и опубликованных транзакций |
| `fraud_scorer_processing_seconds` | Histogram | End-to-end время inference + публикации результата в Kafka |
| `fraud_scorer_score_distribution` | Histogram | Распределение score в диапазоне 0–1 |
| `fraud_scorer_fraud_transactions_total` | Counter | Число транзакций с `fraud_flag=1` |
| `fraud_scorer_fraud_share` | Gauge | Накопленная доля транзакций, отмеченных как фрод |
| `fraud_scorer_model_version_loaded_total{version}` | Counter | Число загрузок каждой версии модели |
| `fraud_scorer_model_version_loaded_timestamp_seconds{version}` | Gauge | Unix timestamp последней загрузки версии |
| `fraud_scorer_input_validation_failures_total{reason}` | Counter | **Дополнительная метрика проекта:** ошибки качества/контракта входящих сообщений |

Дополнительная метрика `fraud_scorer_input_validation_failures_total` нужна, чтобы отличать отсутствие фрода от проблем входного потока: например, если в сообщениях пропали `amount`, `us_state` или `transaction_id`, скорер не должен молча считать некорректные данные. Её график находится на дашборде **Fraud Scoring**, панель **Input validation failures (extra project metric)**.

**Сервис записи (`scoring-writer:8001`)**

| Метрика | Тип | Назначение |
|---|---|---|
| `fraud_writer_scores_written_total` | Counter | Число результатов, успешно закоммиченных в PostgreSQL |
| `fraud_writer_write_seconds` | Histogram | Время INSERT + COMMIT одной записи |
| `fraud_writer_fraud_scores_written_total` | Counter | Число записанных строк с `fraud_flag=1` |
| `fraud_writer_score_distribution` | Histogram | Распределение score, реально сохранённых в БД |

**Node Exporter**

Используются системные метрики `node_cpu_seconds_total`, `node_memory_MemAvailable_bytes`, `node_memory_MemTotal_bytes`, `node_network_receive_bytes_total`, `node_network_transmit_bytes_total`, `node_filesystem_avail_bytes` и `node_filesystem_size_bytes`.

## 4. Hot reload модели без перезапуска скорера

Скорер читает `model_registry/latest.txt` на каждом сообщении. Если указатель изменился, новый JSON загружается в память; контейнер не перезапускается. После загрузки увеличивается `fraud_scorer_model_version_loaded_total{version="..."}`. На дашборде **Fraud Scoring** Prometheus-аннотация `time() - fraud_scorer_model_version_loaded_timestamp_seconds < 30` показывает событие смены версии и подпись новой версии.

Для демонстрации публикации новой версии откройте терминал в корне проекта и выполните:

```bash
python tools/publish_model.py --bias 3.0
```

Скрипт создаст новый артефакт `model-<timestamp>.json`, затем атомарно заменит `latest.txt`. Он не требует Docker и сторонних библиотек Python. Пока симулятор продолжает публиковать транзакции, скорер подхватит новую версию на следующем сообщении.

Проверить обновление можно тремя способами:

1. В логах: `docker compose logs -f fraud-detector` — появится `Hot-loaded model version=...`.
2. В метриках: `curl -s http://localhost:8000/metrics | grep fraud_scorer_model_version_loaded_total` — появится новая label `version`.
3. В Grafana: на дашборде **Fraud Scoring** появится оранжевая отметка смены модели примерно на 30-секундном окне; наведите курсор, чтобы увидеть версию.

`--bias 3.0` намеренно повышает scores и используется ниже для проверки алерта на долю фрода. Для восстановления обычного поведения опубликуйте ещё одну версию:

```bash
python tools/publish_model.py --bias -4.0
```

Это демонстрационная логистическая модель на CPU. Качество модели не является целью задания. В существующем ML-проекте ваш trainer должен создавать JSON/артефакт с тем же контрактом (`version`, `bias`, `weights`, `fraud_threshold`) либо вы можете заменить `FraudModel.load()` и `predict_score()` на загрузку CatBoost/другой модели. Критически важно: сначала записать завершённый артефакт, затем атомарно обновить `latest.txt`.

## 5. Дополнения

### Grafana Alerting

Два правила хранятся в `grafana/provisioning/alerting/alert-rules.yml` и автоматически появляются в **Alerting → Alert rules**.

| Правило | Условие | Смысл |
|---|---|---|
| `Fraud share above 25 percent` | Доля fraud выше 25% по rate за 5 минут в течение 1 минуты | Резкий рост выявленного фрода / изменение поведения модели или потока |
| `Scoring stream stopped` | TPS скорера ниже 0.1 в течение 1 минуты, с окном rate 2 минуты | Поток перестал доходить до скорера |

Пороговые значения заданы прямо в YAML и видны в выражениях правила. Уведомления в Telegram или почту не настраиваются: статус срабатывания доступен в Grafana Alerting.

**Проверка алерта по fraud share:**

```bash
python tools/publish_model.py --bias 3.0
```

Подождите примерно 1–2 минуты, затем откройте **Alerting → Alert rules**. При большом числе high-score транзакций правило перейдёт в Pending/Firing. После проверки верните базовый режим командой с `--bias -4.0`; после затухания пятиминутного окна алерт вернётся в Normal.

**Проверка остановки потока:**

```bash
docker compose stop transaction-simulator
```

Через примерно 3 минуты правило `Scoring stream stopped` должно перейти в Pending/Firing. Возобновить поток:

```bash
docker compose start transaction-simulator
```

### PostgreSQL datasource и разрез по model version

Datasource `Fraud PostgreSQL` (UID `postgres`) автоматически настраивается в `grafana/provisioning/datasources/datasources.yml`. На дашборде **Scoring Writer** находится SQL-панель **Scores and fraud share by model version (PostgreSQL)**:

- `scores_written` — число результатов по версии модели;
- `fraud_share_pct` — процент строк с `fraud_flag=1` по версии;
- `last_score_at` — время последней записи этой версии.

### Бизнес-фильтры и дополнительные графики

На дашборде **Fraud Business Analysis** сделаны:

1. Score density: распределение последних 1 000 транзакций с фильтрами по `us_state` и `merch`.
2. Filtered TPS: TPS в разрезе времени с теми же двумя фильтрами.
3. Barplot fraud share по `cat_id`: `AVG(fraud_flag) * 100` на последних 1 000 транзакциях после фильтрации.
4. Дополнительная бизнес-панель: денежная сумма транзакций с `fraud_flag=1` за последние 24 часа по мерчанту. Помогает оценить не только число подозрительных операций, но и потенциальный денежный объём риска.

## 6. Проверка потока и диагностика

### Убедиться, что Prometheus видит все цели

Откройте http://localhost:9090/targets. Ожидаемый результат: `fraud-detector`, `scoring-writer`, `node-exporter` и `prometheus` имеют статус **UP**.

Через HTTP API:

```bash
curl -s http://localhost:9090/api/v1/targets | grep -E 'fraud-detector|scoring-writer|node-exporter'
```

### Убедиться, что метрики растут

```bash
curl -s http://localhost:8000/metrics | grep fraud_scorer_transactions_processed_total
curl -s http://localhost:8001/metrics | grep fraud_writer_scores_written_total
```

Повторите команды через несколько секунд: значения Counter должны увеличиться. Можно посмотреть PromQL напрямую:

```bash
curl -G -s http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(fraud_scorer_transactions_processed_total[1m])'
```

### Проверить данные в PostgreSQL

```bash
docker compose exec postgres psql -U fraud_user -d fraud_db -c \
  "SELECT transaction_id, score, fraud_flag, model_version, us_state, merch, cat_id, created_at FROM scores ORDER BY created_at DESC LIMIT 10;"
```

Проверить разрез по версии модели:

```bash
docker compose exec postgres psql -U fraud_user -d fraud_db -c \
  "SELECT model_version, COUNT(*) AS scores, ROUND(100 * AVG(fraud_flag::numeric), 2) AS fraud_share_pct FROM scores GROUP BY model_version ORDER BY MAX(created_at) DESC;"
```

### Быстро наполнить одну категорию для проверки фильтров и алерта

Скрипт отправит контролируемую партию прямо в Kafka, не меняя постоянный simulator:

```bash
docker compose exec fraud-detector python /app/tools/publish_demo_batch.py \
  --count 200 --amount 2500 --state NY \
  --merchant demo_high_risk_merchant --category electronics
```

После записи в БД выберите `NY` и `demo_high_risk_merchant` на дашборде **Fraud Business Analysis**. Тестовые записи попадут в density/TPS и категорийный barplot.

## 7. Структура репозитория

```text
.
├── docker-compose.yml
├── .env.example
├── README.md
├── Makefile
├── services/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── sql/init.sql
│   └── src/fraud_monitoring/
│       ├── common.py
│       ├── model.py
│       ├── simulator.py
│       ├── scorer.py
│       └── writer.py
├── model_registry/
│   ├── latest.txt
│   └── baseline-v1.json
├── prometheus/prometheus.yml
├── grafana/
│   ├── dashboards/                   # 4 provisioned dashboard JSON files
│   └── provisioning/
│       ├── datasources/datasources.yml
│       ├── dashboards/dashboards.yml
│       └── alerting/alert-rules.yml
├── tools/
│   ├── generate_dashboards.py
│   ├── publish_model.py
│   ├── publish_demo_batch.py
│   └── validate_project.py
└── tests/test_model.py
```

## 8. Остановка и очистка

Остановить контейнеры, сохранив данные и Grafana provisioning state:

```bash
docker compose down
```

Удалить также данные PostgreSQL, Redpanda, Prometheus и Grafana:

```bash
docker compose down -v
```
