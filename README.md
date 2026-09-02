# Real-Time Commerce CDC and Analytics Platform

A Data Engineering portfolio project that captures PostgreSQL commerce changes in real time, publishes them through Debezium and Apache Kafka, and prepares them for reliable streaming processing and analytics.

## Current status

Milestones M00 through M02 are complete:

- project scope and evidence contract;
- installable Python package;
- PostgreSQL operational source;
- six-table relational commerce model;
- deterministic synthetic-data generation;
- atomic loading and overwrite protection;
- source profiling and integrity validation;
- PostgreSQL logical replication;
- Debezium Change Data Capture;
- Apache Kafka event transport;
- six table-specific CDC topics;
- source-to-topic completeness measurement;
- live create, update and delete verification;
- automated unit, PostgreSQL and CDC integration tests.

Spark Structured Streaming and Bronze storage are planned for M03.

## Why this is a Data Engineering project

This project demonstrates several responsibilities commonly expected in Data Engineer roles:

- designing relational source systems;
- generating and validating reproducible datasets;
- configuring PostgreSQL logical replication;
- capturing database changes without repeatedly reading entire tables;
- transporting events through Kafka;
- validating source-to-stream completeness;
- preserving historical change events;
- testing database, connector and streaming infrastructure;
- measuring data quality and pipeline behavior;
- preparing streaming data for lakehouse and warehouse processing.

## Verified PostgreSQL source results

| Result | Verified value |
|---|---:|
| Customers | 1,000 |
| Products | 250 |
| Orders | 5,000 |
| Order items | 12,500 |
| Payments | 5,000 |
| Shipments | 2,499 |
| Total source rows | 26,249 |
| Total order value | $5,053,882.86 |
| Average order value | $1,010.78 |
| Referential-integrity failures | 0 |
| Order-total mismatches | 0 |
| Payment-total mismatches | 0 |
| PostgreSQL WAL level | logical |
| Baseline load duration | 1.451 seconds |

The source dataset is reproducible with seed `20260902` and SHA-256 fingerprint:

```text
9616a65028c5f571e5cc4d4c5ced080977fbea1bf3f1117d57b80b67b49d83e5
```

## Verified CDC and Kafka results

| Result | Verified value |
|---|---:|
| Debezium connector | RUNNING |
| Connector tasks | 1 RUNNING |
| CDC business topics | 6 |
| Initial source rows | 26,249 |
| Initial Kafka snapshot events | 26,249 |
| Initial snapshot match | Exact |
| Minimum source-to-topic completeness | 100% |
| Live CDC operations | create, update, delete |
| Live events captured in order | `c`, `u`, `d` |
| Temporary probe rows remaining | 0 |
| Measured live verification duration | 930.544 ms |
| Final source rows | 26,249 |
| Final Kafka events | 26,258 |
| Historical probe events retained | 9 |
| Complete automated suite | 17/17 passed |

Kafka contains nine more events than the current PostgreSQL row count because three verification runs each generated a create, update and delete event. The temporary customer was deleted from PostgreSQL, but Kafka correctly retained its historical changes.

## Architecture

```mermaid
flowchart LR
    A[Commerce applications] --> B[(PostgreSQL)]
    B --> C[Logical WAL]
    C --> D[Debezium Connect]
    D --> E[Apache Kafka topics]
    E --> F[Spark Structured Streaming]
    F --> G[Bronze storage]
    G --> H[Warehouse and dbt]
    H --> I[Analytics marts]
```

The implemented M02 flow currently ends at the Kafka topics. Spark, Bronze storage, warehouse models and analytics marts are subsequent milestones.

## Source data model

| Table | Purpose | Rows |
|---|---|---:|
| `customers` | Customer identity and status | 1,000 |
| `products` | Product catalog and pricing | 250 |
| `orders` | Order headers and financial totals | 5,000 |
| `order_items` | Products purchased per order | 12,500 |
| `payments` | Payment lifecycle | 5,000 |
| `shipments` | Shipment lifecycle | 2,499 |

## CDC topic mapping

| PostgreSQL table | Kafka topic |
|---|---|
| `commerce.customers` | `commerce.commerce.customers` |
| `commerce.products` | `commerce.commerce.products` |
| `commerce.orders` | `commerce.commerce.orders` |
| `commerce.order_items` | `commerce.commerce.order_items` |
| `commerce.payments` | `commerce.commerce.payments` |
| `commerce.shipments` | `commerce.commerce.shipments` |

## Fresh local startup

Create and activate the virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Start PostgreSQL first:

```bash
docker compose up -d postgres
python scripts/wait_for_postgres.py
```

Create the deterministic source baseline:

```bash
python scripts/seed_source.py --reset
python scripts/profile_source.py
```

Start Kafka and Debezium Connect:

```bash
docker compose up -d kafka connect
docker compose ps
```

Register or update the CDC connector:

```bash
python scripts/register_connector.py
```

Verify the initial CDC snapshot:

```bash
python scripts/profile_cdc.py
```

Verify live create, update and delete propagation:

```bash
python scripts/verify_cdc_operations.py --timeout 30
```

## Automated tests

Run tests that do not require live services:

```bash
python -m unittest discover -s tests -v
```

Run the complete PostgreSQL and CDC suite:

```bash
RUN_POSTGRES_INTEGRATION=1 \
RUN_CDC_INTEGRATION=1 \
python -m unittest discover -s tests -v
```

## Useful service commands

Check container status:

```bash
docker compose ps
```

Check Debezium connector status:

```bash
curl -sS \
  http://127.0.0.1:8083/connectors/commerce-postgres-cdc/status
```

List Kafka topics:

```bash
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list
```

Stop the services while preserving volumes:

```bash
docker compose down
```

Start the preserved environment again:

```bash
docker compose up -d
```

Delete containers and persistent volumes only when a completely fresh environment is required:

```bash
docker compose down -v
```

The `-v` option permanently deletes the local PostgreSQL and Kafka volumes. Run it only when intentionally rebuilding the entire local baseline.

## Important local ports

| Service | Address |
|---|---|
| PostgreSQL | `127.0.0.1:5433` |
| Kafka host bootstrap server | `127.0.0.1:29092` |
| Debezium Connect REST API | `http://127.0.0.1:8083` |

## Evidence

Verified measurements are maintained in:

- `METRICS.md`
- `docs/evaluation/SOURCE_M01.md`
- `docs/evaluation/CDC_KAFKA_M02.md`

## Scope warning

The project currently uses controlled synthetic commerce data and a local Docker environment. The results verify relational modeling, deterministic generation, data integrity, logical replication, CDC transport, event retention and automated integration testing.

They do not yet prove production-scale throughput, cloud deployment, Spark recovery, warehouse reconciliation or internet-scale performance. Those capabilities remain later milestones.