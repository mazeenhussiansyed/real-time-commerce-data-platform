# P03 Verified Metrics

Only rows marked **Yes** may be used as measured project evidence.

## M00 environment verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Python runtime | local virtual environment | 3.14.4 | `python --version` | Yes, 2026-09-02 |
| Package import | editable installation | 0.1.0 | package import command | Yes, 2026-09-02 |

## M01 operational source verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| PostgreSQL source tables | `commerce` schema | 6 | `python scripts/wait_for_postgres.py` | Yes, 2026-09-02 |
| PostgreSQL WAL level | CDC-ready configuration | logical | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Customers | deterministic seed `20260902` | 1,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Products | deterministic seed `20260902` | 250 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Orders | deterministic seed `20260902` | 5,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Order items | one to four items per order | 12,500 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Payments | one payment per order | 5,000 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Shipments | processing, shipped and delivered orders | 2,499 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Total stored rows | all six source tables | 26,249 | source profile counts | Yes, 2026-09-02 |
| Total order value | synthetic USD orders | $5,053,882.86 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Average order value | 5,000 orders | $1,010.78 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Referential-integrity failures | five relationship checks | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Order-total mismatches | orders compared with item totals | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Payment-total mismatches | orders compared with payments | 0 | `python scripts/profile_source.py` | Yes, 2026-09-02 |
| Dataset fingerprint | SHA-256 | `9616a650...b49d83e5` | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Baseline load duration | local Docker PostgreSQL | 1.451 seconds | `python scripts/seed_source.py --reset` | Yes, 2026-09-02 |
| Existing-data protection | seed without `--reset` | overwrite rejected | `python scripts/seed_source.py` | Yes, 2026-09-02 |
| Complete M01 test suite | unit and live PostgreSQL tests | 10/10 passed | live test command | Yes, 2026-09-02 |

## M02 PostgreSQL CDC and Kafka verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| PostgreSQL replication slot | Debezium logical replication | active | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Publication tables | `commerce_publication` | 6 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Debezium connector | `commerce-postgres-cdc` | RUNNING | connector status command | Yes, 2026-09-02 |
| Debezium connector task | task 0 | RUNNING | connector status command | Yes, 2026-09-02 |
| Captured source tables | PostgreSQL commerce schema | 6 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Table-specific Kafka topics | one per source table | 6 | Kafka topic-list command | Yes, 2026-09-02 |
| Kafka source-topic partitions | three per table topic | 18 | Kafka topic-description command | Yes, 2026-09-02 |
| Initial source rows | six PostgreSQL tables | 26,249 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial Kafka snapshot events | six source topics | 26,249 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial source-to-topic difference | exact snapshot checkpoint | 0 | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Initial source-to-topic completeness | exact snapshot checkpoint | 100% | `python scripts/profile_cdc.py` | Yes, 2026-09-02 |
| Live create event | customer probe | captured | `python scripts/verify_cdc_operations.py --timeout 30` | Yes, 2026-09-02 |
| Live update event | customer probe | captured | `python scripts/verify_cdc_operations.py --timeout 30` | Yes, 2026-09-02 |
| Live delete event | customer probe | captured | `python scripts/verify_cdc_operations.py --timeout 30` | Yes, 2026-09-02 |
| Residual probe rows | source cleanup after delete | 0 | CDC verification result | Yes, 2026-09-02 |
| Successful CDC verification duration | local Docker services | 930.544 ms | CDC verification result | Yes, 2026-09-02 |
| Complete M02 test suite | unit and live PostgreSQL/CDC tests | 17/17 passed | live test command | Yes, 2026-09-02 |

## M03 Spark streaming and Bronze verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Spark runtime | official Python image | 4.2.0 | Spark version command | Yes, 2026-09-02 |
| Scala runtime | Spark 4.2.0 image | 2.13.18 | Spark version command | Yes, 2026-09-02 |
| Java runtime | Spark 4.2.0 image | OpenJDK 21.0.11 | Spark version command | Yes, 2026-09-02 |
| Spark Kafka connector | Scala 2.13 artifact | `4.2.0` | Spark dependency resolution | Yes, 2026-09-02 |
| Kafka topics consumed | six commerce topics | 6 | Bronze profile command | Yes, 2026-09-03 |
| Kafka partitions consumed | three per source topic | 18 | Bronze profile command | Yes, 2026-09-03 |
| Initial Bronze input events | Kafka history checkpoint | 26,264 | first Bronze stream result | Yes, 2026-09-02 |
| Initial Bronze records written | valid Kafka events | 26,264 | first Bronze stream result | Yes, 2026-09-02 |
| Initial invalid events | first Bronze run | 0 | first Bronze stream result | Yes, 2026-09-02 |
| Initial duplicate event IDs | first Bronze run | 0 | first Bronze stream result | Yes, 2026-09-02 |
| Initial Bronze duration | local Spark `local[2]` | 26.460 seconds | first Bronze stream result | Yes, 2026-09-02 |
| Initial Bronze throughput | 26,264 events, local Docker | 992.59 events/second | first Bronze stream result | Yes, 2026-09-02 |
| Checkpoint rerun input rows | no new Kafka events | 0 | second Bronze stream result | Yes, 2026-09-03 |
| Checkpoint rerun new records | persistent Spark checkpoints | 0 | second Bronze stream result | Yes, 2026-09-03 |
| Checkpoint rerun duplicate IDs | persistent Spark checkpoints | 0 | second Bronze stream result | Yes, 2026-09-03 |
| Incremental Kafka events | one create, update and delete | 3 | incremental Bronze stream result | Yes, 2026-09-03 |
| Incremental Bronze records | only new Kafka offsets | 3 | incremental Bronze stream result | Yes, 2026-09-03 |
| Incremental duplicate event IDs | checkpoint continuation | 0 | incremental Bronze stream result | Yes, 2026-09-03 |
| Unsupported operations submitted | manual and automated checks | 2 | quarantine verification | Yes, 2026-09-03 |
| Unsupported operations written to Bronze | invalid operation `x` | 0 | Bronze stream results | Yes, 2026-09-03 |
| Unsupported operations quarantined | reason `unsupported_operation` | 2/2 | Bronze profile command | Yes, 2026-09-03 |
| Final Bronze records | final M03 checkpoint | 26,270 | Bronze profile command | Yes, 2026-09-03 |
| Final unique Bronze event IDs | SHA-256 topic/partition/offset IDs | 26,270 | Bronze profile command | Yes, 2026-09-03 |
| Final duplicate Bronze event IDs | final M03 checkpoint | 0 | Bronze profile command | Yes, 2026-09-03 |
| Required metadata null values | nine required metadata fields | 0 | Bronze profile command | Yes, 2026-09-03 |
| Source tables represented | final Bronze dataset | 6 | Bronze profile command | Yes, 2026-09-03 |
| Snapshot operations | Debezium operation `r` | 26,249 | Bronze profile command | Yes, 2026-09-03 |
| Create operations | Debezium operation `c` | 7 | Bronze profile command | Yes, 2026-09-03 |
| Update operations | Debezium operation `u` | 7 | Bronze profile command | Yes, 2026-09-03 |
| Delete operations | Debezium operation `d` | 7 | Bronze profile command | Yes, 2026-09-03 |
| Quarantine records | final M03 checkpoint | 2 | Bronze profile command | Yes, 2026-09-03 |
| Quarantine duplicate event IDs | final M03 checkpoint | 0 | Bronze profile command | Yes, 2026-09-03 |
| Connector latency sample | live create/update/delete events | 21 | Bronze profile command | Yes, 2026-09-03 |
| Median connector latency | connector minus source timestamp | 399 ms | Bronze profile command | Yes, 2026-09-03 |
| P95 connector latency | connector minus source timestamp | 471 ms | Bronze profile command | Yes, 2026-09-03 |
| Maximum connector latency | connector minus source timestamp | 503 ms | Bronze profile command | Yes, 2026-09-03 |
| Complete M03 test suite | unit plus all live integrations | 27/27 passed | complete live test command | Yes, 2026-09-03 |
| Complete M03 test duration | local Docker environment | 29.783 seconds | complete live test command | Yes, 2026-09-03 |

## M04 warehouse and dbt analytics verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Warehouse runtime | local analytics service | PostgreSQL 17 | `python scripts/wait_for_warehouse.py` | Yes, 2026-09-03 |
| Warehouse schemas | raw, staging, snapshots, analytics and audit | 5 | `python scripts/wait_for_warehouse.py` | Yes, 2026-09-03 |
| Initial Bronze records loaded | Spark JDBC batch | 26,273 | warehouse load result | Yes, 2026-09-03 |
| Initial target records | `raw.bronze_events` | 26,273 | warehouse load result | Yes, 2026-09-03 |
| Initial missing target records | source event IDs compared with target | 0 | warehouse load result | Yes, 2026-09-03 |
| Initial warehouse load duration | local Spark `local[2]` | 14.863 seconds | warehouse load result | Yes, 2026-09-03 |
| Initial insertion throughput | 26,273 records, local Docker | 1,767.66 records/second | warehouse load result | Yes, 2026-09-03 |
| Idempotent rerun inserted records | unchanged Bronze input | 0 | second warehouse load result | Yes, 2026-09-03 |
| Idempotent rerun duration | existing target IDs | 8.041 seconds | second warehouse load result | Yes, 2026-09-03 |
| Controlled incremental load | one customer update | 1 record | incremental warehouse load result | Yes, 2026-09-03 |
| Final warehouse events | after complete M04 verification | 26,277 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Final unique warehouse event IDs | event-ID deduplication | 26,277 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Final duplicate warehouse event IDs | final M04 profile | 0 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Required warehouse metadata nulls | six required fields | 0 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Warehouse load audit runs | initial, rerun and incremental loads | 5 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed current tables | six source entities | 6 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed customers | latest non-deleted records | 1,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed products | latest non-deleted records | 250 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed orders | latest non-deleted records | 5,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed order items | latest non-deleted records | 12,500 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed payments | latest non-deleted records | 5,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Reconstructed shipments | latest non-deleted records | 2,499 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| dbt Core version | isolated Python 3.13 image | 1.11.14 | `dbt --version` container command | Yes, 2026-09-03 |
| dbt PostgreSQL adapter | isolated dbt image | 1.11.0 | `dbt --version` container command | Yes, 2026-09-03 |
| dbt models | staging, intermediate and analytics | 22 | complete dbt build | Yes, 2026-09-03 |
| dbt view models | staging and intermediate | 13 | complete dbt build | Yes, 2026-09-03 |
| dbt table models | dimensions, facts and marts | 9 | complete dbt build | Yes, 2026-09-03 |
| dbt snapshots | customer SCD Type 2 | 1 | complete dbt build | Yes, 2026-09-03 |
| Customer dimension versions | 1,000 customers | 1,001 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Current customer versions | null `dbt_valid_to` | 1,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Historical customer versions | controlled customer update | 1 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Overlapping customer histories | SCD validity test | 0 | dbt history test | Yes, 2026-09-03 |
| Product dimension records | all current products | 250 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Active product records | current active products | 240 | dimension verification query | Yes, 2026-09-03 |
| Order facts | one row per order | 5,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Order-item facts | one row per order item | 12,500 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Payment facts | one row per payment | 5,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Shipment facts | one row per shipment | 2,499 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Daily reporting rows | 2026-01-01 through 2026-01-18 | 18 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Customer-value mart rows | one current row per customer | 1,000 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Product-performance mart rows | one row per product | 250 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Total order value | order facts | $5,053,882.86 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Total order-item value | item facts | $5,053,882.86 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Total payment value | payment facts | $5,053,882.86 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Order-to-item difference | financial reconciliation | $0.00 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Order-to-payment difference | financial reconciliation | $0.00 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Daily mart count differences | orders, payments and shipments | 0 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| Daily mart financial differences | orders and payments | $0.00 | `python scripts/profile_warehouse.py` | Yes, 2026-09-03 |
| dbt data tests | schema, relationship and business tests | 92/92 passed | `dbt test` container command | Yes, 2026-09-03 |
| Complete dbt build | models, snapshot and tests | 115/115 passed | `dbt build` container command | Yes, 2026-09-03 |
| dbt execution duration | local warm warehouse | 2.16 seconds | timed complete dbt build | Yes, 2026-09-03 |
| dbt Docker wall time | complete local command | 4.630 seconds | timed complete dbt build | Yes, 2026-09-03 |
| Complete M04 Python suite | unit plus all live integrations | 33/33 passed | complete live test command | Yes, 2026-09-03 |
| Complete M04 test duration | local Docker environment | 30.976 seconds | complete live test command | Yes, 2026-09-03 |

## Planned orchestration metrics

| Metric | Result | Evidence command | Verified |
|---|---:|---|---|
| Backfill duplicate records | Not measured | future M05 evaluation | No |
| Airflow DAG success and retry behavior | Not measured | future M05 evaluation | No |
| Airflow pipeline duration | Not measured | future M05 benchmark | No |
| Failed-run audit records | Not measured | future M05 evaluation | No |

## Planned reliability metrics

| Metric | Result | Evidence command | Verified |
|---|---:|---|---|
| Late-event handling | Not measured | future M06 evaluation | No |
| Schema-change handling | Not measured | future M06 evaluation | No |
| Controlled failure recovery time | Not measured | future M06 benchmark | No |
| Source-to-warehouse freshness | Not measured | future M06 benchmark | No |
| Final CI verification | Not measured | future M07 workflow | No |

## Integrity rules

- Do not replace missing results with targets.
- Do not report throughput without the event count and execution environment.
- Do not report latency without defining its start and end timestamps.
- A checkpoint rerun is successful only when no already-committed Kafka offsets are appended again.
- Checkpoint idempotency must not be presented as protection after intentional checkpoint deletion.
- CDC completeness requires reconciling source rows or changes with Kafka events.
- Warehouse event history counts must not be confused with current business-record counts.
- A quarantined record must preserve its event ID, original value and failure reason.
- Cloud services may be listed only after they are used successfully.
- Local benchmarks must not be presented as universal production performance.

## Scope warning

This project uses controlled synthetic commerce data and a local Docker development environment.

The completed measurements verify source modeling, deterministic generation, PostgreSQL logical replication, Debezium CDC, Kafka transport, Spark Structured Streaming, checkpoint continuation, Bronze storage, quarantine handling, incremental warehouse loading, dbt transformations, SCD Type 2 history, dimensional modeling, reporting marts and financial reconciliation.

They do not prove internet-scale performance, production-cluster scalability, arbitrary replay safety after checkpoint deletion, Snowflake execution or cloud-scale latency.
