# P03 Verified Metrics

Only rows marked **Yes** may be used as measured project evidence.

## M00 foundation verification

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Python environment | local virtual environment | 3.14.4 | `python --version` | Yes, 2026-09-02 |
| Package import | editable local installation | 0.1.0 | `python -c "import commerce_pipeline; print(commerce_pipeline.__version__)"` | Yes, 2026-09-02 |

## Planned pipeline measurements

| Metric | Configuration | Result | Evidence command | Verified |
|---|---|---:|---|---|
| Source transactions | controlled commerce workload | not measured | future M01 command | No |
| CDC capture completeness | PostgreSQL to Kafka | not measured | future M02 command | No |
| Streaming throughput | Kafka to Bronze | not measured | future M03 command | No |
| Median processing latency | event creation to Bronze | not measured | future M03 command | No |
| P95 processing latency | event creation to Bronze | not measured | future M03 command | No |
| Duplicate-event handling | idempotent processing | not measured | future M03 command | No |
| Invalid-event quarantine | controlled invalid events | not measured | future M03 command | No |
| Warehouse reconciliation | source to analytics models | not measured | future M06 command | No |
| Pipeline recovery | controlled service interruption | not measured | future M06 command | No |
| dbt model tests | facts and dimensions | not measured | future M04 command | No |
| Complete automated suite | unit and live integration tests | not measured | future verification command | No |

## Integrity rules

- Do not replace targets with results without running the evidence command.
- Do not remove failed records from reported totals.
- Report the workload size with performance results.
- Report latency as a local or cloud-specific measurement.
- Record configuration changes before comparing results.
- Preserve failed evaluations and explain their causes.
- Do not claim exactly-once delivery unless it is demonstrated.
- Do not claim Snowflake, AWS, Airflow, Kafka or Spark experience until the corresponding implementation works.

## Scope warning

Initial measurements will use a controlled portfolio workload. They will verify pipeline behavior and reproducibility but will not prove unlimited production-scale performance.