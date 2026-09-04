# M05 Airflow Orchestration and Backfill Evaluation

**Evaluation date:** 2026-09-03
**Environment:** Local Docker Desktop and WSL2
**Orchestrator:** Apache Airflow 3.3.1
**Executor:** LocalExecutor
**Warehouse:** PostgreSQL 17
**Transformation framework:** dbt Core 1.11.14
**Streaming engine:** Apache Spark 4.2.0

## Objective

M05 adds dependency-aware workflow orchestration, controlled date-range backfills, retries, timeouts, failure callbacks and run-level audit history to the completed commerce data platform.

## Implemented components

- Custom Apache Airflow 3.3.1 image.
- Separate Airflow metadata PostgreSQL database.
- Airflow API server, scheduler and DAG processor.
- Lightweight LocalExecutor configuration.
- Docker-based Spark and dbt task execution.
- Eight-task incremental and backfill DAG.
- Maximum 31-day backfill safeguard.
- Warehouse pipeline-run audit table.
- Idempotent audit updates by Airflow run ID.
- Success and failure callbacks.
- Date-range incremental daily-mart replacement.
- Backfill recovery and reconciliation verification.
- Unit and live orchestration integration tests.

## Orchestrated DAG

The `commerce_incremental_pipeline` DAG executes these tasks in order:

| Order | Task | Purpose |
|---:|---|---|
| 1 | `start_run_audit` | Validate configuration and create the running audit record |
| 2 | `validate_services` | Validate PostgreSQL, warehouse, Debezium and Kafka |
| 3 | `run_bronze_stream` | Consume new Kafka events with Spark |
| 4 | `profile_bronze` | Validate Bronze metadata, uniqueness and quarantine state |
| 5 | `load_bronze_to_warehouse` | Load missing Bronze events without duplicates |
| 6 | `run_dbt_build` | Build and test warehouse analytics models |
| 7 | `profile_warehouse` | Reconcile warehouse dimensions, facts and marts |
| 8 | `complete_run_audit` | Mark the audit record successful with measured duration |

The DAG allows one active run at a time and applies two retries with a 30-second retry delay. Task and DAG timeouts prevent indefinitely running jobs.

## Verified results

| Result | Verified value |
|---|---:|
| Airflow version | 3.3.1 |
| Docker Engine access from Airflow | Successful |
| Docker Engine version | 29.6.2 |
| DAG tasks | 8 |
| Successful incremental duration | 48.036 seconds |
| Controlled backfill dates | 2026-01-05 through 2026-01-07 |
| Backfill window | 3 days |
| Successful backfill duration | 45.464 seconds |
| Maximum permitted backfill window | 31 days |
| Backfilled daily-mart rows | 3 |
| Total daily-mart rows after backfill | 18 |
| Unique daily-mart dates | 18 |
| Duplicate daily-mart dates | 0 |
| Reconciled orders | 5,000 |
| Reconciled order value | $5,053,882.86 |
| Corrected backfill dbt build | 115/115 passed |
| Corrected backfill dbt duration | 2.31 seconds |
| Recorded pipeline audit runs | 3 |
| Successful audit runs | 2 |
| Failed audit runs | 1 |
| Running audit records after completion | 0 |
| Incomplete completed-run records | 0 |
| Incomplete failure records | 0 |
| Recovered backfill failures | 1 |
| Complete Python and live integration suite | 47/47 passed |
| Complete test duration | 33.921 seconds |

## Failure and recovery verification

The first Airflow-controlled backfill used a partial dbt ancestor selection. The selected graph included a relationship test that referenced an intermediate model outside the selected build graph.

The daily incremental model itself successfully replaced the requested three dates, and its uniqueness, not-null and financial-reconciliation tests passed. The unrelated relationship test then failed because its other parent relation was not selected.

Airflow applied the configured retries. After the final attempt, the failure callback recorded:

- run mode `backfill`;
- dates 2026-01-05 through 2026-01-07;
- status `failed`;
- failed task `run_dbt_build`;
- measured duration 116.183 seconds.

The correction changed the backfill to execute the complete dbt graph while retaining the date filter only inside the incremental daily mart.

The corrected dbt build passed 115 of 115 resources. A new Airflow backfill completed in 45.464 seconds and replaced the same three dates without producing duplicate reporting dates or changing financial totals.

## Audit controls

The `audit.pipeline_runs` table stores:

- Airflow run ID;
- DAG ID;
- incremental or backfill mode;
- backfill start and end dates;
- running, successful or failed status;
- start and completion timestamps;
- duration;
- failed task ID;
- failure message;
- additional JSON metadata.

The Airflow run ID is the primary key, allowing retries or task clearing to update the same audit record instead of creating duplicate records.

## Backfill controls

Backfill configuration requires:

- `run_mode` equal to `backfill`;
- both start and end dates;
- ISO `YYYY-MM-DD` date format;
- start date not later than end date;
- a maximum inclusive window of 31 days.

The daily mart uses `order_date` as its unique key with dbt incremental delete-and-insert behavior. Repeating the same window replaces those dates rather than appending duplicates.

## Evidence commands

```bash
python scripts/profile_orchestration.py
