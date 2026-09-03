# M04 Warehouse and dbt Analytics Evaluation

**Evaluation date:** 2026-09-03
**Environment:** Local Docker Desktop and WSL2
**Warehouse:** PostgreSQL 17
**Transformation framework:** dbt Core 1.11.14 with dbt-postgres 1.11.0
**Streaming engine:** Apache Spark 4.2.0

## Objective

M04 converts immutable Spark Bronze CDC events into a tested analytical warehouse.

The milestone verifies:

- incremental Bronze-to-warehouse loading;
- duplicate-safe event ingestion;
- current-state reconstruction from CDC history;
- SCD Type 2 customer history;
- dimensional and fact modeling;
- Power BI-ready reporting marts;
- business and financial reconciliation;
- automated dbt and live integration testing.

## Data flow

```text
PostgreSQL source
        |
        v
Debezium CDC
        |
        v
Apache Kafka
        |
        v
Spark Bronze Parquet
        |
        v
Incremental JDBC warehouse loader
        |
        v
PostgreSQL analytics warehouse
        |
        v
dbt staging, intermediate, dimensions, facts and marts
