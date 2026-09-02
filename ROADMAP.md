# P03 Commerce Platform Roadmap

## M00 - Scope and evidence contract - In progress

- Define the business problem and project boundaries.
- Record architecture decisions.
- Create the Python package and test structure.
- Define the verified-metrics policy.
- Commit the reproducible project foundation.

## M01 - Operational commerce source

- Run PostgreSQL with Docker Compose.
- Create customers, products, orders, order items, payments and shipments.
- Add primary keys, foreign keys, constraints and timestamps.
- Generate reproducible commerce transactions.
- Introduce controlled invalid and duplicate cases.
- Profile source-table counts and quality.

## M02 - Change Data Capture and Kafka

- Configure PostgreSQL logical replication.
- Deploy Kafka-compatible event transport.
- Configure the Debezium PostgreSQL connector.
- Capture inserts, updates and deletes.
- Validate event keys, schemas and operation types.
- Measure captured-event completeness.

## M03 - Streaming processing and Bronze storage

- Consume CDC events with Spark Structured Streaming.
- Apply schema validation and event metadata.
- Handle duplicates and malformed records.
- Quarantine rejected events.
- Store immutable Bronze events in partitioned files.
- Add checkpoints and restart recovery.
- Measure throughput and processing latency.

## M04 - Warehouse and dbt modeling

- Load cleaned events incrementally into Snowflake.
- Create dbt staging models.
- Build customer, product and date dimensions.
- Build order, payment and shipment facts.
- Implement SCD Type 2 history.
- Add dbt uniqueness, relationship and accepted-value tests.
- Generate dbt documentation and lineage.

## M05 - Airflow orchestration

- Orchestrate batch loads and dbt execution.
- Add dependencies, retries and failure handling.
- Implement parameterized historical backfills.
- Record pipeline-run metadata.
- Prevent overlapping or duplicate runs.

## M06 - Reliability and observability

- Reconcile PostgreSQL, Kafka, Bronze and warehouse counts.
- Test late and out-of-order events.
- Test schema evolution.
- Simulate service interruption and recovery.
- Measure freshness, lag, throughput and recovery time.
- Document failure investigation procedures.

## M07 - Analytics, deployment and CI

- Publish operational Power BI datasets.
- Build order, payment and shipment KPIs.
- Containerize the reproducible local platform.
- Add service health checks.
- Add GitHub Actions verification.
- Document the live demonstration.
- Record final verified metrics.