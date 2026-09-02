# P03 Real-Time Commerce Platform Roadmap

## M00 - Scope and evidence contract - Complete

- Created the repository and installable Python package.
- Defined the commerce data-platform business problem.
- Defined project boundaries and non-goals.
- Created the initial architecture.
- Defined evidence and resume-claim rules.
- Added an automated package test.

## M01 - Operational commerce source - Complete

- Created a PostgreSQL 17 operational source.
- Enabled logical write-ahead logging for CDC.
- Modeled customers, products, orders, order items, payments and shipments.
- Added primary keys, foreign keys, constraints, indexes and update triggers.
- Built deterministic synthetic-data generation.
- Loaded 26,249 relational rows atomically.
- Added protection against accidental source replacement.
- Verified table counts, financial totals and referential integrity.
- Added unit and live PostgreSQL integration tests.
- Recorded verified M01 evidence.

## M02 - Change Data Capture and Kafka transport - Complete

- Added Apache Kafka 4.1.2 in KRaft mode.
- Added Debezium Connect 3.5.2.Final.
- Configured PostgreSQL logical replication.
- Created a Debezium publication covering six commerce tables.
- Created and activated a PostgreSQL replication slot.
- Registered the `commerce-postgres-cdc` source connector.
- Published changes to six table-specific Kafka topics.
- Verified an exact 26,249-event initial snapshot.
- Measured 100% minimum source-to-topic completeness.
- Verified live create, update and delete events.
- Confirmed CDC operation order as `c`, `u`, `d`.
- Confirmed temporary verification rows were removed from PostgreSQL.
- Preserved historical events in Kafka.
- Added schema-wrapped Debezium event handling.
- Added unit and live CDC integration tests.
- Verified 17/17 tests across the complete project suite.
- Recorded verified M02 evidence.

## M03 - Streaming processing and Bronze storage

- Add Spark Structured Streaming.
- Consume all six Debezium Kafka topics.
- Parse Debezium schema and payload envelopes.
- Preserve event time, operation type and source metadata.
- Write immutable raw events to Bronze storage.
- Partition Bronze data for efficient processing.
- Add checkpointing for restart safety.
- Deduplicate replayed Kafka events.
- Quarantine malformed and invalid events.
- Verify restart and checkpoint recovery.
- Measure event throughput and end-to-end latency.

## M04 - Warehouse and dbt analytics models

- Load validated records into Snowflake incrementally.
- Build dbt staging models.
- Create customer and product dimensions.
- Implement SCD Type 2 history where appropriate.
- Create order, payment and shipment fact models.
- Add dbt relationship, uniqueness and accepted-value tests.
- Generate dbt documentation.
- Publish Power BI-ready analytics marts.
- Reconcile source and warehouse totals.

## M05 - Airflow orchestration and backfills

- Add dependency-aware Airflow DAGs.
- Orchestrate source checks, streaming checks and warehouse loads.
- Add retries, timeouts and failure callbacks.
- Implement date-range backfills.
- Prevent duplicate business records during reruns.
- Record run-level audit metadata.
- Verify backfill idempotency.

## M06 - Reliability and observability

- Measure freshness, latency and throughput.
- Detect source-to-target count differences.
- Simulate late and out-of-order events.
- Simulate compatible and incompatible schema changes.
- Verify checkpoint recovery.
- Record failed records and rejection reasons.
- Measure recovery time after a controlled failure.
- Add operational health and reconciliation reporting.

## M07 - Demonstration, deployment and CI

- Containerize the complete local stack.
- Add service health checks and startup dependencies.
- Add GitHub Actions verification.
- Create an operational dashboard.
- Document a fresh-machine quick start.
- Record final verified metrics.
- Prepare an interview demonstration.
- Prepare evidence-backed resume bullets.

## Definition of done

- Fresh-machine startup is documented.
- All automated tests pass.
- CDC completeness is measured.
- Duplicate processing is prevented.
- Invalid events are quarantined.
- Source and warehouse totals reconcile.
- Historical changes remain auditable.
- Failure recovery is demonstrated.
- Dashboard models are reproducible.
- Resume claims use only verified metrics.