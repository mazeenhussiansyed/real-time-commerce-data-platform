# P03 Real-Time Commerce Platform Roadmap

## M00 - Scope and evidence contract - Complete

- Created the repository and Python package.
- Defined the business problem and project boundaries.
- Frozen the initial architecture.
- Defined evidence and resume-claim rules.
- Added an automated package test.

## M01 - Operational commerce source - Complete

- Created a PostgreSQL source service.
- Enabled logical write-ahead logging for future CDC.
- Modeled customers, products, orders, order items, payments and shipments.
- Added primary keys, foreign keys, constraints, indexes and update triggers.
- Built deterministic synthetic-data generation.
- Loaded 26,249 relational rows atomically.
- Added overwrite protection.
- Verified counts, financial totals and referential integrity.
- Added unit and live PostgreSQL integration tests.

## M02 - Change Data Capture and event transport

- Configure PostgreSQL logical replication.
- Add Debezium CDC.
- Publish table changes to Kafka-compatible topics.
- Define event keys and schemas.
- Verify insert, update and delete events.
- Measure source-to-topic completeness.

## M03 - Streaming processing and Bronze storage

- Consume CDC events with Spark Structured Streaming.
- Preserve event time, operation type and source metadata.
- Deduplicate replayed events.
- Quarantine invalid events.
- Write immutable partitioned Bronze data.
- Test restart and checkpoint behavior.
- Measure throughput and end-to-end latency.

## M04 - Warehouse and dbt analytics models

- Load validated records into Snowflake incrementally.
- Build dbt staging models.
- Create customer and product dimensions.
- Implement SCD Type 2 history where appropriate.
- Create order, payment and shipment fact models.
- Add dbt tests and documentation.
- Publish Power BI-ready marts.

## M05 - Airflow orchestration and backfills

- Add dependency-aware Airflow DAGs.
- Orchestrate reconciliation and warehouse loads.
- Add retries, timeouts and failure callbacks.
- Implement date-range backfills.
- Prevent duplicate records during reruns.
- Record run-level audit metadata.

## M06 - Reliability and observability

- Measure freshness, latency and throughput.
- Detect source-to-target count differences.
- Simulate late and out-of-order events.
- Simulate schema changes.
- Verify checkpoint recovery.
- Record failed records and reasons.
- Measure recovery time after a controlled failure.

## M07 - Demonstration, deployment and CI

- Containerize the complete local stack.
- Add service health checks and startup dependencies.
- Add GitHub Actions verification.
- Create an operational dashboard.
- Document a fresh-machine quick start.
- Record final verified metrics.
- Prepare the interview demonstration and resume evidence.

## Definition of done

- Fresh-machine startup is documented.
- All automated tests pass.
- CDC completeness is measured.
- Duplicate processing is prevented.
- Invalid events are quarantined.
- Source and warehouse totals reconcile.
- Historical changes are auditable.
- Failure recovery is demonstrated.
- Dashboard models are reproducible.
- Resume claims use only verified metrics.