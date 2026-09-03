# P03 Real-Time Commerce Platform Roadmap

## M00 - Scope and evidence contract - Complete

- Created the Git repository and Python package.
- Defined the commerce data-engineering problem.
- Defined project boundaries and non-goals.
- Created the initial platform architecture.
- Defined evidence and resume-claim rules.
- Added automated package verification.

## M01 - Operational commerce source - Complete

- Created a PostgreSQL 17 operational source.
- Enabled logical write-ahead logging for CDC.
- Modeled customers, products, orders, order items, payments and shipments.
- Added primary keys, foreign keys, constraints, indexes and update triggers.
- Built deterministic synthetic-data generation.
- Loaded 26,249 relational records atomically.
- Added protection against accidental source overwrites.
- Verified counts, financial totals and referential integrity.
- Added unit and live PostgreSQL integration tests.

### Verified M01 outcome

- Six source tables.
- 26,249 stored records.
- Zero referential-integrity failures.
- Zero order-total mismatches.
- Zero payment-total mismatches.
- 10/10 tests passed.

## M02 - Change Data Capture and Kafka transport - Complete

- Configured PostgreSQL logical replication.
- Added Debezium PostgreSQL Change Data Capture.
- Added Apache Kafka event transport.
- Created six table-specific Kafka topics.
- Configured three partitions per source topic.
- Preserved Debezium source metadata.
- Verified the complete initial database snapshot.
- Reconciled PostgreSQL source rows with Kafka events.
- Verified live create, update and delete operations.
- Verified wrapped and unwrapped Debezium payloads.
- Added unit and live CDC integration tests.

### Verified M02 outcome

- Debezium connector and task running.
- Six source tables captured.
- Six Kafka topics and 18 partitions.
- 26,249 initial snapshot events.
- 100% initial source-to-topic completeness.
- Ordered create, update and delete events verified.
- Zero residual probe rows.
- 17/17 tests passed.

## M03 - Spark streaming and Bronze storage - Complete

- Added Apache Spark 4.2.0.
- Connected Spark Structured Streaming to all six Kafka topics.
- Added the Spark Kafka connector for Scala 2.13.
- Preserved Kafka, PostgreSQL and Debezium metadata.
- Generated deterministic SHA-256 event identifiers.
- Wrote valid events to immutable Parquet Bronze storage.
- Partitioned Bronze data by source table and event date.
- Added persistent Spark checkpoint storage.
- Verified zero-record checkpoint reruns.
- Verified three-record incremental CDC processing.
- Added invalid-event classification.
- Wrote rejected records to partitioned quarantine storage.
- Added a Bronze profiling command.
- Added unit and live Spark integration tests.
- Recorded local throughput and connector-latency measurements.

### Verified M03 outcome

- 26,270 final Bronze records.
- 26,270 unique event identifiers.
- Zero duplicate Bronze event identifiers.
- Six source tables represented.
- Six Kafka topics and 18 partitions represented.
- Zero missing required metadata values.
- 992.59 events/second initial local throughput.
- Zero new records during checkpoint rerun.
- Three of three incremental CDC events processed.
- Two unsupported-operation events quarantined.
- Median connector latency of 399 ms.
- P95 connector latency of 471 ms.
- 27/27 unit and live integration tests passed.

## M04 - Warehouse and dbt analytics models

- Add an analytical warehouse target.
- Load validated records incrementally from Bronze.
- Build dbt staging models.
- Create customer and product dimensions.
- Implement SCD Type 2 history where appropriate.
- Create order, payment and shipment fact models.
- Add dbt uniqueness, relationship, accepted-value and not-null tests.
- Generate dbt documentation and lineage.
- Reconcile source, Bronze and warehouse totals.
- Publish Power BI-ready analytics marts.

## M05 - Airflow orchestration and backfills

- Add dependency-aware Airflow DAGs.
- Orchestrate source checks, CDC reconciliation and warehouse loads.
- Add retries, timeouts and failure callbacks.
- Implement date-range backfills.
- Prevent duplicate warehouse records during reruns.
- Record pipeline run IDs and audit metadata.
- Verify successful and failed DAG behavior.
- Measure backfill correctness and duration.

## M06 - Reliability and observability

- Measure data freshness, processing latency and throughput.
- Detect source-to-Bronze and Bronze-to-warehouse differences.
- Simulate late and out-of-order events.
- Simulate compatible and incompatible schema changes.
- Verify checkpoint recovery after controlled interruption.
- Record failed records and failure reasons.
- Add operational health and reconciliation reporting.
- Measure recovery time after a controlled failure.

## M07 - Demonstration, deployment and CI

- Containerize the complete project stack.
- Add service health checks and startup dependencies.
- Add GitHub Actions verification.
- Create an operational data-quality dashboard.
- Document a fresh-machine quick start.
- Record final verified metrics.
- Prepare the interview demonstration.
- Prepare accurate resume evidence.

## Definition of done

- Fresh-machine startup is documented and reproducible.
- All automated tests pass.
- CDC completeness is measured.
- Kafka offsets remain auditable.
- Duplicate Bronze processing is prevented during normal checkpoint recovery.
- Invalid events are quarantined with a reason.
- Source, Bronze and warehouse totals reconcile.
- Historical dimension changes remain auditable.
- Pipeline reruns do not create duplicate warehouse records.
- Failure recovery is demonstrated.
- Dashboard models are reproducible.
- Resume claims use only verified values from `METRICS.md`.

## Evidence rules

- Do not report targets as completed measurements.
- Do not change controlled source counts without creating a new dataset version.
- Do not report latency without its measurement boundary.
- Do not report throughput without the event count and execution environment.
- Local Docker results must not be presented as cloud-scale performance.
- Checkpoint idempotency must not be described as arbitrary replay protection.
- Cloud technologies may be claimed only after successful implementation and verification.
