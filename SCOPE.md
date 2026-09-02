# P03 Commerce Platform Scope

## Project goal

Build a reproducible real-time commerce data platform that captures operational database changes, transports them through Kafka, processes them incrementally and publishes trustworthy analytics models.

The project is designed primarily as evidence for Data Engineer roles.

## Business problem

An online retailer creates and updates customers, products, orders, payments and shipments throughout the day.

Analytics teams need timely reporting without running expensive analytical queries directly against the operational database. They also need protection from duplicate events, invalid records, late data, schema changes and pipeline failures.

## Primary users

- Data engineers operating the pipelines.
- Analytics engineers building warehouse models.
- Data analysts creating operational reports.
- Operations teams monitoring orders, payments and shipments.
- Engineering managers reviewing reliability and data quality.

## In scope

- PostgreSQL operational commerce database.
- Realistic commerce data generation.
- Debezium Change Data Capture.
- Kafka event transportation.
- Spark Structured Streaming.
- Immutable Bronze event storage.
- Incremental Snowflake loading.
- dbt transformations, tests and documentation.
- Dimensional facts and dimensions.
- SCD Type 2 historical tracking.
- Airflow orchestration and backfills.
- Duplicate and invalid-event handling.
- Source-to-warehouse reconciliation.
- Data freshness and pipeline-run metrics.
- Docker Compose local deployment.
- GitHub Actions verification.
- Power BI operational reporting.

## Out of scope

- Customer-facing e-commerce website.
- Payment processing with real financial information.
- Production credentials or private customer data.
- Generative AI or LLM functionality.
- Kubernetes deployment.
- Multi-region disaster recovery.
- Claims of production scale without measured evidence.

## Data-safety boundary

The project will use synthetic or safely licensed public data only.

No private customer information, payment credentials, protected health information or employer data will be stored in the repository.

## Evidence contract

A result may be used on a resume only when:

1. The implementation exists in the repository.
2. A reproducible command measures it.
3. The result is recorded in `METRICS.md`.
4. The result is marked `Yes`.
5. The test or evaluation date is recorded.

Targets and planned architecture are not measured achievements.

## Definition of done

- Fresh-machine startup is documented.
- Operational source tables are reproducible.
- Inserts and updates are captured through CDC.
- Kafka events are processed idempotently.
- Raw events remain available for replay.
- Invalid events are quarantined.
- Warehouse facts and dimensions pass dbt tests.
- Source and warehouse counts are reconciled.
- Failed work can be retried or backfilled.
- Pipeline latency, throughput and freshness are measured.
- Docker Compose starts the complete local platform.
- CI runs the automated verification suite.
- Power BI consumes analytics-ready tables.
- Resume claims use only verified evidence.