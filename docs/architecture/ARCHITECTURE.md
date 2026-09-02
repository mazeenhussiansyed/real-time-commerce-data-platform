# Architecture: Real-Time Commerce Data Platform

## Design goal

Convert operational commerce changes into reliable, analytics-ready warehouse models while preserving raw evidence and supporting recovery from failures.

## Planned data flow

1. A commerce application writes customers, products, orders, payments and shipments to PostgreSQL.
2. PostgreSQL records database changes in its write-ahead log.
3. Debezium reads those changes using logical replication.
4. Debezium publishes structured CDC events to Kafka topics.
5. Spark Structured Streaming consumes the events.
6. Spark validates schemas and attaches processing metadata.
7. Invalid events are written to quarantine storage.
8. Valid raw events are written to immutable Bronze storage.
9. Incremental loading publishes cleaned records to Snowflake.
10. dbt builds staging models, dimensions, facts and tests.
11. Airflow coordinates bounded loads, dbt runs, reconciliation and backfills.
12. Power BI reads analytics-ready warehouse models.

## Primary source entities

- `customers`
- `products`
- `orders`
- `order_items`
- `payments`
- `shipments`

## Event metadata

Every processed CDC event should preserve:

- source table;
- primary key;
- operation type;
- event timestamp;
- processing timestamp;
- source transaction position;
- schema version;
- ingestion run identifier;
- event checksum;
- validation status.

## Reliability model

The platform will assume that an event may be delivered more than once.

Correctness will come from:

- deterministic event identities;
- checkpointed stream processing;
- idempotent writes;
- uniqueness constraints;
- transactional warehouse loads;
- source-to-target reconciliation;
- replayable Bronze events.

## Data-quality controls

Controls will include:

- required-field validation;
- accepted-value validation;
- numeric-range validation;
- foreign-key checks;
- duplicate detection;
- event-order checks;
- schema-version checks;
- source and target count reconciliation;
- freshness thresholds.

## Local deployment

Docker Compose will provide isolated services for development.

Local object storage may use an S3-compatible service. Cloud evidence will remain separate until AWS or Snowflake integration is successfully verified.

## Security boundary

The repository will contain only synthetic or safely