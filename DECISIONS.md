# P03 Architecture Decisions

## ADR-001 - Data Engineering is the primary objective

**Decision:** Build a commerce data platform without an LLM feature.

**Reason:** Project 2 already demonstrates RAG and semantic retrieval. Project 3 must provide stronger evidence in CDC, streaming, orchestration, warehouse modeling and reliability.

## ADR-002 - Local-first development

**Decision:** Build and verify services locally with Docker Compose before adding cloud resources.

**Reason:** Local development is reproducible, inexpensive and easier to debug. Cloud services will be claimed only after they are used successfully.

## ADR-003 - CDC instead of repeated full-table extraction

**Decision:** Use Debezium to capture PostgreSQL inserts, updates and deletes.

**Reason:** Change Data Capture demonstrates incremental processing and avoids repeatedly scanning complete source tables.

## ADR-004 - Immutable Bronze events

**Decision:** Preserve raw CDC events without silently modifying them.

**Reason:** Immutable events support auditing, replay, debugging and recovery.

## ADR-005 - Idempotent processing

**Decision:** Design consumers and loads so replaying the same event does not create duplicate analytical records.

**Reason:** Kafka and distributed processors can deliver records more than once. Safe reruns are more realistic than assuming perfect delivery.

## ADR-006 - Event time is separate from processing time

**Decision:** Record when a business event occurred and when the platform processed it.

**Reason:** The distinction is required to measure lateness, lag and out-of-order delivery.

## ADR-007 - dbt owns analytical transformations

**Decision:** Use dbt for warehouse staging, facts, dimensions, tests and documentation.

**Reason:** Version-controlled SQL models make analytical transformations testable and understandable.

## ADR-008 - Airflow orchestrates bounded work

**Decision:** Use Airflow for scheduled loads, reconciliation, dbt execution and backfills, not as the Kafka event transporter.

**Reason:** Kafka handles continuous events while Airflow coordinates tasks with defined starts and finishes.

## ADR-009 - Evidence before claims

**Decision:** Record a technology or result on the resume only after reproducible verification.

**Reason:** Planned architecture is not equivalent to completed engineering work.