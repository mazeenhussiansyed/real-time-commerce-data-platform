# M02 CDC and Kafka Evaluation

**Evaluation date:** 2026-09-02

## Objective

Verify that changes committed to the PostgreSQL commerce source are captured through logical replication, converted into Debezium events, published to Apache Kafka and retained for downstream streaming consumers.

## Components

| Component | Configuration |
|---|---|
| Source database | PostgreSQL 17 Alpine |
| Source schema | `commerce` |
| Source tables | 6 |
| CDC connector | Debezium PostgreSQL 3.5.2.Final |
| Event transport | Apache Kafka 4.1.2 |
| Kafka mode | Single-node KRaft |
| Connector name | `commerce-postgres-cdc` |
| Topic prefix | `commerce` |
| Local Kafka endpoint | `127.0.0.1:29092` |
| Debezium REST endpoint | `http://127.0.0.1:8083` |

## Initial snapshot verification

The initial profile was captured before live verification events were generated.

| Table | PostgreSQL rows | Kafka events | Difference |
|---|---:|---:|---:|
| Customers | 1,000 | 1,000 | 0 |
| Products | 250 | 250 | 0 |
| Orders | 5,000 | 5,000 | 0 |
| Order items | 12,500 | 12,500 | 0 |
| Payments | 5,000 | 5,000 | 0 |
| Shipments | 2,499 | 2,499 | 0 |
| Total | 26,249 | 26,249 | 0 |

Initial snapshot findings:

- connector state: `RUNNING`;
- connector task state: `RUNNING`;
- replication slot: active;
- publication tables: 6;
- exact initial snapshot match: true;
- minimum source-to-topic completeness: 100%.

## Live operation verification

A temporary customer was inserted, updated and deleted in three separately committed PostgreSQL transactions.

The verifier captured the following Kafka records:

| Order | Operation | Kafka partition | Kafka offset |
|---:|---|---:|---:|
| 1 | Create (`c`) | 0 | 312 |
| 2 | Update (`u`) | 0 | 313 |
| 3 | Delete (`d`) | 0 | 314 |

Verification result:

| Check | Result |
|---|---:|
| Expected operations | `c`, `u`, `d` |
| Actual operations | `c`, `u`, `d` |
| Events received | 3 |
| Correct order | Yes |
| Remaining PostgreSQL probe rows | 0 |
| Local warm duration | 930.544 ms |
| Status | Verified |

## Debezium envelope handling

Kafka Connect emitted JSON records containing both `schema` and `payload` fields.

The verification consumer was updated to read the actual Debezium data from:

```text
event.payload.before
event.payload.after
event.payload.op
event.payload.source