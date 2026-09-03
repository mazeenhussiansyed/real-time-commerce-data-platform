# M03 Spark Structured Streaming and Bronze Storage

**Evaluation date:** 2026-09-03

## Purpose

Milestone M03 adds a streaming processing layer between Kafka and downstream analytics storage.

Apache Spark Structured Streaming consumes PostgreSQL change events published by Debezium to Kafka. Valid events are preserved in immutable, partitioned Parquet Bronze storage. Invalid events are written separately to quarantine storage with a failure reason.

## Runtime configuration

| Component | Verified configuration |
|---|---|
| Apache Spark | 4.2.0 |
| Spark execution | Local Docker, `local[2]` |
| Scala | 2.13.18 |
| Java | OpenJDK 21.0.11 |
| Kafka connector | `spark-sql-kafka-0-10_2.13:4.2.0` |
| Kafka topics | 6 |
| Kafka partitions | 18 |
| Bronze format | Parquet |
| Bronze partitions | `source_table`, `event_date` |
| Quarantine partitions | `invalid_reason`, `event_date` |
| Offset recovery | Spark checkpoint storage |

## Bronze event contract

Each valid Bronze record preserves:

- deterministic SHA-256 event ID;
- ingestion run ID;
- ingestion timestamp;
- event timestamp;
- Debezium operation type;
- source timestamp;
- connector timestamp;
- PostgreSQL LSN;
- PostgreSQL transaction ID;
- Kafka topic;
- Kafka partition;
- Kafka offset;
- Kafka timestamp;
- message key;
- before record;
- after record;
- Debezium payload;
- original message value;
- schema name;
- source table;
- event-date partition.

The deterministic event ID is generated from:

```text
Kafka topic + Kafka partition + Kafka offset
