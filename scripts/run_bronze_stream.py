from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TABLE_TOPICS = {
    "customers": "commerce.commerce.customers",
    "products": "commerce.commerce.products",
    "orders": "commerce.commerce.orders",
    "order_items": "commerce.commerce.order_items",
    "payments": "commerce.commerce.payments",
    "shipments": "commerce.commerce.shipments",
}

ALLOWED_OPERATIONS = ("r", "c", "u", "d")
MINIMUM_BASELINE_EVENTS = 26_249


def environment_path(name: str, default: str) -> str:
    return os.getenv(name, default)


def json_field(
    value_column: str,
    wrapped_path: str,
    unwrapped_path: str,
) -> F.Column:
    return F.coalesce(
        F.get_json_object(
            F.col(value_column),
            wrapped_path,
        ),
        F.get_json_object(
            F.col(value_column),
            unwrapped_path,
        ),
    )


def enrich_kafka_events(
    events: DataFrame,
    run_id: str,
) -> DataFrame:
    enriched = events.select(
        F.col("key").cast("string").alias("key_json"),
        F.col("value").cast("string").alias("value_json"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )

    enriched = enriched.withColumn(
        "source_table",
        F.regexp_extract(
            F.col("kafka_topic"),
            r"([^.]+)$",
            1,
        ),
    )

    enriched = enriched.withColumn(
        "operation",
        json_field(
            "value_json",
            "$.payload.op",
            "$.op",
        ),
    )

    enriched = enriched.withColumn(
        "source_timestamp_ms",
        json_field(
            "value_json",
            "$.payload.source.ts_ms",
            "$.source.ts_ms",
        ).cast("long"),
    )

    enriched = enriched.withColumn(
        "connector_timestamp_ms",
        json_field(
            "value_json",
            "$.payload.ts_ms",
            "$.ts_ms",
        ).cast("long"),
    )

    enriched = enriched.withColumn(
        "source_lsn",
        json_field(
            "value_json",
            "$.payload.source.lsn",
            "$.source.lsn",
        ).cast("long"),
    )

    enriched = enriched.withColumn(
        "source_transaction_id",
        json_field(
            "value_json",
            "$.payload.source.txId",
            "$.source.txId",
        ).cast("long"),
    )

    enriched = enriched.withColumn(
        "before_json",
        json_field(
            "value_json",
            "$.payload.before",
            "$.before",
        ),
    )

    enriched = enriched.withColumn(
        "after_json",
        json_field(
            "value_json",
            "$.payload.after",
            "$.after",
        ),
    )

    enriched = enriched.withColumn(
        "payload_json",
        F.coalesce(
            F.get_json_object(
                F.col("value_json"),
                "$.payload",
            ),
            F.col("value_json"),
        ),
    )

    enriched = enriched.withColumn(
        "schema_name",
        F.get_json_object(
            F.col("value_json"),
            "$.schema.name",
        ),
    )

    enriched = enriched.withColumn(
        "event_timestamp",
        F.coalesce(
            F.expr(
                "timestamp_millis(connector_timestamp_ms)"
            ),
            F.col("kafka_timestamp"),
        ),
    )

    enriched = enriched.withColumn(
        "event_date",
        F.to_date(F.col("event_timestamp")),
    )

    enriched = enriched.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                ":",
                F.col("kafka_topic"),
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
            ),
            256,
        ),
    )

    enriched = enriched.withColumn(
        "ingestion_run_id",
        F.lit(run_id),
    )

    enriched = enriched.withColumn(
        "ingested_at",
        F.current_timestamp(),
    )

    enriched = enriched.withColumn(
        "invalid_reason",
        F.when(
            F.col("value_json").isNull(),
            F.lit("null_kafka_value"),
        )
        .when(
            F.col("operation").isNull(),
            F.lit("missing_or_invalid_operation"),
        )
        .when(
            ~F.col("operation").isin(
                *ALLOWED_OPERATIONS
            ),
            F.lit("unsupported_operation"),
        )
        .when(
            ~F.col("source_table").isin(
                *TABLE_TOPICS.keys()
            ),
            F.lit("unexpected_source_table"),
        )
        .otherwise(F.lit(None).cast("string")),
    )

    return enriched.select(
        "event_id",
        "ingestion_run_id",
        "ingested_at",
        "event_date",
        "event_timestamp",
        "source_table",
        "operation",
        "source_timestamp_ms",
        "connector_timestamp_ms",
        "source_lsn",
        "source_transaction_id",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "key_json",
        "before_json",
        "after_json",
        "payload_json",
        "value_json",
        "schema_name",
        "invalid_reason",
    )


def parquet_files_exist(path: str) -> bool:
    root = Path(path)

    return root.exists() and any(
        root.rglob("*.parquet")
    )


def parquet_count(
    spark: SparkSession,
    path: str,
) -> int:
    if not parquet_files_exist(path):
        return 0

    return int(
        spark.read.parquet(path).count()
    )


def query_input_rows(query: object) -> int:
    progress_records = getattr(
        query,
        "recentProgress",
        [],
    )

    return sum(
        int(progress.get("numInputRows", 0))
        for progress in progress_records
    )


def start_file_query(
    dataframe: DataFrame,
    *,
    query_name: str,
    output_path: str,
    checkpoint_path: str,
    partition_columns: list[str],
) -> object:
    writer = (
        dataframe.writeStream
        .queryName(query_name)
        .format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .trigger(availableNow=True)
    )

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    query = writer.start(output_path)
    query.awaitTermination()

    return query


def main() -> None:
    started = time.perf_counter()
    run_id = f"bronze-{uuid.uuid4()}"

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )
    bronze_path = environment_path(
        "BRONZE_PATH",
        "/workspace/data/landing/bronze",
    )
    quarantine_path = environment_path(
        "QUARANTINE_PATH",
        "/workspace/data/quarantine/bronze",
    )
    checkpoint_root = environment_path(
        "CHECKPOINT_PATH",
        "/workspace/data/checkpoints/bronze",
    )

    spark = (
        SparkSession.builder
        .appName("commerce-bronze-stream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_before = parquet_count(
            spark,
            bronze_path,
        )
        quarantine_before = parquet_count(
            spark,
            quarantine_path,
        )

        kafka_events = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                bootstrap_servers,
            )
            .option(
                "subscribe",
                ",".join(TABLE_TOPICS.values()),
            )
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("maxOffsetsPerTrigger", 100_000)
            .load()
        )

        enriched = enrich_kafka_events(
            kafka_events,
            run_id,
        )

        valid_events = enriched.filter(
            F.col("invalid_reason").isNull()
        )
        invalid_events = enriched.filter(
            F.col("invalid_reason").isNotNull()
        )

        valid_query = start_file_query(
            valid_events,
            query_name="commerce_bronze_valid",
            output_path=bronze_path,
            checkpoint_path=(
                f"{checkpoint_root}/valid"
            ),
            partition_columns=[
                "source_table",
                "event_date",
            ],
        )

        invalid_query = start_file_query(
            invalid_events,
            query_name="commerce_bronze_quarantine",
            output_path=quarantine_path,
            checkpoint_path=(
                f"{checkpoint_root}/quarantine"
            ),
            partition_columns=[
                "invalid_reason",
                "event_date",
            ],
        )

        bronze_after = parquet_count(
            spark,
            bronze_path,
        )
        quarantine_after = parquet_count(
            spark,
            quarantine_path,
        )

        bronze_data = spark.read.parquet(
            bronze_path
        )

        table_counts = {
            str(row["source_table"]): int(row["count"])
            for row in (
                bronze_data
                .groupBy("source_table")
                .count()
                .collect()
            )
        }

        operation_counts = {
            str(row["operation"]): int(row["count"])
            for row in (
                bronze_data
                .groupBy("operation")
                .count()
                .collect()
            )
        }

        unique_event_count = int(
            bronze_data
            .select("event_id")
            .distinct()
            .count()
        )
        duplicate_event_ids = (
            bronze_after - unique_event_count
        )

        duration_seconds = (
            time.perf_counter() - started
        )
        new_bronze_events = (
            bronze_after - bronze_before
        )
        new_quarantine_events = (
            quarantine_after - quarantine_before
        )

        valid = (
            bronze_after >= MINIMUM_BASELINE_EVENTS
            and len(table_counts) == len(TABLE_TOPICS)
            and duplicate_event_ids == 0
        )

        result = {
            "status": "valid" if valid else "failed",
            "run_id": run_id,
            "spark_version": spark.version,
            "bootstrap_servers": bootstrap_servers,
            "bronze_path": bronze_path,
            "quarantine_path": quarantine_path,
            "checkpoint_path": checkpoint_root,
            "bronze_before": bronze_before,
            "bronze_after": bronze_after,
            "new_bronze_events": new_bronze_events,
            "quarantine_before": quarantine_before,
            "quarantine_after": quarantine_after,
            "new_quarantine_events": (
                new_quarantine_events
            ),
            "valid_query_input_rows": (
                query_input_rows(valid_query)
            ),
            "quarantine_query_input_rows": (
                query_input_rows(invalid_query)
            ),
            "source_tables": len(table_counts),
            "table_counts": dict(
                sorted(table_counts.items())
            ),
            "operation_counts": dict(
                sorted(operation_counts.items())
            ),
            "unique_event_ids": unique_event_count,
            "duplicate_event_ids": duplicate_event_ids,
            "duration_seconds": round(
                duration_seconds,
                3,
            ),
            "new_events_per_second": round(
                new_bronze_events / duration_seconds,
                2,
            )
            if duration_seconds > 0
            else 0.0,
        }

        print(
            "BRONZE_STREAM_RESULT="
            + json.dumps(result, sort_keys=True)
        )

        if not valid:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()