from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


EXPECTED_TABLES = {
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
}

REQUIRED_METADATA_COLUMNS = (
    "event_id",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "source_table",
    "operation",
    "event_timestamp",
    "ingestion_run_id",
    "ingested_at",
)


def validate_columns(
    dataframe: DataFrame,
    required_columns: tuple[str, ...],
) -> None:
    missing_columns = sorted(
        set(required_columns) - set(dataframe.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "Bronze data is missing required columns: "
            + ", ".join(missing_columns)
        )


def grouped_counts(
    dataframe: DataFrame,
    column_name: str,
) -> dict[str, int]:
    rows = (
        dataframe.groupBy(column_name)
        .count()
        .orderBy(column_name)
        .collect()
    )

    return {
        str(row[column_name]): int(row["count"])
        for row in rows
    }


def count_null_metadata(dataframe: DataFrame) -> dict[str, int]:
    expressions = [
        F.sum(
            F.when(F.col(column_name).isNull(), 1).otherwise(0)
        ).alias(column_name)
        for column_name in REQUIRED_METADATA_COLUMNS
    ]

    row = dataframe.agg(*expressions).first()

    return {
        column_name: int(row[column_name] or 0)
        for column_name in REQUIRED_METADATA_COLUMNS
    }


def read_optional_parquet(
    spark: SparkSession,
    path: Path,
) -> DataFrame | None:
    if not path.exists():
        return None

    if not any(path.rglob("*.parquet")):
        return None

    return spark.read.parquet(str(path))


def calculate_connector_latency(
    bronze: DataFrame,
) -> dict[str, float | int | None]:
    eligible = (
        bronze.filter(F.col("operation").isin("c", "u", "d"))
        .filter(F.col("source_timestamp_ms").isNotNull())
        .filter(F.col("connector_timestamp_ms").isNotNull())
        .withColumn(
            "connector_latency_ms",
            F.col("connector_timestamp_ms")
            - F.col("source_timestamp_ms"),
        )
        .filter(F.col("connector_latency_ms") >= 0)
    )

    measured_events = eligible.count()

    if measured_events == 0:
        return {
            "measured_events": 0,
            "median_ms": None,
            "p95_ms": None,
            "maximum_ms": None,
        }

    row = eligible.agg(
        F.expr(
            "percentile_approx(connector_latency_ms, 0.50)"
        ).alias("median_ms"),
        F.expr(
            "percentile_approx(connector_latency_ms, 0.95)"
        ).alias("p95_ms"),
        F.max("connector_latency_ms").alias("maximum_ms"),
    ).first()

    return {
        "measured_events": measured_events,
        "median_ms": float(row["median_ms"]),
        "p95_ms": float(row["p95_ms"]),
        "maximum_ms": float(row["maximum_ms"]),
    }


def main() -> None:
    bronze_path = Path(
        os.getenv(
            "BRONZE_PATH",
            "/workspace/data/landing/bronze",
        )
    )
    quarantine_path = Path(
        os.getenv(
            "QUARANTINE_PATH",
            "/workspace/data/quarantine/bronze",
        )
    )

    spark = (
        SparkSession.builder
        .appName("commerce-bronze-profile")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        bronze = read_optional_parquet(spark, bronze_path)

        if bronze is None:
            raise RuntimeError(
                f"no Bronze Parquet records found at {bronze_path}"
            )

        validate_columns(bronze, REQUIRED_METADATA_COLUMNS)

        bronze_records = bronze.count()
        unique_event_ids = (
            bronze.select("event_id").distinct().count()
        )
        duplicate_event_ids = (
            bronze_records - unique_event_ids
        )

        table_counts = grouped_counts(
            bronze,
            "source_table",
        )
        operation_counts = grouped_counts(
            bronze,
            "operation",
        )

        null_metadata = count_null_metadata(bronze)
        total_null_metadata = sum(null_metadata.values())

        source_tables = set(table_counts)
        missing_tables = sorted(
            EXPECTED_TABLES - source_tables
        )
        unexpected_tables = sorted(
            source_tables - EXPECTED_TABLES
        )

        topic_count = (
            bronze.select("kafka_topic").distinct().count()
        )
        topic_partition_count = (
            bronze.select(
                "kafka_topic",
                "kafka_partition",
            )
            .distinct()
            .count()
        )

        event_date_count = (
            bronze.select("event_date").distinct().count()
        )
        table_date_partition_count = (
            bronze.select(
                "source_table",
                "event_date",
            )
            .distinct()
            .count()
        )

        quarantine = read_optional_parquet(
            spark,
            quarantine_path,
        )

        if quarantine is None:
            quarantine_records = 0
            quarantine_reason_counts: dict[str, int] = {}
            quarantine_unique_event_ids = 0
            quarantine_duplicate_event_ids = 0
        else:
            quarantine_records = quarantine.count()

            quarantine_reason_counts = grouped_counts(
                quarantine,
                "invalid_reason",
            )

            quarantine_unique_event_ids = (
                quarantine.select("event_id")
                .distinct()
                .count()
            )

            quarantine_duplicate_event_ids = (
                quarantine_records
                - quarantine_unique_event_ids
            )

        connector_latency = calculate_connector_latency(
            bronze
        )

        valid = (
            bronze_records >= 26249
            and source_tables == EXPECTED_TABLES
            and duplicate_event_ids == 0
            and total_null_metadata == 0
            and quarantine_records >= 1
            and quarantine_duplicate_event_ids == 0
        )

        result: dict[str, Any] = {
            "status": "valid" if valid else "failed",
            "spark_version": spark.version,
            "bronze_path": str(bronze_path),
            "bronze_records": bronze_records,
            "unique_event_ids": unique_event_ids,
            "duplicate_event_ids": duplicate_event_ids,
            "source_table_count": len(source_tables),
            "table_counts": table_counts,
            "operation_counts": operation_counts,
            "topic_count": topic_count,
            "topic_partition_count": (
                topic_partition_count
            ),
            "event_date_count": event_date_count,
            "table_date_partition_count": (
                table_date_partition_count
            ),
            "null_metadata": null_metadata,
            "total_null_metadata": total_null_metadata,
            "missing_tables": missing_tables,
            "unexpected_tables": unexpected_tables,
            "quarantine_path": str(quarantine_path),
            "quarantine_records": quarantine_records,
            "quarantine_reason_counts": (
                quarantine_reason_counts
            ),
            "quarantine_unique_event_ids": (
                quarantine_unique_event_ids
            ),
            "quarantine_duplicate_event_ids": (
                quarantine_duplicate_event_ids
            ),
            "connector_latency": connector_latency,
        }

        print(
            "BRONZE_PROFILE_RESULT="
            + json.dumps(result, sort_keys=True)
        )

        if not valid:
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
