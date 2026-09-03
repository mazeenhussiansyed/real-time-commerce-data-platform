from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TARGET_COLUMNS = (
    "event_id",
    "ingestion_run_id",
    "ingested_at",
    "event_timestamp",
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
    "source_table",
    "event_date",
)


def jdbc_options() -> dict[str, str]:
    return {
        "url": os.getenv(
            "WAREHOUSE_JDBC_URL",
            "jdbc:postgresql://warehouse:5432/analytics",
        ),
        "user": os.getenv(
            "WAREHOUSE_POSTGRES_USER",
            "warehouse_app",
        ),
        "password": os.getenv(
            "WAREHOUSE_POSTGRES_PASSWORD",
            "warehouse_dev_password",
        ),
        "driver": "org.postgresql.Driver",
    }


def validate_columns(dataframe: DataFrame) -> None:
    missing_columns = sorted(
        set(TARGET_COLUMNS) - set(dataframe.columns)
    )

    if missing_columns:
        raise RuntimeError(
            "Bronze data is missing required columns: "
            + ", ".join(missing_columns)
        )


def read_jdbc_query(
    spark: SparkSession,
    query: str,
) -> DataFrame:
    options = jdbc_options()

    return (
        spark.read.format("jdbc")
        .option("url", options["url"])
        .option("dbtable", f"({query}) AS warehouse_query")
        .option("user", options["user"])
        .option("password", options["password"])
        .option("driver", options["driver"])
        .load()
    )


def write_jdbc_table(
    dataframe: DataFrame,
    table_name: str,
) -> None:
    options = jdbc_options()

    (
        dataframe.write.format("jdbc")
        .option("url", options["url"])
        .option("dbtable", table_name)
        .option("user", options["user"])
        .option("password", options["password"])
        .option("driver", options["driver"])
        .option("batchsize", "1000")
        .mode("append")
        .save()
    )


def count_target_records(
    spark: SparkSession,
) -> int:
    row = read_jdbc_query(
        spark,
        """
        SELECT COUNT(*) AS record_count
        FROM raw.bronze_events
        """,
    ).first()

    return int(row["record_count"])


def count_target_duplicates(
    spark: SparkSession,
) -> int:
    row = read_jdbc_query(
        spark,
        """
        SELECT
            COUNT(*) - COUNT(DISTINCT event_id)
                AS duplicate_count
        FROM raw.bronze_events
        """,
    ).first()

    return int(row["duplicate_count"])


def write_success_audit(
    spark: SparkSession,
    *,
    run_id: str,
    started_at: datetime,
    completed_at: datetime,
    source_record_count: int,
    inserted_record_count: int,
    existing_record_count: int,
) -> None:
    audit_record = (
        spark.range(1)
        .select(
            F.lit(run_id).alias("run_id"),
            F.lit("bronze_to_warehouse").alias(
                "pipeline_name"
            ),
            F.lit("succeeded").alias("status"),
            F.lit(started_at).cast("timestamp").alias(
                "started_at"
            ),
            F.lit(completed_at).cast("timestamp").alias(
                "completed_at"
            ),
            F.lit(source_record_count)
            .cast("long")
            .alias("source_record_count"),
            F.lit(inserted_record_count)
            .cast("long")
            .alias("inserted_record_count"),
            F.lit(existing_record_count)
            .cast("long")
            .alias("existing_record_count"),
            F.lit(0)
            .cast("long")
            .alias("failed_record_count"),
        )
    )

    write_jdbc_table(
        audit_record,
        "audit.warehouse_load_runs",
    )


def main() -> None:
    bronze_path = Path(
        os.getenv(
            "BRONZE_PATH",
            "/workspace/data/landing/bronze",
        )
    )

    if not bronze_path.exists():
        raise SystemExit(
            f"Bronze path does not exist: {bronze_path}"
        )

    run_id = f"warehouse-{uuid.uuid4()}"
    started_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )
    started_timer = time.perf_counter()

    spark = (
        SparkSession.builder
        .appName("commerce-bronze-to-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        bronze = spark.read.parquet(str(bronze_path))
        validate_columns(bronze)

        source_events = (
            bronze.select(*TARGET_COLUMNS)
            .dropDuplicates(["event_id"])
            .cache()
        )

        source_record_count = source_events.count()
        original_bronze_count = bronze.count()
        source_duplicate_event_ids = (
            original_bronze_count - source_record_count
        )

        existing_ids = read_jdbc_query(
            spark,
            """
            SELECT event_id
            FROM raw.bronze_events
            """,
        )

        new_events = (
            source_events.join(
                existing_ids,
                on="event_id",
                how="left_anti",
            )
            .cache()
        )

        inserted_record_count = new_events.count()
        existing_record_count = (
            source_record_count - inserted_record_count
        )

        if inserted_record_count > 0:
            write_jdbc_table(
                new_events.select(*TARGET_COLUMNS),
                "raw.bronze_events",
            )

        target_record_count = count_target_records(spark)
        target_duplicate_event_ids = (
            count_target_duplicates(spark)
        )

        target_ids = read_jdbc_query(
            spark,
            """
            SELECT event_id
            FROM raw.bronze_events
            """,
        )

        missing_target_records = (
            source_events.select("event_id")
            .join(
                target_ids,
                on="event_id",
                how="left_anti",
            )
            .count()
        )

        completed_at = datetime.now(timezone.utc).replace(
            tzinfo=None
        )

        write_success_audit(
            spark,
            run_id=run_id,
            started_at=started_at,
            completed_at=completed_at,
            source_record_count=source_record_count,
            inserted_record_count=inserted_record_count,
            existing_record_count=existing_record_count,
        )

        duration_seconds = (
            time.perf_counter() - started_timer
        )

        inserted_per_second = (
            inserted_record_count / duration_seconds
            if duration_seconds > 0
            else 0.0
        )

        valid = (
            source_record_count >= 26249
            and source_duplicate_event_ids == 0
            and missing_target_records == 0
            and target_duplicate_event_ids == 0
            and (
                inserted_record_count
                + existing_record_count
                == source_record_count
            )
        )

        result: dict[str, Any] = {
            "status": "valid" if valid else "failed",
            "run_id": run_id,
            "spark_version": spark.version,
            "bronze_path": str(bronze_path),
            "warehouse_jdbc_url": jdbc_options()["url"],
            "source_record_count": source_record_count,
            "source_duplicate_event_ids": (
                source_duplicate_event_ids
            ),
            "inserted_record_count": (
                inserted_record_count
            ),
            "existing_record_count": (
                existing_record_count
            ),
            "target_record_count": target_record_count,
            "target_duplicate_event_ids": (
                target_duplicate_event_ids
            ),
            "missing_target_records": (
                missing_target_records
            ),
            "duration_seconds": round(
                duration_seconds,
                3,
            ),
            "inserted_records_per_second": round(
                inserted_per_second,
                2,
            ),
        }

        print(
            "WAREHOUSE_LOAD_RESULT="
            + json.dumps(result, sort_keys=True)
        )

        if not valid:
            raise SystemExit(1)
    finally:
        try:
            source_events.unpersist()
        except UnboundLocalError:
            pass

        try:
            new_events.unpersist()
        except UnboundLocalError:
            pass

        spark.stop()


if __name__ == "__main__":
    main()
