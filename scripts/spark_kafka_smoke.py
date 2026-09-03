from __future__ import annotations

import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


TABLE_TOPICS = {
    "customers": "commerce.commerce.customers",
    "products": "commerce.commerce.products",
    "orders": "commerce.commerce.orders",
    "order_items": "commerce.commerce.order_items",
    "payments": "commerce.commerce.payments",
    "shipments": "commerce.commerce.shipments",
}

MINIMUM_EXPECTED_EVENTS = 26_249


def main() -> None:
    started = time.perf_counter()
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:9092",
    )

    spark = (
        SparkSession.builder
        .appName("commerce-kafka-smoke")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        events = (
            spark.read
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
            .option("endingOffsets", "latest")
            .option("failOnDataLoss", "true")
            .load()
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        topic_rows = (
            events.groupBy("topic")
            .count()
            .collect()
        )

        counts_by_topic = {
            str(row["topic"]): int(row["count"])
            for row in topic_rows
        }

        total_events = sum(counts_by_topic.values())
        missing_topics = sorted(
            topic
            for topic in TABLE_TOPICS.values()
            if topic not in counts_by_topic
        )

        distinct_topic_partitions = (
            events.select("topic", "partition")
            .distinct()
            .count()
        )

        valid = (
            not missing_topics
            and total_events >= MINIMUM_EXPECTED_EVENTS
        )

        result = {
            "status": "valid" if valid else "failed",
            "spark_version": spark.version,
            "bootstrap_servers": bootstrap_servers,
            "expected_topics": len(TABLE_TOPICS),
            "observed_topics": len(counts_by_topic),
            "observed_topic_partitions": (
                distinct_topic_partitions
            ),
            "minimum_expected_events": (
                MINIMUM_EXPECTED_EVENTS
            ),
            "total_kafka_events": total_events,
            "missing_topics": missing_topics,
            "topic_event_counts": dict(
                sorted(counts_by_topic.items())
            ),
            "duration_seconds": round(
                time.perf_counter() - started,
                3,
            ),
        }

        print(
            "SPARK_KAFKA_SMOKE_RESULT="
            + json.dumps(result, sort_keys=True)
        )

        if not valid:
            raise SystemExit(1)
    finally:
        try:
            events.unpersist()
        except UnboundLocalError:
            pass

        spark.stop()


if __name__ == "__main__":
    main()