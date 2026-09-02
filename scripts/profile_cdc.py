from __future__ import annotations

import argparse
import json
import os
import uuid
from typing import Any
from urllib.request import urlopen

from commerce_pipeline.cdc import (
    TABLE_TOPICS,
    create_consumer,
    wait_for_minimum_counts,
)
from commerce_pipeline.database import connect_postgres


def source_counts() -> dict[str, int]:
    counts: dict[str, int] = {}

    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            for table in TABLE_TOPICS:
                cursor.execute(
                    f"SELECT COUNT(*) FROM commerce.{table}"
                )
                counts[table] = int(cursor.fetchone()[0])

    return counts


def connector_status() -> dict[str, Any]:
    base_url = os.getenv(
        "KAFKA_CONNECT_URL",
        "http://127.0.0.1:8083",
    ).rstrip("/")
    url = (
        f"{base_url}/connectors/"
        "commerce-postgres-cdc/status"
    )

    with urlopen(url, timeout=10) as response:
        return json.loads(
            response.read().decode("utf-8")
        )


def replication_state() -> dict[str, object]:
    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    active,
                    restart_lsn::TEXT,
                    confirmed_flush_lsn::TEXT
                FROM pg_replication_slots
                WHERE slot_name = 'commerce_debezium_slot'
                """
            )
            slot_row = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM pg_publication_tables
                WHERE pubname = 'commerce_debezium_publication'
                  AND schemaname = 'commerce'
                """
            )
            publication_table_count = int(
                cursor.fetchone()[0]
            )

    if slot_row is None:
        slot = {
            "exists": False,
            "active": False,
            "restart_lsn": None,
            "confirmed_flush_lsn": None,
        }
    else:
        slot = {
            "exists": True,
            "active": bool(slot_row[0]),
            "restart_lsn": slot_row[1],
            "confirmed_flush_lsn": slot_row[2],
        }

    return {
        "slot": slot,
        "publication_table_count": publication_table_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile PostgreSQL-to-Kafka CDC state"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
    )
    args = parser.parse_args()

    expected = source_counts()
    consumer = create_consumer(
        f"cdc-profile-{uuid.uuid4()}"
    )

    try:
        topic_counts = wait_for_minimum_counts(
            consumer,
            expected,
            timeout=args.timeout,
        )
    finally:
        consumer.close()

    differences = {
        table: topic_counts[table] - expected[table]
        for table in expected
    }
    total_source_rows = sum(expected.values())
    total_topic_events = sum(topic_counts.values())

    status = connector_status()
    connector = status.get("connector", {})
    tasks = status.get("tasks", [])
    replication = replication_state()

    connector_running = (
        connector.get("state") == "RUNNING"
        and len(tasks) > 0
        and all(
            task.get("state") == "RUNNING"
            for task in tasks
        )
    )
    slot = replication["slot"]

    valid = (
        connector_running
        and bool(slot["exists"])
        and bool(slot["active"])
        and replication["publication_table_count"] == 6
        and all(
            topic_counts[table] >= expected[table]
            for table in expected
        )
    )

    result = {
        "status": "valid" if valid else "invalid",
        "connector_state": connector.get("state"),
        "task_states": [
            task.get("state")
            for task in tasks
        ],
        "source_counts": expected,
        "topic_event_counts": topic_counts,
        "topic_minus_current_rows": differences,
        "total_source_rows": total_source_rows,
        "total_topic_events": total_topic_events,
        "minimum_completeness_rate": round(
            min(
                topic_counts[table] / expected[table]
                for table in expected
            ),
            6,
        ),
        "exact_initial_snapshot_match": all(
            difference == 0
            for difference in differences.values()
        ),
        "replication": replication,
    }

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()