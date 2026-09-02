from __future__ import annotations

import argparse
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from commerce_pipeline.cdc import (
    TABLE_TOPICS,
    create_consumer,
    topic_tail_positions,
)
from commerce_pipeline.database import connect_postgres


CUSTOMER_TOPIC = TABLE_TOPICS["customers"]


def probe_customer_id() -> int:
    return 9_000_000_000_000 + (
        time.time_ns() % 900_000_000_000
    )


def debezium_payload(
    event: dict[str, Any],
) -> dict[str, Any]:
    """Return the Debezium payload with or without schema wrapping."""

    payload = event.get("payload")

    if isinstance(payload, dict):
        return payload

    return event


def customer_id_from_event(
    event: dict[str, Any],
) -> int | None:
    payload = debezium_payload(event)
    record = payload.get("after") or payload.get("before")

    if not isinstance(record, dict):
        return None

    value = record.get("customer_id")
    return int(value) if value is not None else None


def emit_database_changes(customer_id: int) -> None:
    now = datetime.now(timezone.utc)

    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO commerce.customers (
                    customer_id,
                    email,
                    first_name,
                    last_name,
                    customer_status,
                    city,
                    state_code,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    customer_id,
                    f"cdc.probe.{customer_id}@example.test",
                    "CDC",
                    "Probe",
                    "active",
                    "Jersey City",
                    "NJ",
                    now,
                    now,
                ),
            )
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE commerce.customers
                SET city = %s,
                    state_code = %s,
                    customer_status = %s,
                    updated_at = %s
                WHERE customer_id = %s
                """,
                (
                    "New York",
                    "NY",
                    "inactive",
                    datetime.now(timezone.utc),
                    customer_id,
                ),
            )
        connection.commit()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM commerce.customers
                WHERE customer_id = %s
                """,
                (customer_id,),
            )
        connection.commit()


def consume_probe_events(
    customer_id: int,
    *,
    timeout: float,
) -> list[dict[str, object]]:
    consumer = create_consumer(
        f"cdc-operation-probe-{uuid.uuid4()}"
    )

    try:
        tail_positions = topic_tail_positions(
            consumer,
            CUSTOMER_TOPIC,
        )
        consumer.assign(tail_positions)

        # Initialize the explicit partition assignments before writing.
        consumer.poll(0.1)

        emit_database_changes(customer_id)

        deadline = time.monotonic() + timeout
        events: list[dict[str, object]] = []

        while time.monotonic() < deadline:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error() is not None:
                raise RuntimeError(
                    f"Kafka consumption failed: {message.error()}"
                )

            raw_value = message.value()

            # Debezium may generate a null tombstone after a delete.
            if raw_value is None:
                continue

            decoded_event = json.loads(
                raw_value.decode("utf-8")
            )

            if not isinstance(decoded_event, dict):
                continue

            if (
                customer_id_from_event(decoded_event)
                != customer_id
            ):
                continue

            payload = debezium_payload(decoded_event)
            source = payload.get("source")

            if not isinstance(source, dict):
                source = {}

            events.append(
                {
                    "operation": payload.get("op"),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "source_timestamp_ms": source.get("ts_ms"),
                    "connector_timestamp_ms": payload.get("ts_ms"),
                }
            )

            if len(events) == 3:
                break

        return events
    finally:
        consumer.close()


def remaining_probe_rows(customer_id: int) -> int:
    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM commerce.customers
                WHERE customer_id = %s
                """,
                (customer_id,),
            )
            return int(cursor.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify live create, update and delete CDC events"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
    )
    args = parser.parse_args()

    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    customer_id = probe_customer_id()
    started = time.perf_counter()

    try:
        events = consume_probe_events(
            customer_id,
            timeout=args.timeout,
        )
    except (RuntimeError, OSError, ValueError) as exc:
        parser.exit(
            2,
            f"CDC operation verification failed: {exc}\n",
        )

    operations = [
        str(event["operation"])
        for event in events
    ]
    expected_operations = ["c", "u", "d"]
    remaining_rows = remaining_probe_rows(customer_id)

    valid = (
        operations == expected_operations
        and remaining_rows == 0
    )

    result = {
        "status": "verified" if valid else "failed",
        "topic": CUSTOMER_TOPIC,
        "probe_customer_id": customer_id,
        "operations": operations,
        "expected_operations": expected_operations,
        "events_received": len(events),
        "remaining_source_rows": remaining_rows,
        "duration_ms": round(
            (time.perf_counter() - started) * 1000,
            3,
        ),
        "events": events,
    }

    print(json.dumps(result, indent=2))

    if not valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()