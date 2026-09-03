from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any


EXPECTED_SOURCE_TABLES = frozenset(
    {
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    }
)

ALLOWED_OPERATIONS = frozenset({"r", "c", "u", "d"})


@dataclass(frozen=True)
class BronzeEventClassification:
    valid: bool
    reason: str | None
    operation: str | None
    source_table: str | None
    record: dict[str, Any] | None


def build_event_id(
    topic: str,
    partition: int,
    offset: int,
) -> str:
    if not topic.strip():
        raise ValueError("topic cannot be empty")

    if partition < 0:
        raise ValueError("partition cannot be negative")

    if offset < 0:
        raise ValueError("offset cannot be negative")

    event_position = f"{topic}:{partition}:{offset}"
    return hashlib.sha256(
        event_position.encode("utf-8")
    ).hexdigest()


def classify_debezium_value(
    raw_value: str,
) -> BronzeEventClassification:
    try:
        decoded = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return BronzeEventClassification(
            valid=False,
            reason="invalid_json",
            operation=None,
            source_table=None,
            record=None,
        )

    if not isinstance(decoded, dict):
        return BronzeEventClassification(
            valid=False,
            reason="payload_not_object",
            operation=None,
            source_table=None,
            record=None,
        )

    envelope = decoded.get("payload", decoded)

    if not isinstance(envelope, dict):
        return BronzeEventClassification(
            valid=False,
            reason="missing_payload",
            operation=None,
            source_table=None,
            record=None,
        )

    operation_value = envelope.get("op")
    operation = (
        str(operation_value)
        if operation_value is not None
        else None
    )

    source = envelope.get("source")
    source_table: str | None = None

    if isinstance(source, dict):
        table_value = source.get("table")
        if table_value is not None:
            source_table = str(table_value)

    if operation is None:
        return BronzeEventClassification(
            valid=False,
            reason="missing_operation",
            operation=None,
            source_table=source_table,
            record=None,
        )

    if operation not in ALLOWED_OPERATIONS:
        return BronzeEventClassification(
            valid=False,
            reason="unsupported_operation",
            operation=operation,
            source_table=source_table,
            record=None,
        )

    if source_table is None:
        return BronzeEventClassification(
            valid=False,
            reason="missing_source_table",
            operation=operation,
            source_table=None,
            record=None,
        )

    if source_table not in EXPECTED_SOURCE_TABLES:
        return BronzeEventClassification(
            valid=False,
            reason="unexpected_source_table",
            operation=operation,
            source_table=source_table,
            record=None,
        )

    record_field = (
        "before"
        if operation == "d"
        else "after"
    )
    record = envelope.get(record_field)

    if not isinstance(record, dict):
        return BronzeEventClassification(
            valid=False,
            reason=f"missing_{record_field}_record",
            operation=operation,
            source_table=source_table,
            record=None,
        )

    return BronzeEventClassification(
        valid=True,
        reason=None,
        operation=operation,
        source_table=source_table,
        record=record,
    )
