from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg

from commerce_pipeline.reliability import (
    assess_count_reconciliation,
    assess_event_order,
    assess_record_schema,
)


def source_connection_parameters() -> dict[str, Any]:
    return {
        "host": os.getenv(
            "COMMERCE_POSTGRES_HOST",
            "127.0.0.1",
        ),
        "port": int(
            os.getenv("COMMERCE_POSTGRES_PORT", "5433")
        ),
        "dbname": os.getenv(
            "COMMERCE_POSTGRES_DB",
            "commerce",
        ),
        "user": os.getenv(
            "COMMERCE_POSTGRES_USER",
            "commerce_app",
        ),
        "password": os.getenv(
            "COMMERCE_POSTGRES_PASSWORD",
            "commerce_dev_password",
        ),
    }


def fetch_customer_record(
    connection: psycopg.Connection,
) -> tuple[dict[str, Any], int]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select
                to_jsonb(customers),
                (
                    select count(*)
                    from commerce.customers
                )
            from commerce.customers
            order by customer_id
            limit 1
            """
        )
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "no customer record was available for simulation"
        )

    record, customer_count = row

    if not isinstance(record, dict):
        raise RuntimeError(
            "customer record was not decoded as an object"
        )

    return record, int(customer_count)


def main() -> None:
    with psycopg.connect(
        **source_connection_parameters()
    ) as connection:
        customer_record, customer_count = (
            fetch_customer_record(connection)
        )

    unchanged_schema = assess_record_schema(
        "customers",
        customer_record,
    )

    additive_record = {
        **customer_record,
        "loyalty_tier": "gold",
    }
    additive_schema = assess_record_schema(
        "customers",
        additive_record,
    )

    missing_key_record = dict(customer_record)
    missing_key_record.pop("customer_id")
    missing_key_schema = assess_record_schema(
        "customers",
        missing_key_record,
    )

    changed_key_record = {
        **customer_record,
        "customer_id": str(
            customer_record["customer_id"]
        ),
    }
    changed_key_schema = assess_record_schema(
        "customers",
        changed_key_record,
    )

    watermark = datetime.now(timezone.utc)

    on_time_event = assess_event_order(
        watermark + timedelta(seconds=1),
        watermark,
    )
    out_of_order_event = assess_event_order(
        watermark - timedelta(seconds=60),
        watermark,
        allowed_lateness_seconds=300,
    )
    late_event = assess_event_order(
        watermark - timedelta(hours=2),
        watermark,
        allowed_lateness_seconds=300,
    )

    exact_reconciliation = assess_count_reconciliation(
        customer_count,
        customer_count,
    )
    mismatch_reconciliation = (
        assess_count_reconciliation(
            customer_count,
            customer_count - 3,
        )
    )

    checks = {
        "unchanged_schema_is_compatible": (
            unchanged_schema.compatible
            and unchanged_schema.change_type
            == "unchanged"
        ),
        "additive_schema_is_compatible": (
            additive_schema.compatible
            and additive_schema.change_type == "additive"
            and additive_schema.extra_fields
            == ("loyalty_tier",)
        ),
        "missing_primary_key_is_detected": (
            not missing_key_schema.compatible
            and missing_key_schema.reason
            == "missing_primary_key"
        ),
        "primary_key_type_change_is_detected": (
            not changed_key_schema.compatible
            and changed_key_schema.reason
            == "incompatible_primary_key_type"
        ),
        "on_time_event_is_identified": (
            on_time_event.status == "on_time"
        ),
        "out_of_order_event_is_identified": (
            out_of_order_event.status == "out_of_order"
            and not out_of_order_event.requires_backfill
        ),
        "late_event_requires_backfill": (
            late_event.status == "late"
            and late_event.requires_backfill
        ),
        "exact_counts_reconcile": (
            exact_reconciliation.reconciled
        ),
        "count_difference_is_detected": (
            not mismatch_reconciliation.reconciled
            and mismatch_reconciliation.missing_records == 3
        ),
    }

    valid = all(checks.values())

    result = {
        "status": "valid" if valid else "failed",
        "source_table": "customers",
        "source_customer_id": int(
            customer_record["customer_id"]
        ),
        "source_customer_count": customer_count,
        "allowed_lateness_seconds": 300,
        "schema_scenarios": {
            "unchanged": unchanged_schema.as_dict(),
            "compatible_additive": (
                additive_schema.as_dict()
            ),
            "incompatible_missing_key": (
                missing_key_schema.as_dict()
            ),
            "incompatible_key_type": (
                changed_key_schema.as_dict()
            ),
        },
        "event_order_scenarios": {
            "on_time": on_time_event.as_dict(),
            "out_of_order": (
                out_of_order_event.as_dict()
            ),
            "late": late_event.as_dict(),
        },
        "reconciliation_scenarios": {
            "exact": exact_reconciliation.as_dict(),
            "simulated_missing_records": (
                mismatch_reconciliation.as_dict()
            ),
        },
        "checks": checks,
    }

    print(
        "RELIABILITY_SCENARIO_RESULT="
        + json.dumps(result, sort_keys=True)
    )

    if not valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
