from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg


EXPECTED_CURRENT_COUNTS = {
    "customers": 1000,
    "products": 250,
    "orders": 5000,
    "order_items": 12500,
    "payments": 5000,
    "shipments": 2499,
}

EXPECTED_FACT_COUNTS = {
    "fact_orders": 5000,
    "fact_order_items": 12500,
    "fact_payments": 5000,
    "fact_shipments": 2499,
}


def json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    raise TypeError(f"cannot serialize value of type {type(value).__name__}")


def fetch_one(connection: psycopg.Connection, query: str) -> tuple[Any, ...]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError("warehouse profile query returned no result")

    return row


def fetch_count_mapping(
    connection: psycopg.Connection,
    query: str,
) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return {
        str(name): int(record_count)
        for name, record_count in rows
    }


def main() -> None:
    connection_parameters = {
        "host": os.getenv("WAREHOUSE_POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("WAREHOUSE_POSTGRES_PORT", "5434")),
        "dbname": os.getenv("WAREHOUSE_POSTGRES_DB", "analytics"),
        "user": os.getenv("WAREHOUSE_POSTGRES_USER", "warehouse_app"),
        "password": os.getenv(
            "WAREHOUSE_POSTGRES_PASSWORD",
            "warehouse_dev_password",
        ),
    }

    with psycopg.connect(**connection_parameters) as connection:
        (
            raw_record_count,
            unique_raw_event_ids,
            duplicate_raw_event_ids,
            null_required_metadata,
        ) = fetch_one(
            connection,
            """
            select
                count(*) as raw_record_count,
                count(distinct event_id) as unique_event_ids,
                count(*) - count(distinct event_id)
                    as duplicate_event_ids,
                count(*) filter (
                    where event_id is null
                       or source_table is null
                       or operation is null
                       or kafka_topic is null
                       or kafka_partition is null
                       or kafka_offset is null
                ) as null_required_metadata
            from raw.bronze_events
            """,
        )

        warehouse_load_runs = fetch_one(
            connection,
            """
            select count(*)
            from audit.warehouse_load_runs
            """,
        )[0]

        current_state_counts = fetch_count_mapping(
            connection,
            """
            select 'customers', count(*)
            from staging.int_customers_current

            union all

            select 'products', count(*)
            from staging.int_products_current

            union all

            select 'orders', count(*)
            from staging.int_orders_current

            union all

            select 'order_items', count(*)
            from staging.int_order_items_current

            union all

            select 'payments', count(*)
            from staging.int_payments_current

            union all

            select 'shipments', count(*)
            from staging.int_shipments_current
            """,
        )

        (
            snapshot_versions,
            snapshot_customers,
            current_snapshot_versions,
            historical_snapshot_versions,
            deleted_snapshot_versions,
        ) = fetch_one(
            connection,
            """
            select
                count(*) as snapshot_versions,
                count(distinct customer_id) as snapshot_customers,
                count(*) filter (
                    where dbt_valid_to is null
                ) as current_versions,
                count(*) filter (
                    where dbt_valid_to is not null
                ) as historical_versions,
                count(*) filter (
                    where dbt_is_deleted::boolean
                ) as deleted_versions
            from snapshots.snap_customers
            """,
        )

        dimension_counts = fetch_count_mapping(
            connection,
            """
            select 'dim_customers', count(*)
            from analytics.dim_customers

            union all

            select 'dim_products', count(*)
            from analytics.dim_products
            """,
        )

        fact_counts = fetch_count_mapping(
            connection,
            """
            select 'fact_orders', count(*)
            from analytics.fact_orders

            union all

            select 'fact_order_items', count(*)
            from analytics.fact_order_items

            union all

            select 'fact_payments', count(*)
            from analytics.fact_payments

            union all

            select 'fact_shipments', count(*)
            from analytics.fact_shipments
            """,
        )

        (
            total_order_value,
            total_line_value,
            total_payment_value,
            order_item_difference,
            order_payment_difference,
        ) = fetch_one(
            connection,
            """
            with totals as (
                select
                    (
                        select round(sum(order_total), 2)
                        from analytics.fact_orders
                    ) as order_value,

                    (
                        select round(sum(line_total), 2)
                        from analytics.fact_order_items
                    ) as line_value,

                    (
                        select round(sum(amount), 2)
                        from analytics.fact_payments
                    ) as payment_value
            )

            select
                order_value,
                line_value,
                payment_value,
                round(abs(order_value - line_value), 2),
                round(abs(order_value - payment_value), 2)
            from totals
            """,
        )

        (
            reporting_days,
            first_order_date,
            last_order_date,
            daily_order_count,
            daily_payment_count,
            daily_shipment_count,
            daily_order_value,
            daily_payment_value,
        ) = fetch_one(
            connection,
            """
            select
                count(*) as reporting_days,
                min(order_date) as first_order_date,
                max(order_date) as last_order_date,
                sum(order_count) as order_count,
                sum(payment_count) as payment_count,
                sum(shipment_count) as shipment_count,
                round(sum(gross_order_value), 2) as order_value,
                round(sum(total_payment_value), 2) as payment_value
            from analytics.mart_daily_commerce
            """,
        )

        customer_mart_count = fetch_one(
            connection,
            """
            select count(*)
            from analytics.mart_customer_value
            """,
        )[0]

        product_mart_count = fetch_one(
            connection,
            """
            select count(*)
            from analytics.mart_product_performance
            """,
        )[0]

    daily_reconciliation = {
        "order_count_difference": abs(
            int(daily_order_count) - EXPECTED_FACT_COUNTS["fact_orders"]
        ),
        "payment_count_difference": abs(
            int(daily_payment_count) - EXPECTED_FACT_COUNTS["fact_payments"]
        ),
        "shipment_count_difference": abs(
            int(daily_shipment_count) - EXPECTED_FACT_COUNTS["fact_shipments"]
        ),
        "order_value_difference": abs(
            total_order_value - daily_order_value
        ),
        "payment_value_difference": abs(
            total_payment_value - daily_payment_value
        ),
    }

    status_is_valid = all(
        [
            int(duplicate_raw_event_ids) == 0,
            int(null_required_metadata) == 0,
            current_state_counts == EXPECTED_CURRENT_COUNTS,
            fact_counts == EXPECTED_FACT_COUNTS,
            int(current_snapshot_versions)
            == EXPECTED_CURRENT_COUNTS["customers"],
            int(snapshot_versions) >= int(snapshot_customers),
            order_item_difference <= Decimal("0.01"),
            order_payment_difference <= Decimal("0.01"),
            all(
                difference == 0
                for difference in daily_reconciliation.values()
            ),
            int(customer_mart_count)
            == EXPECTED_CURRENT_COUNTS["customers"],
            int(product_mart_count)
            == EXPECTED_CURRENT_COUNTS["products"],
        ]
    )

    result = {
        "status": "valid" if status_is_valid else "failed",
        "warehouse": {
            "host": connection_parameters["host"],
            "port": connection_parameters["port"],
            "database": connection_parameters["dbname"],
            "raw_bronze_records": int(raw_record_count),
            "unique_raw_event_ids": int(unique_raw_event_ids),
            "duplicate_raw_event_ids": int(duplicate_raw_event_ids),
            "null_required_metadata": int(null_required_metadata),
            "warehouse_load_runs": int(warehouse_load_runs),
        },
        "current_state_counts": current_state_counts,
        "customer_snapshot": {
            "total_versions": int(snapshot_versions),
            "distinct_customers": int(snapshot_customers),
            "current_versions": int(current_snapshot_versions),
            "historical_versions": int(historical_snapshot_versions),
            "deleted_versions": int(deleted_snapshot_versions),
        },
        "dimension_counts": dimension_counts,
        "fact_counts": fact_counts,
        "financial_reconciliation": {
            "total_order_value": total_order_value,
            "total_line_value": total_line_value,
            "total_payment_value": total_payment_value,
            "order_item_difference": order_item_difference,
            "order_payment_difference": order_payment_difference,
        },
        "daily_mart": {
            "reporting_days": int(reporting_days),
            "first_order_date": first_order_date,
            "last_order_date": last_order_date,
            "total_orders": int(daily_order_count),
            "total_payments": int(daily_payment_count),
            "total_shipments": int(daily_shipment_count),
            "total_order_value": daily_order_value,
            "total_payment_value": daily_payment_value,
        },
        "daily_reconciliation": daily_reconciliation,
        "reporting_marts": {
            "customer_rows": int(customer_mart_count),
            "product_rows": int(product_mart_count),
        },
    }

    print(
        json.dumps(
            result,
            indent=2,
            sort_keys=True,
            default=json_default,
        )
    )

    if not status_is_valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
