from __future__ import annotations

import argparse
import json
import time
from collections.abc import Iterable
from typing import Any

from commerce_pipeline.database import connect_postgres
from commerce_pipeline.source_data import (
    SourceDataConfig,
    generate_dataset,
    summarize_dataset,
    validate_dataset,
)


def row_values(
    rows: Iterable[dict[str, Any]],
    columns: tuple[str, ...],
) -> list[tuple[Any, ...]]:
    return [
        tuple(row[column] for column in columns)
        for row in rows
    ]


def database_counts(cursor) -> dict[str, int]:
    tables = (
        "customers",
        "products",
        "orders",
        "order_items",
        "payments",
        "shipments",
    )

    counts: dict[str, int] = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM commerce.{table}")
        counts[table] = int(cursor.fetchone()[0])

    return counts


def seed_database(
    *,
    config: SourceDataConfig,
    reset: bool,
) -> dict[str, object]:
    started = time.perf_counter()

    dataset = generate_dataset(config)
    errors = validate_dataset(dataset)

    if errors:
        raise ValueError(
            "generated dataset failed validation: "
            + "; ".join(errors[:10])
        )

    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            existing_counts = database_counts(cursor)
            existing_rows = sum(existing_counts.values())

            if existing_rows and not reset:
                raise RuntimeError(
                    "source database already contains data; "
                    "use --reset to replace the synthetic baseline"
                )

            if reset:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                        commerce.shipments,
                        commerce.payments,
                        commerce.order_items,
                        commerce.orders,
                        commerce.products,
                        commerce.customers
                    RESTART IDENTITY CASCADE
                    """
                )

            cursor.executemany(
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
                row_values(
                    dataset.customers,
                    (
                        "customer_id",
                        "email",
                        "first_name",
                        "last_name",
                        "customer_status",
                        "city",
                        "state_code",
                        "created_at",
                        "updated_at",
                    ),
                ),
            )

            cursor.executemany(
                """
                INSERT INTO commerce.products (
                    product_id,
                    sku,
                    product_name,
                    category,
                    unit_price,
                    active,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row_values(
                    dataset.products,
                    (
                        "product_id",
                        "sku",
                        "product_name",
                        "category",
                        "unit_price",
                        "active",
                        "created_at",
                        "updated_at",
                    ),
                ),
            )

            cursor.executemany(
                """
                INSERT INTO commerce.orders (
                    order_id,
                    customer_id,
                    order_status,
                    currency,
                    order_total,
                    ordered_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                row_values(
                    dataset.orders,
                    (
                        "order_id",
                        "customer_id",
                        "order_status",
                        "currency",
                        "order_total",
                        "ordered_at",
                        "updated_at",
                    ),
                ),
            )

            cursor.executemany(
                """
                INSERT INTO commerce.order_items (
                    order_item_id,
                    order_id,
                    product_id,
                    quantity,
                    unit_price
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                row_values(
                    dataset.order_items,
                    (
                        "order_item_id",
                        "order_id",
                        "product_id",
                        "quantity",
                        "unit_price",
                    ),
                ),
            )

            cursor.executemany(
                """
                INSERT INTO commerce.payments (
                    payment_id,
                    order_id,
                    payment_status,
                    payment_method,
                    amount,
                    paid_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row_values(
                    dataset.payments,
                    (
                        "payment_id",
                        "order_id",
                        "payment_status",
                        "payment_method",
                        "amount",
                        "paid_at",
                        "created_at",
                        "updated_at",
                    ),
                ),
            )

            cursor.executemany(
                """
                INSERT INTO commerce.shipments (
                    shipment_id,
                    order_id,
                    shipment_status,
                    carrier,
                    tracking_code,
                    shipped_at,
                    delivered_at,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                row_values(
                    dataset.shipments,
                    (
                        "shipment_id",
                        "order_id",
                        "shipment_status",
                        "carrier",
                        "tracking_code",
                        "shipped_at",
                        "delivered_at",
                        "created_at",
                        "updated_at",
                    ),
                ),
            )

            stored_counts = database_counts(cursor)

    summary = summarize_dataset(dataset)
    summary.update(
        {
            "status": "seeded",
            "reset_performed": reset,
            "stored_counts": stored_counts,
            "duration_seconds": round(
                time.perf_counter() - started,
                3,
            ),
        }
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load deterministic commerce data into PostgreSQL"
    )
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--products", type=int, default=250)
    parser.add_argument("--orders", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=20260902)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Replace existing synthetic source data",
    )
    args = parser.parse_args()

    config = SourceDataConfig(
        customer_count=args.customers,
        product_count=args.products,
        order_count=args.orders,
        seed=args.seed,
    )

    try:
        result = seed_database(
            config=config,
            reset=args.reset,
        )
    except (ValueError, RuntimeError) as exc:
        parser.exit(2, f"source seeding failed: {exc}\n")

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()