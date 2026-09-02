from __future__ import annotations

import json
from decimal import Decimal

from commerce_pipeline.database import connect_postgres


TABLES = (
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
)


def scalar(cursor, query: str) -> int:
    cursor.execute(query)
    return int(cursor.fetchone()[0])


def status_counts(
    cursor,
    *,
    table: str,
    column: str,
) -> dict[str, int]:
    cursor.execute(
        f"""
        SELECT {column}, COUNT(*)
        FROM commerce.{table}
        GROUP BY {column}
        ORDER BY {column}
        """
    )
    return {
        str(status): int(count)
        for status, count in cursor.fetchall()
    }


def profile_source() -> dict[str, object]:
    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            table_counts: dict[str, int] = {}
            for table in TABLES:
                table_counts[table] = scalar(
                    cursor,
                    f"SELECT COUNT(*) FROM commerce.{table}",
                )

            integrity = {
                "orders_without_customer": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.orders AS orders
                    LEFT JOIN commerce.customers AS customers
                      ON customers.customer_id = orders.customer_id
                    WHERE customers.customer_id IS NULL
                    """,
                ),
                "items_without_order": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.order_items AS items
                    LEFT JOIN commerce.orders AS orders
                      ON orders.order_id = items.order_id
                    WHERE orders.order_id IS NULL
                    """,
                ),
                "items_without_product": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.order_items AS items
                    LEFT JOIN commerce.products AS products
                      ON products.product_id = items.product_id
                    WHERE products.product_id IS NULL
                    """,
                ),
                "payments_without_order": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.payments AS payments
                    LEFT JOIN commerce.orders AS orders
                      ON orders.order_id = payments.order_id
                    WHERE orders.order_id IS NULL
                    """,
                ),
                "shipments_without_order": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.shipments AS shipments
                    LEFT JOIN commerce.orders AS orders
                      ON orders.order_id = shipments.order_id
                    WHERE orders.order_id IS NULL
                    """,
                ),
                "order_total_mismatches": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.orders AS orders
                    LEFT JOIN (
                        SELECT order_id, SUM(line_total) AS item_total
                        FROM commerce.order_items
                        GROUP BY order_id
                    ) AS items
                      ON items.order_id = orders.order_id
                    WHERE items.item_total IS NULL
                       OR orders.order_total <> items.item_total
                    """,
                ),
                "payment_total_mismatches": scalar(
                    cursor,
                    """
                    SELECT COUNT(*)
                    FROM commerce.orders AS orders
                    LEFT JOIN (
                        SELECT order_id, SUM(amount) AS payment_total
                        FROM commerce.payments
                        GROUP BY order_id
                    ) AS payments
                      ON payments.order_id = orders.order_id
                    WHERE payments.payment_total IS NULL
                       OR orders.order_total <> payments.payment_total
                    """,
                ),
            }

            cursor.execute(
                """
                SELECT
                    COALESCE(SUM(order_total), 0),
                    COALESCE(AVG(order_total), 0)
                FROM commerce.orders
                """
            )
            total_order_value, average_order_value = cursor.fetchone()

            cursor.execute(
                """
                SELECT current_setting('wal_level')
                """
            )
            wal_level = str(cursor.fetchone()[0])

    total_integrity_failures = sum(integrity.values())

    return {
        "status": (
            "valid"
            if total_integrity_failures == 0
            else "invalid"
        ),
        "table_counts": table_counts,
        "integrity": integrity,
        "total_integrity_failures": total_integrity_failures,
        "order_statuses": status_counts_from_database(
            "orders",
            "order_status",
        ),
        "payment_statuses": status_counts_from_database(
            "payments",
            "payment_status",
        ),
        "shipment_statuses": status_counts_from_database(
            "shipments",
            "shipment_status",
        ),
        "total_order_value": format(
            Decimal(total_order_value),
            ".2f",
        ),
        "average_order_value": format(
            Decimal(average_order_value),
            ".2f",
        ),
        "wal_level": wal_level,
    }


def status_counts_from_database(
    table: str,
    column: str,
) -> dict[str, int]:
    with connect_postgres() as connection:
        with connection.cursor() as cursor:
            return status_counts(
                cursor,
                table=table,
                column=column,
            )


def main() -> None:
    print(json.dumps(profile_source(), indent=2))


if __name__ == "__main__":
    main()