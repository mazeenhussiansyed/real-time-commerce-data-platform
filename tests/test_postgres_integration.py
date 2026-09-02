from __future__ import annotations

import os
import unittest

from commerce_pipeline.database import connect_postgres


RUN_INTEGRATION = os.getenv("RUN_POSTGRES_INTEGRATION") == "1"


@unittest.skipUnless(
    RUN_INTEGRATION,
    "Set RUN_POSTGRES_INTEGRATION=1 to run PostgreSQL tests.",
)
class PostgreSQLIntegrationTests(unittest.TestCase):
    def scalar(self, query: str) -> int:
        with connect_postgres() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                return int(cursor.fetchone()[0])

    def test_source_tables_have_expected_counts(self) -> None:
        expected_counts = {
            "customers": 1_000,
            "products": 250,
            "orders": 5_000,
            "order_items": 12_500,
            "payments": 5_000,
            "shipments": 2_499,
        }

        for table, expected in expected_counts.items():
            with self.subTest(table=table):
                actual = self.scalar(
                    f"SELECT COUNT(*) FROM commerce.{table}"
                )
                self.assertEqual(actual, expected)

    def test_foreign_key_relationships_have_no_orphans(self) -> None:
        queries = {
            "orders_without_customer": """
                SELECT COUNT(*)
                FROM commerce.orders AS orders
                LEFT JOIN commerce.customers AS customers
                  ON customers.customer_id = orders.customer_id
                WHERE customers.customer_id IS NULL
            """,
            "items_without_order": """
                SELECT COUNT(*)
                FROM commerce.order_items AS items
                LEFT JOIN commerce.orders AS orders
                  ON orders.order_id = items.order_id
                WHERE orders.order_id IS NULL
            """,
            "items_without_product": """
                SELECT COUNT(*)
                FROM commerce.order_items AS items
                LEFT JOIN commerce.products AS products
                  ON products.product_id = items.product_id
                WHERE products.product_id IS NULL
            """,
        }

        for check_name, query in queries.items():
            with self.subTest(check=check_name):
                self.assertEqual(self.scalar(query), 0)

    def test_order_totals_match_order_items(self) -> None:
        mismatches = self.scalar(
            """
            SELECT COUNT(*)
            FROM commerce.orders AS orders
            JOIN (
                SELECT order_id, SUM(line_total) AS item_total
                FROM commerce.order_items
                GROUP BY order_id
            ) AS items
              ON items.order_id = orders.order_id
            WHERE orders.order_total <> items.item_total
            """
        )
        self.assertEqual(mismatches, 0)

    def test_postgresql_uses_logical_wal(self) -> None:
        with connect_postgres() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT current_setting('wal_level')"
                )
                wal_level = str(cursor.fetchone()[0])

        self.assertEqual(wal_level, "logical")


if __name__ == "__main__":
    unittest.main()