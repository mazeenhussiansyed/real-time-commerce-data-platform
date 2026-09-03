from __future__ import annotations

import os
import unittest
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


@unittest.skipUnless(
    os.getenv("RUN_WAREHOUSE_INTEGRATION") == "1",
    "Set RUN_WAREHOUSE_INTEGRATION=1 to run warehouse integration tests.",
)
class WarehouseIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.connection = psycopg.connect(
            host=os.getenv("WAREHOUSE_POSTGRES_HOST", "127.0.0.1"),
            port=int(os.getenv("WAREHOUSE_POSTGRES_PORT", "5434")),
            dbname=os.getenv("WAREHOUSE_POSTGRES_DB", "analytics"),
            user=os.getenv("WAREHOUSE_POSTGRES_USER", "warehouse_app"),
            password=os.getenv(
                "WAREHOUSE_POSTGRES_PASSWORD",
                "warehouse_dev_password",
            ),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()

    def fetch_one(self, query: str) -> tuple[Any, ...]:
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

        self.assertIsNotNone(row)
        return row  # type: ignore[return-value]

    def fetch_count_mapping(self, query: str) -> dict[str, int]:
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        return {
            str(name): int(record_count)
            for name, record_count in rows
        }

    def test_raw_warehouse_events_are_unique_and_complete(self) -> None:
        (
            total_records,
            unique_event_ids,
            null_required_metadata,
        ) = self.fetch_one(
            """
            select
                count(*),
                count(distinct event_id),
                count(*) filter (
                    where event_id is null
                       or source_table is null
                       or operation is null
                       or kafka_topic is null
                       or kafka_partition is null
                       or kafka_offset is null
                )
            from raw.bronze_events
            """
        )

        self.assertGreater(int(total_records), 0)
        self.assertEqual(int(total_records), int(unique_event_ids))
        self.assertEqual(int(null_required_metadata), 0)

    def test_current_state_counts_match_source_baseline(self) -> None:
        actual_counts = self.fetch_count_mapping(
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
            """
        )

        self.assertEqual(actual_counts, EXPECTED_CURRENT_COUNTS)

    def test_customer_snapshot_has_valid_scd2_history(self) -> None:
        (
            total_versions,
            distinct_customers,
            latest_versions,
        ) = self.fetch_one(
            """
            select
                count(*),
                count(distinct customer_id),
                count(*) filter (
                    where dbt_valid_to is null
                )
            from snapshots.snap_customers
            """
        )

        overlapping_versions = self.fetch_one(
            """
            with ordered_versions as (
                select
                    customer_id,
                    dbt_valid_from,
                    dbt_valid_to,

                    lead(dbt_valid_from) over (
                        partition by customer_id
                        order by dbt_valid_from
                    ) as next_valid_from

                from snapshots.snap_customers
            )

            select count(*)
            from ordered_versions
            where next_valid_from is not null
              and (
                  dbt_valid_to is null
                  or dbt_valid_to > next_valid_from
              )
            """
        )[0]

        self.assertGreaterEqual(
            int(total_versions),
            int(distinct_customers),
        )
        self.assertEqual(
            int(latest_versions),
            int(distinct_customers),
        )
        self.assertEqual(int(overlapping_versions), 0)

    def test_fact_counts_and_financial_totals_reconcile(self) -> None:
        actual_fact_counts = self.fetch_count_mapping(
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
            """
        )

        (
            order_value,
            line_value,
            payment_value,
        ) = self.fetch_one(
            """
            select
                (
                    select round(sum(order_total), 2)
                    from analytics.fact_orders
                ),
                (
                    select round(sum(line_total), 2)
                    from analytics.fact_order_items
                ),
                (
                    select round(sum(amount), 2)
                    from analytics.fact_payments
                )
            """
        )

        self.assertEqual(actual_fact_counts, EXPECTED_FACT_COUNTS)
        self.assertEqual(order_value, Decimal("5053882.86"))
        self.assertEqual(order_value, line_value)
        self.assertEqual(order_value, payment_value)

    def test_reporting_marts_reconcile_with_facts(self) -> None:
        (
            reporting_days,
            daily_orders,
            daily_payments,
            daily_shipments,
            daily_order_value,
            daily_payment_value,
        ) = self.fetch_one(
            """
            select
                count(*),
                sum(order_count),
                sum(payment_count),
                sum(shipment_count),
                round(sum(gross_order_value), 2),
                round(sum(total_payment_value), 2)
            from analytics.mart_daily_commerce
            """
        )

        customer_mart_rows = self.fetch_one(
            """
            select count(*)
            from analytics.mart_customer_value
            """
        )[0]

        product_mart_rows = self.fetch_one(
            """
            select count(*)
            from analytics.mart_product_performance
            """
        )[0]

        self.assertEqual(int(reporting_days), 18)
        self.assertEqual(int(daily_orders), 5000)
        self.assertEqual(int(daily_payments), 5000)
        self.assertEqual(int(daily_shipments), 2499)
        self.assertEqual(daily_order_value, Decimal("5053882.86"))
        self.assertEqual(daily_payment_value, Decimal("5053882.86"))
        self.assertEqual(int(customer_mart_rows), 1000)
        self.assertEqual(int(product_mart_rows), 250)

    def test_warehouse_load_audit_exists(self) -> None:
        load_run_count = self.fetch_one(
            """
            select count(*)
            from audit.warehouse_load_runs
            """
        )[0]

        self.assertGreaterEqual(int(load_run_count), 1)


if __name__ == "__main__":
    unittest.main()
