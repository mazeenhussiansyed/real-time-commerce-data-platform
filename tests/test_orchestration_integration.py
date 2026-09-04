from __future__ import annotations

import os
import unittest
from datetime import date
from decimal import Decimal

import psycopg


RUN_INTEGRATION = (
    os.getenv("RUN_ORCHESTRATION_INTEGRATION") == "1"
)


@unittest.skipUnless(
    RUN_INTEGRATION,
    "Set RUN_ORCHESTRATION_INTEGRATION=1 "
    "to run orchestration integration tests.",
)
class OrchestrationIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.connection = psycopg.connect(
            host=os.getenv(
                "WAREHOUSE_POSTGRES_HOST",
                "127.0.0.1",
            ),
            port=int(
                os.getenv(
                    "WAREHOUSE_POSTGRES_PORT",
                    "5434",
                )
            ),
            dbname=os.getenv(
                "WAREHOUSE_POSTGRES_DB",
                "analytics",
            ),
            user=os.getenv(
                "WAREHOUSE_POSTGRES_USER",
                "warehouse_app",
            ),
            password=os.getenv(
                "WAREHOUSE_POSTGRES_PASSWORD",
                "warehouse_dev_password",
            ),
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.connection.close()

    def fetch_one(
        self,
        statement: str,
    ) -> tuple:
        with self.connection.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchone()

        self.assertIsNotNone(result)
        return result

    def test_successful_incremental_and_backfill_runs_exist(
        self,
    ) -> None:
        result = self.fetch_one(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE run_mode = 'incremental'
                      AND status = 'success'
                ),
                COUNT(*) FILTER (
                    WHERE run_mode = 'backfill'
                      AND status = 'success'
                )
            FROM audit.pipeline_runs
            """
        )

        self.assertGreaterEqual(result[0], 1)
        self.assertGreaterEqual(result[1], 1)

    def test_latest_successful_backfill_is_valid(
        self,
    ) -> None:
        result = self.fetch_one(
            """
            SELECT
                backfill_start_date,
                backfill_end_date,
                duration_seconds,
                failed_task_id
            FROM audit.pipeline_runs
            WHERE run_mode = 'backfill'
              AND status = 'success'
            ORDER BY started_at DESC
            LIMIT 1
            """
        )

        self.assertEqual(result[0], date(2026, 1, 5))
        self.assertEqual(result[1], date(2026, 1, 7))
        self.assertGreater(result[2], Decimal("0"))
        self.assertIsNone(result[3])

    def test_completed_audit_rows_are_consistent(
        self,
    ) -> None:
        result = self.fetch_one(
            """
            SELECT COUNT(*)
            FROM audit.pipeline_runs
            WHERE status IN ('success', 'failed')
              AND (
                  completed_at IS NULL
                  OR duration_seconds IS NULL
                  OR duration_seconds <= 0
              )
            """
        )

        self.assertEqual(result[0], 0)

    def test_failed_runs_identify_the_failed_task(
        self,
    ) -> None:
        result = self.fetch_one(
            """
            SELECT COUNT(*)
            FROM audit.pipeline_runs
            WHERE status = 'failed'
              AND (
                  failed_task_id IS NULL
                  OR error_message IS NULL
              )
            """
        )

        self.assertEqual(result[0], 0)

    def test_daily_mart_remains_unique_and_reconciled(
        self,
    ) -> None:
        result = self.fetch_one(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT order_date),
                SUM(order_count),
                ROUND(SUM(gross_order_value), 2)
            FROM analytics.mart_daily_commerce
            """
        )

        self.assertEqual(result[0], 18)
        self.assertEqual(result[1], 18)
        self.assertEqual(result[2], 5000)
        self.assertEqual(
            result[3],
            Decimal("5053882.86"),
        )


if __name__ == "__main__":
    unittest.main()
