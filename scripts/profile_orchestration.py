from __future__ import annotations

import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg
from psycopg.rows import dict_row

from commerce_pipeline.orchestration import MAX_BACKFILL_DAYS


EXPECTED_REPORTING_DAYS = 18
EXPECTED_ORDERS = 5000
EXPECTED_ORDER_VALUE = Decimal("5053882.86")


def json_default(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    raise TypeError(
        f"Object is not JSON serializable: {type(value)}"
    )


def warehouse_connection() -> psycopg.Connection:
    return psycopg.connect(
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


def fetch_one(
    cursor: psycopg.Cursor,
    statement: str,
    parameters: tuple[object, ...] = (),
) -> dict[str, Any]:
    cursor.execute(statement, parameters)
    result = cursor.fetchone()

    if result is None:
        raise RuntimeError(
            "expected the profiling query to return one row"
        )

    return dict(result)


def main() -> None:
    with warehouse_connection() as connection:
        with connection.cursor(
            row_factory=dict_row
        ) as cursor:
            audit_summary = fetch_one(
                cursor,
                """
                SELECT
                    COUNT(*) AS total_runs,

                    COUNT(*) FILTER (
                        WHERE status = 'success'
                    ) AS successful_runs,

                    COUNT(*) FILTER (
                        WHERE status = 'failed'
                    ) AS failed_runs,

                    COUNT(*) FILTER (
                        WHERE status = 'running'
                    ) AS running_runs,

                    COUNT(*) FILTER (
                        WHERE run_mode = 'incremental'
                          AND status = 'success'
                    ) AS successful_incremental_runs,

                    COUNT(*) FILTER (
                        WHERE run_mode = 'backfill'
                          AND status = 'success'
                    ) AS successful_backfill_runs,

                    COUNT(*) FILTER (
                        WHERE run_mode = 'backfill'
                          AND status = 'failed'
                    ) AS failed_backfill_runs
                FROM audit.pipeline_runs
                """,
            )

            audit_integrity = fetch_one(
                cursor,
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE status IN ('success', 'failed')
                          AND (
                              completed_at IS NULL
                              OR duration_seconds IS NULL
                              OR duration_seconds <= 0
                          )
                    ) AS inconsistent_completed_runs,

                    COUNT(*) FILTER (
                        WHERE status = 'failed'
                          AND (
                              failed_task_id IS NULL
                              OR error_message IS NULL
                          )
                    ) AS incomplete_failure_records
                FROM audit.pipeline_runs
                """,
            )

            latest_incremental = fetch_one(
                cursor,
                """
                SELECT
                    run_id,
                    status,
                    duration_seconds,
                    started_at,
                    completed_at
                FROM audit.pipeline_runs
                WHERE run_mode = 'incremental'
                  AND status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
                """,
            )

            latest_backfill = fetch_one(
                cursor,
                """
                SELECT
                    run_id,
                    status,
                    backfill_start_date,
                    backfill_end_date,
                    duration_seconds,
                    started_at,
                    completed_at
                FROM audit.pipeline_runs
                WHERE run_mode = 'backfill'
                  AND status = 'success'
                ORDER BY started_at DESC
                LIMIT 1
                """,
            )

            latest_failure = fetch_one(
                cursor,
                """
                SELECT
                    run_id,
                    run_mode,
                    backfill_start_date,
                    backfill_end_date,
                    duration_seconds,
                    failed_task_id,
                    error_message,
                    started_at,
                    completed_at
                FROM audit.pipeline_runs
                WHERE status = 'failed'
                ORDER BY started_at DESC
                LIMIT 1
                """,
            )

            recovered_failures = fetch_one(
                cursor,
                """
                SELECT COUNT(*) AS recovered_backfill_failures
                FROM audit.pipeline_runs AS failed
                WHERE failed.run_mode = 'backfill'
                  AND failed.status = 'failed'
                  AND EXISTS (
                      SELECT 1
                      FROM audit.pipeline_runs AS succeeded
                      WHERE succeeded.run_mode = 'backfill'
                        AND succeeded.status = 'success'
                        AND succeeded.backfill_start_date
                            = failed.backfill_start_date
                        AND succeeded.backfill_end_date
                            = failed.backfill_end_date
                        AND succeeded.started_at
                            > failed.started_at
                  )
                """,
            )

            mart_summary = fetch_one(
                cursor,
                """
                SELECT
                    COUNT(*) AS reporting_days,
                    COUNT(DISTINCT order_date)
                        AS unique_reporting_days,
                    COUNT(*) - COUNT(DISTINCT order_date)
                        AS duplicate_reporting_days,
                    MIN(order_date) AS first_order_date,
                    MAX(order_date) AS last_order_date,
                    SUM(order_count) AS total_orders,
                    ROUND(SUM(gross_order_value), 2)
                        AS total_order_value
                FROM analytics.mart_daily_commerce
                """,
            )

            latest_backfill_rows = fetch_one(
                cursor,
                """
                SELECT
                    COUNT(*) AS reporting_days,
                    MIN(order_date) AS first_order_date,
                    MAX(order_date) AS last_order_date
                FROM analytics.mart_daily_commerce
                WHERE orchestration_run_id = %s
                """,
                (latest_backfill["run_id"],),
            )

    start_date = latest_backfill[
        "backfill_start_date"
    ]
    end_date = latest_backfill[
        "backfill_end_date"
    ]

    backfill_window_days = (
        end_date - start_date
    ).days + 1

    checks = {
        "successful_incremental_exists": (
            audit_summary[
                "successful_incremental_runs"
            ] >= 1
        ),
        "successful_backfill_exists": (
            audit_summary[
                "successful_backfill_runs"
            ] >= 1
        ),
        "no_running_audit_records": (
            audit_summary["running_runs"] == 0
        ),
        "completed_audit_records_are_consistent": (
            audit_integrity[
                "inconsistent_completed_runs"
            ] == 0
        ),
        "failure_records_identify_tasks": (
            audit_integrity[
                "incomplete_failure_records"
            ] == 0
        ),
        "backfill_window_is_valid": (
            1
            <= backfill_window_days
            <= MAX_BACKFILL_DAYS
        ),
        "reporting_dates_are_unique": (
            mart_summary[
                "duplicate_reporting_days"
            ] == 0
        ),
        "reporting_day_count_reconciles": (
            mart_summary["reporting_days"]
            == EXPECTED_REPORTING_DAYS
        ),
        "order_count_reconciles": (
            mart_summary["total_orders"]
            == EXPECTED_ORDERS
        ),
        "order_value_reconciles": (
            mart_summary["total_order_value"]
            == EXPECTED_ORDER_VALUE
        ),
    }

    report = {
        "status": (
            "valid"
            if all(checks.values())
            else "failed"
        ),
        "audit_summary": audit_summary,
        "audit_integrity": audit_integrity,
        "latest_successful_incremental": (
            latest_incremental
        ),
        "latest_successful_backfill": (
            latest_backfill
        ),
        "latest_failed_run": latest_failure,
        "recovery": recovered_failures,
        "backfill_window_days": backfill_window_days,
        "latest_backfill_mart_rows": (
            latest_backfill_rows
        ),
        "daily_mart": mart_summary,
        "maximum_backfill_days": MAX_BACKFILL_DAYS,
        "checks": checks,
    }

    print(
        json.dumps(
            report,
            indent=2,
            sort_keys=True,
            default=json_default,
        )
    )

    if report["status"] != "valid":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
