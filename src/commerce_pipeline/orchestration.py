from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


VALID_RUN_MODES = {"incremental", "backfill"}
MAX_BACKFILL_DAYS = 31


@dataclass(frozen=True)
class RunConfiguration:
    run_mode: str
    backfill_start_date: date | None
    backfill_end_date: date | None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "run_mode": self.run_mode,
            "backfill_start_date": (
                self.backfill_start_date.isoformat()
                if self.backfill_start_date
                else None
            ),
            "backfill_end_date": (
                self.backfill_end_date.isoformat()
                if self.backfill_end_date
                else None
            ),
        }


def _parse_date(
    value: object,
    field_name: str,
) -> date | None:
    if value is None or value == "":
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must use YYYY-MM-DD format"
        ) from exc


def parse_run_configuration(
    conf: Mapping[str, object] | None,
) -> RunConfiguration:
    values = dict(conf or {})

    run_mode = str(
        values.get("run_mode", "incremental")
    ).strip().lower()

    if run_mode not in VALID_RUN_MODES:
        raise ValueError(
            "run_mode must be either incremental or backfill"
        )

    start_value = values.get(
        "backfill_start_date",
        values.get("start_date"),
    )
    end_value = values.get(
        "backfill_end_date",
        values.get("end_date"),
    )

    start_date = _parse_date(
        start_value,
        "backfill_start_date",
    )
    end_date = _parse_date(
        end_value,
        "backfill_end_date",
    )

    if run_mode == "incremental":
        if start_date is not None or end_date is not None:
            raise ValueError(
                "incremental runs must not include backfill dates"
            )

        return RunConfiguration(
            run_mode="incremental",
            backfill_start_date=None,
            backfill_end_date=None,
        )

    if start_date is None or end_date is None:
        raise ValueError(
            "backfill runs require both start_date and end_date"
        )

    if start_date > end_date:
        raise ValueError(
            "backfill start_date must not be after end_date"
        )

    backfill_days = (end_date - start_date).days + 1

    if backfill_days > MAX_BACKFILL_DAYS:
        raise ValueError(
            f"backfill window cannot exceed "
            f"{MAX_BACKFILL_DAYS} days"
        )

    return RunConfiguration(
        run_mode="backfill",
        backfill_start_date=start_date,
        backfill_end_date=end_date,
    )


def _warehouse_connection() -> psycopg.Connection:
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


def record_pipeline_start(
    run_id: str,
    dag_id: str,
    configuration: RunConfiguration,
    details: Mapping[str, Any] | None = None,
) -> None:
    if not run_id.strip():
        raise ValueError("run_id cannot be empty")

    if not dag_id.strip():
        raise ValueError("dag_id cannot be empty")

    statement = """
        INSERT INTO audit.pipeline_runs (
            run_id,
            dag_id,
            run_mode,
            backfill_start_date,
            backfill_end_date,
            status,
            started_at,
            completed_at,
            duration_seconds,
            failed_task_id,
            error_message,
            details,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            'running',
            CURRENT_TIMESTAMP,
            NULL,
            NULL,
            NULL,
            NULL,
            %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (run_id)
        DO UPDATE SET
            dag_id = EXCLUDED.dag_id,
            run_mode = EXCLUDED.run_mode,
            backfill_start_date =
                EXCLUDED.backfill_start_date,
            backfill_end_date =
                EXCLUDED.backfill_end_date,
            status = 'running',
            started_at = CURRENT_TIMESTAMP,
            completed_at = NULL,
            duration_seconds = NULL,
            failed_task_id = NULL,
            error_message = NULL,
            details = EXCLUDED.details,
            updated_at = CURRENT_TIMESTAMP
    """

    with _warehouse_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    run_id,
                    dag_id,
                    configuration.run_mode,
                    configuration.backfill_start_date,
                    configuration.backfill_end_date,
                    Jsonb(dict(details or {})),
                ),
            )


def record_pipeline_success(
    run_id: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    statement = """
        UPDATE audit.pipeline_runs
        SET
            status = 'success',
            completed_at = CURRENT_TIMESTAMP,
            duration_seconds = ROUND(
                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP - started_at
                    )
                )::NUMERIC,
                3
            ),
            failed_task_id = NULL,
            error_message = NULL,
            details = details || %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
    """

    with _warehouse_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    Jsonb(dict(details or {})),
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise LookupError(
                    f"pipeline run was not found: {run_id}"
                )


def record_pipeline_failure(
    run_id: str,
    failed_task_id: str,
    error_message: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    statement = """
        UPDATE audit.pipeline_runs
        SET
            status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            duration_seconds = ROUND(
                EXTRACT(
                    EPOCH FROM (
                        CURRENT_TIMESTAMP - started_at
                    )
                )::NUMERIC,
                3
            ),
            failed_task_id = %s,
            error_message = %s,
            details = details || %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = %s
    """

    safe_error_message = str(error_message)[:4000]

    with _warehouse_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                statement,
                (
                    failed_task_id,
                    safe_error_message,
                    Jsonb(dict(details or {})),
                    run_id,
                ),
            )

            if cursor.rowcount != 1:
                raise LookupError(
                    f"pipeline run was not found: {run_id}"
                )


def fetch_pipeline_run(
    run_id: str,
) -> dict[str, Any] | None:
    statement = """
        SELECT
            run_id,
            dag_id,
            run_mode,
            backfill_start_date,
            backfill_end_date,
            status,
            started_at,
            completed_at,
            duration_seconds,
            failed_task_id,
            error_message,
            details
        FROM audit.pipeline_runs
        WHERE run_id = %s
    """

    with _warehouse_connection() as connection:
        with connection.cursor(
            row_factory=dict_row
        ) as cursor:
            cursor.execute(statement, (run_id,))
            result = cursor.fetchone()

    return dict(result) if result else None
