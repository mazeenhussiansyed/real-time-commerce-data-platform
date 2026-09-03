from __future__ import annotations

import json
import os
import time
from typing import Any

import psycopg


EXPECTED_SCHEMAS = {
    "raw",
    "staging",
    "snapshots",
    "analytics",
    "audit",
}

EXPECTED_TABLES = {
    ("raw", "bronze_events"),
    ("audit", "warehouse_load_runs"),
}


def connection_settings() -> dict[str, Any]:
    return {
        "host": os.getenv(
            "WAREHOUSE_POSTGRES_HOST",
            "127.0.0.1",
        ),
        "port": int(
            os.getenv(
                "WAREHOUSE_POSTGRES_PORT",
                "5434",
            )
        ),
        "dbname": os.getenv(
            "WAREHOUSE_POSTGRES_DB",
            "analytics",
        ),
        "user": os.getenv(
            "WAREHOUSE_POSTGRES_USER",
            "warehouse_app",
        ),
        "password": os.getenv(
            "WAREHOUSE_POSTGRES_PASSWORD",
            "warehouse_dev_password",
        ),
        "connect_timeout": 3,
    }


def inspect_warehouse() -> dict[str, Any]:
    with psycopg.connect(**connection_settings()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name = ANY(%s)
                ORDER BY schema_name
                """,
                (sorted(EXPECTED_SCHEMAS),),
            )
            schemas = {
                row[0]
                for row in cursor.fetchall()
            }

            cursor.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = ANY(%s)
                ORDER BY table_schema, table_name
                """,
                (sorted(EXPECTED_SCHEMAS),),
            )
            tables = {
                (row[0], row[1])
                for row in cursor.fetchall()
            }

    missing_schemas = sorted(
        EXPECTED_SCHEMAS - schemas
    )
    missing_tables = sorted(
        EXPECTED_TABLES - tables
    )

    return {
        "status": (
            "ready"
            if not missing_schemas and not missing_tables
            else "incomplete"
        ),
        "database": connection_settings()["dbname"],
        "user": connection_settings()["user"],
        "schemas": sorted(schemas),
        "schema_count": len(schemas),
        "tables": [
            f"{schema}.{table}"
            for schema, table in sorted(tables)
        ],
        "missing_schemas": missing_schemas,
        "missing_tables": [
            f"{schema}.{table}"
            for schema, table in missing_tables
        ],
    }


def main() -> None:
    timeout_seconds = 60
    started_at = time.monotonic()
    last_error: str | None = None

    while time.monotonic() - started_at < timeout_seconds:
        try:
            result = inspect_warehouse()

            if result["status"] == "ready":
                print(
                    json.dumps(
                        result,
                        indent=2,
                        sort_keys=True,
                    )
                )
                return

            last_error = json.dumps(result)
        except psycopg.Error as error:
            last_error = str(error)

        time.sleep(2)

    raise SystemExit(
        "warehouse did not become ready within "
        f"{timeout_seconds} seconds: {last_error}"
    )


if __name__ == "__main__":
    main()
