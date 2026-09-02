from __future__ import annotations

import argparse
import json
import os
import time

import psycopg


EXPECTED_TABLES = {
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "shipments",
}


def connection_settings() -> dict[str, object]:
    return {
        "host": os.getenv("COMMERCE_POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("COMMERCE_POSTGRES_PORT", "5433")),
        "dbname": os.getenv("COMMERCE_POSTGRES_DB", "commerce"),
        "user": os.getenv("COMMERCE_POSTGRES_USER", "commerce_app"),
        "password": os.getenv(
            "COMMERCE_POSTGRES_PASSWORD",
            "commerce_dev_password",
        ),
        "connect_timeout": 3,
    }


def inspect_database() -> dict[str, object]:
    with psycopg.connect(**connection_settings()) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_database(), current_user,
                       current_setting('wal_level')
                """
            )
            database, user, wal_level = cursor.fetchone()

            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'commerce'
                ORDER BY table_name
                """
            )
            tables = [row[0] for row in cursor.fetchall()]

    missing_tables = sorted(EXPECTED_TABLES - set(tables))
    if missing_tables:
        raise RuntimeError(
            f"PostgreSQL is available but tables are missing: {missing_tables}"
        )

    return {
        "status": "ready",
        "database": database,
        "user": user,
        "wal_level": wal_level,
        "schema": "commerce",
        "tables": tables,
        "table_count": len(tables),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Wait for the commerce PostgreSQL database"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=90.0,
        help="Maximum number of seconds to wait",
    )
    args = parser.parse_args()

    deadline = time.monotonic() + args.timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            result = inspect_database()
            print(json.dumps(result, indent=2))
            return
        except (psycopg.Error, RuntimeError) as exc:
            last_error = exc
            time.sleep(1)

    raise SystemExit(
        f"PostgreSQL did not become ready within {args.timeout} seconds: "
        f"{last_error}"
    )


if __name__ == "__main__":
    main()