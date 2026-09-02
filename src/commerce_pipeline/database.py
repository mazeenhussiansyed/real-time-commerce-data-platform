from __future__ import annotations

import os

import psycopg
from psycopg import Connection


def postgres_settings() -> dict[str, object]:
    return {
        "host": os.getenv("COMMERCE_POSTGRES_HOST", "127.0.0.1"),
        "port": int(os.getenv("COMMERCE_POSTGRES_PORT", "5433")),
        "dbname": os.getenv("COMMERCE_POSTGRES_DB", "commerce"),
        "user": os.getenv("COMMERCE_POSTGRES_USER", "commerce_app"),
        "password": os.getenv(
            "COMMERCE_POSTGRES_PASSWORD",
            "commerce_dev_password",
        ),
        "connect_timeout": 5,
    }


def connect_postgres() -> Connection:
    return psycopg.connect(**postgres_settings())