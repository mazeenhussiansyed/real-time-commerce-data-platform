from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
from confluent_kafka.admin import AdminClient

from commerce_pipeline.reliability import (
    assess_count_reconciliation,
    freshness_seconds,
)


EXPECTED_TOPICS = {
    "commerce.commerce.customers",
    "commerce.commerce.products",
    "commerce.commerce.orders",
    "commerce.commerce.order_items",
    "commerce.commerce.payments",
    "commerce.commerce.shipments",
}

FRESHNESS_SLO_SECONDS = int(
    os.getenv("RELIABILITY_FRESHNESS_SLO_SECONDS", "86400")
)


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    raise TypeError(
        f"cannot serialize value of type "
        f"{type(value).__name__}"
    )


def database_parameters(prefix: str) -> dict[str, Any]:
    if prefix == "COMMERCE":
        defaults = {
            "host": "127.0.0.1",
            "port": "5433",
            "database": "commerce",
            "user": "commerce_app",
            "password": "commerce_dev_password",
        }
    elif prefix == "WAREHOUSE":
        defaults = {
            "host": "127.0.0.1",
            "port": "5434",
            "database": "analytics",
            "user": "warehouse_app",
            "password": "warehouse_dev_password",
        }
    else:
        raise ValueError(
            f"unsupported database prefix: {prefix}"
        )

    return {
        "host": os.getenv(
            f"{prefix}_POSTGRES_HOST",
            defaults["host"],
        ),
        "port": int(
            os.getenv(
                f"{prefix}_POSTGRES_PORT",
                defaults["port"],
            )
        ),
        "dbname": os.getenv(
            f"{prefix}_POSTGRES_DB",
            defaults["database"],
        ),
        "user": os.getenv(
            f"{prefix}_POSTGRES_USER",
            defaults["user"],
        ),
        "password": os.getenv(
            f"{prefix}_POSTGRES_PASSWORD",
            defaults["password"],
        ),
    }


def fetch_one(
    connection: psycopg.Connection,
    query: str,
) -> tuple[Any, ...]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            "reliability query returned no result"
        )

    return tuple(row)


def fetch_counts(
    connection: psycopg.Connection,
    query: str,
) -> dict[str, int]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return {
        str(table_name): int(record_count)
        for table_name, record_count in rows
    }


def http_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "commerce-reliability-profile",
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=10,
    ) as response:
        payload = json.loads(
            response.read().decode("utf-8")
        )

    if not isinstance(payload, dict):
        raise RuntimeError(
            f"health endpoint did not return an object: {url}"
        )

    return payload


def fetch_live_bronze_profile() -> dict[str, Any]:
    command = [
        "docker",
        "compose",
        "--profile",
        "spark",
        "run",
        "--rm",
        "spark",
        "--master",
        "local[2]",
        "--driver-memory",
        "1g",
        "--conf",
        "spark.sql.shuffle.partitions=4",
        "/workspace/scripts/profile_bronze.py",
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )

    if completed.returncode != 0:
        diagnostic = (
            completed.stdout
            + "\n"
            + completed.stderr
        )[-4000:]

        raise RuntimeError(
            "live Bronze profiling failed:\n"
            + diagnostic
        )

    result_prefix = "BRONZE_PROFILE_RESULT="

    for line in completed.stdout.splitlines():
        prefix_position = line.find(result_prefix)

        if prefix_position == -1:
            continue

        result = json.loads(
            line[
                prefix_position
                + len(result_prefix):
            ]
        )

        if not isinstance(result, dict):
            raise RuntimeError(
                "Bronze profile result was not an object"
            )

        return result

    raise RuntimeError(
        "BRONZE_PROFILE_RESULT was not found "
        "in Spark profile output"
    )


def main() -> None:
    checked_at = datetime.now(timezone.utc)
    bronze_profile = fetch_live_bronze_profile()
    live_bronze_event_count = int(
        bronze_profile["bronze_records"]
    )

    with psycopg.connect(
        **database_parameters("COMMERCE")
    ) as source_connection:
        source_counts = fetch_counts(
            source_connection,
            """
            select 'customers', count(*)
            from commerce.customers

            union all

            select 'products', count(*)
            from commerce.products

            union all

            select 'orders', count(*)
            from commerce.orders

            union all

            select 'order_items', count(*)
            from commerce.order_items

            union all

            select 'payments', count(*)
            from commerce.payments

            union all

            select 'shipments', count(*)
            from commerce.shipments
            """,
        )

    with psycopg.connect(
        **database_parameters("WAREHOUSE")
    ) as warehouse_connection:
        warehouse_current_counts = fetch_counts(
            warehouse_connection,
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
            """,
        )

        (
            warehouse_event_count,
            unique_event_ids,
            duplicate_event_ids,
            null_required_metadata,
            latest_bronze_ingested_at,
            latest_warehouse_loaded_at,
        ) = fetch_one(
            warehouse_connection,
            """
            select
                count(*) as warehouse_event_count,
                count(distinct event_id)
                    as unique_event_ids,
                count(*) - count(distinct event_id)
                    as duplicate_event_ids,
                count(*) filter (
                    where event_id is null
                       or source_table is null
                       or operation is null
                       or kafka_topic is null
                       or kafka_partition is null
                       or kafka_offset is null
                       or ingested_at is null
                       or warehouse_loaded_at is null
                ) as null_required_metadata,
                max(ingested_at),
                max(warehouse_loaded_at)
            from raw.bronze_events
            """,
        )

        (
            latest_load_run_id,
            latest_bronze_event_count,
            latest_load_completed_at,
        ) = fetch_one(
            warehouse_connection,
            """
            select
                run_id,
                source_record_count,
                completed_at
            from audit.warehouse_load_runs
            where status = 'succeeded'
              and completed_at is not null
            order by completed_at desc
            limit 1
            """,
        )

        (
            successful_pipeline_runs,
            failed_pipeline_runs,
            latest_pipeline_success_at,
        ) = fetch_one(
            warehouse_connection,
            """
            select
                count(*) filter (
                    where status = 'success'
                ),
                count(*) filter (
                    where status = 'failed'
                ),
                max(completed_at) filter (
                    where status = 'success'
                )
            from audit.pipeline_runs
            """,
        )

        (
            measured_latency_events,
            average_connector_latency_ms,
            maximum_connector_latency_ms,
            average_bronze_processing_ms,
            maximum_bronze_processing_ms,
            average_warehouse_load_ms,
            maximum_warehouse_load_ms,
            average_end_to_end_ms,
            maximum_end_to_end_ms,
            observed_out_of_order_events,
        ) = fetch_one(
            warehouse_connection,
            """
            with measurements as (
                select
                    greatest(
                        connector_timestamp_ms
                        - source_timestamp_ms,
                        0
                    )::numeric
                        as connector_latency_ms,

                    greatest(
                        extract(
                            epoch from (
                                ingested_at
                                - to_timestamp(
                                    connector_timestamp_ms
                                    / 1000.0
                                )
                            )
                        ) * 1000,
                        0
                    )::numeric
                        as bronze_processing_ms,

                    greatest(
                        extract(
                            epoch from (
                                warehouse_loaded_at
                                - ingested_at
                            )
                        ) * 1000,
                        0
                    )::numeric
                        as warehouse_load_ms,

                    greatest(
                        extract(
                            epoch from (
                                warehouse_loaded_at
                                - to_timestamp(
                                    source_timestamp_ms
                                    / 1000.0
                                )
                            )
                        ) * 1000,
                        0
                    )::numeric
                        as end_to_end_ms
                from raw.bronze_events
                where source_timestamp_ms is not null
                  and connector_timestamp_ms is not null
            ),

            ordered_events as (
                select
                    connector_timestamp_ms,
                    lag(connector_timestamp_ms) over (
                        partition by
                            kafka_topic,
                            kafka_partition
                        order by kafka_offset
                    ) as previous_connector_timestamp_ms
                from raw.bronze_events
                where connector_timestamp_ms is not null
            )

            select
                count(*),
                round(
                    avg(connector_latency_ms),
                    3
                ),
                max(connector_latency_ms),
                round(
                    avg(bronze_processing_ms),
                    3
                ),
                max(bronze_processing_ms),
                round(
                    avg(warehouse_load_ms),
                    3
                ),
                max(warehouse_load_ms),
                round(
                    avg(end_to_end_ms),
                    3
                ),
                max(end_to_end_ms),
                (
                    select count(*)
                    from ordered_events
                    where connector_timestamp_ms
                        < previous_connector_timestamp_ms
                )
            from measurements
            """,
        )

    connect_url = os.getenv(
        "KAFKA_CONNECT_URL",
        "http://127.0.0.1:8083",
    ).rstrip("/")
    connector_name = os.getenv(
        "DEBEZIUM_CONNECTOR_NAME",
        "commerce-postgres-cdc",
    )

    connector_status = http_json(
        f"{connect_url}/connectors/"
        f"{connector_name}/status"
    )

    connector_task_states = [
        str(task.get("state"))
        for task in connector_status.get("tasks", [])
        if isinstance(task, dict)
    ]
    connector_is_healthy = (
        connector_status.get("connector", {}).get("state")
        == "RUNNING"
        and bool(connector_task_states)
        and all(
            state == "RUNNING"
            for state in connector_task_states
        )
    )

    airflow_url = os.getenv(
        "AIRFLOW_HEALTH_URL",
        "http://127.0.0.1:8080"
        "/api/v2/monitor/health",
    )
    airflow_health = http_json(airflow_url)

    required_airflow_components = (
        "metadatabase",
        "scheduler",
        "dag_processor",
    )
    airflow_component_states = {
        component: (
            airflow_health.get(component, {}).get("status")
        )
        for component in required_airflow_components
    }
    airflow_is_healthy = all(
        state == "healthy"
        for state in airflow_component_states.values()
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "127.0.0.1:29092",
    )
    kafka_metadata = AdminClient(
        {"bootstrap.servers": bootstrap_servers}
    ).list_topics(timeout=10)

    available_topics = set(kafka_metadata.topics)
    missing_topics = sorted(
        EXPECTED_TOPICS - available_topics
    )
    kafka_is_healthy = not missing_topics

    current_state_reconciliation = {
        table_name: assess_count_reconciliation(
            source_count,
            warehouse_current_counts.get(
                table_name,
                0,
            ),
        )
        for table_name, source_count
        in source_counts.items()
    }

    event_reconciliation = assess_count_reconciliation(
        live_bronze_event_count,
        int(warehouse_event_count),
    )

    warehouse_freshness_seconds = freshness_seconds(
        latest_warehouse_loaded_at,
        checked_at=checked_at,
    )
    pipeline_success_age_seconds = freshness_seconds(
        latest_pipeline_success_at,
        checked_at=checked_at,
    )

    checks = {
        "live_bronze_profile_is_valid": (
            bronze_profile.get("status") == "valid"
        ),
        "source_to_bronze_current_state_reconciles": all(
            assessment.reconciled
            for assessment
            in current_state_reconciliation.values()
        ),
        "bronze_to_warehouse_events_reconcile": (
            event_reconciliation.reconciled
        ),
        "warehouse_event_ids_are_unique": (
            int(duplicate_event_ids) == 0
            and int(unique_event_ids)
            == int(warehouse_event_count)
        ),
        "required_metadata_is_complete": (
            int(null_required_metadata) == 0
        ),
        "connector_is_healthy": connector_is_healthy,
        "kafka_topics_are_healthy": kafka_is_healthy,
        "airflow_is_healthy": airflow_is_healthy,
        "successful_pipeline_run_exists": (
            int(successful_pipeline_runs) >= 1
        ),
        "warehouse_data_meets_freshness_slo": (
            warehouse_freshness_seconds
            <= FRESHNESS_SLO_SECONDS
        ),
        "pipeline_meets_freshness_slo": (
            pipeline_success_age_seconds
            <= FRESHNESS_SLO_SECONDS
        ),
        "latency_measurements_exist": (
            int(measured_latency_events) > 0
        ),
    }

    valid = all(checks.values())

    result = {
        "status": "valid" if valid else "failed",
        "checked_at": checked_at,
        "freshness_slo_seconds": (
            FRESHNESS_SLO_SECONDS
        ),
        "service_health": {
            "postgres_source": "healthy",
            "postgres_warehouse": "healthy",
            "debezium_connector": (
                connector_status.get(
                    "connector",
                    {},
                ).get("state")
            ),
            "debezium_tasks": connector_task_states,
            "kafka": (
                "healthy"
                if kafka_is_healthy
                else "failed"
            ),
            "airflow": airflow_component_states,
        },
        "kafka": {
            "bootstrap_servers": bootstrap_servers,
            "expected_topic_count": len(
                EXPECTED_TOPICS
            ),
            "missing_topics": missing_topics,
        },
        "current_state_counts": {
            "source": source_counts,
            "bronze_derived_warehouse_state": (
                warehouse_current_counts
            ),
        },
        "current_state_reconciliation": {
            table_name: assessment.as_dict()
            for table_name, assessment
            in current_state_reconciliation.items()
        },
        "event_reconciliation": {
            "latest_load_run_id": latest_load_run_id,
            "latest_load_source_record_count": int(
                latest_bronze_event_count
            ),
            "live_bronze_event_count": (
                live_bronze_event_count
            ),
            "warehouse_event_count": int(
                warehouse_event_count
            ),
            **event_reconciliation.as_dict(),
        },
        "bronze_storage": {
            "status": bronze_profile.get("status"),
            "bronze_records": live_bronze_event_count,
            "quarantine_records": int(
                bronze_profile["quarantine_records"]
            ),
            "quarantine_reason_counts": {
                str(reason): int(record_count)
                for reason, record_count
                in bronze_profile[
                    "quarantine_reason_counts"
                ].items()
            },
        },
        "warehouse_quality": {
            "unique_event_ids": int(
                unique_event_ids
            ),
            "duplicate_event_ids": int(
                duplicate_event_ids
            ),
            "null_required_metadata": int(
                null_required_metadata
            ),
        },
        "freshness": {
            "latest_bronze_ingested_at": (
                latest_bronze_ingested_at
            ),
            "latest_warehouse_loaded_at": (
                latest_warehouse_loaded_at
            ),
            "latest_load_completed_at": (
                latest_load_completed_at
            ),
            "latest_pipeline_success_at": (
                latest_pipeline_success_at
            ),
            "warehouse_data_age_seconds": (
                warehouse_freshness_seconds
            ),
            "pipeline_success_age_seconds": (
                pipeline_success_age_seconds
            ),
        },
        "latency_ms": {
            "measured_events": int(
                measured_latency_events
            ),
            "connector_average": (
                average_connector_latency_ms
            ),
            "connector_maximum": (
                maximum_connector_latency_ms
            ),
            "bronze_processing_average": (
                average_bronze_processing_ms
            ),
            "bronze_processing_maximum": (
                maximum_bronze_processing_ms
            ),
            "warehouse_load_average": (
                average_warehouse_load_ms
            ),
            "warehouse_load_maximum": (
                maximum_warehouse_load_ms
            ),
            "end_to_end_average": (
                average_end_to_end_ms
            ),
            "end_to_end_maximum": (
                maximum_end_to_end_ms
            ),
        },
        "event_order_baseline": {
            "observed_out_of_order_events": int(
                observed_out_of_order_events
            ),
        },
        "pipeline_audit": {
            "successful_runs": int(
                successful_pipeline_runs
            ),
            "failed_runs": int(
                failed_pipeline_runs
            ),
        },
        "checks": checks,
    }

    print(
        "RELIABILITY_PROFILE_RESULT="
        + json.dumps(
            result,
            sort_keys=True,
            default=json_default,
        )
    )

    if not valid:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
