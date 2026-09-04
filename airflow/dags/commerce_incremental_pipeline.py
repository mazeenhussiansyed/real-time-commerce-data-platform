from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

from commerce_pipeline.orchestration import (
    parse_run_configuration,
    record_pipeline_failure,
    record_pipeline_start,
    record_pipeline_success,
)


DAG_ID = "commerce_incremental_pipeline"
PROJECT_HOST_PATH = os.environ.get("COMMERCE_PROJECT_HOST_PATH")

if not PROJECT_HOST_PATH:
    raise RuntimeError(
        "COMMERCE_PROJECT_HOST_PATH is required for Docker mounts."
    )


SPARK_ENVIRONMENT = {
    "HOME": "/tmp",
    "PYTHONPATH": "/workspace/src",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "BRONZE_PATH": "/workspace/data/landing/bronze",
    "QUARANTINE_PATH": "/workspace/data/quarantine/bronze",
    "CHECKPOINT_PATH": "/workspace/data/checkpoints/bronze",
    "WAREHOUSE_POSTGRES_HOST": "warehouse",
    "WAREHOUSE_POSTGRES_PORT": "5432",
    "WAREHOUSE_POSTGRES_DB": "analytics",
    "WAREHOUSE_POSTGRES_USER": "warehouse_app",
    "WAREHOUSE_POSTGRES_PASSWORD": "warehouse_dev_password",
}


SPARK_MOUNTS = [
    Mount(
        source=PROJECT_HOST_PATH,
        target="/workspace",
        type="bind",
        read_only=True,
    ),
    Mount(
        source="commerce-spark-ivy",
        target="/tmp/.ivy2",
        type="volume",
    ),
    Mount(
        source="commerce-bronze-data",
        target="/workspace/data/landing",
        type="volume",
    ),
    Mount(
        source="commerce-quarantine-data",
        target="/workspace/data/quarantine",
        type="volume",
    ),
    Mount(
        source="commerce-spark-checkpoints",
        target="/workspace/data/checkpoints",
        type="volume",
    ),
]


def spark_task(
    task_id: str,
    script_path: str,
    packages: str | None = None,
) -> DockerOperator:
    command = [
        "--master",
        "local[2]",
        "--driver-memory",
        "1g",
    ]

    if packages:
        command.extend(
            [
                "--packages",
                packages,
                "--conf",
                "spark.jars.ivy=/tmp/.ivy2",
            ]
        )

    command.extend(
        [
            "--conf",
            "spark.sql.shuffle.partitions=4",
            script_path,
        ]
    )

    return DockerOperator(
        task_id=task_id,
        image="apache/spark:4.2.0-python3",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-commerce_default",
        api_version="auto",
        entrypoint=["/opt/spark/bin/spark-submit"],
        command=command,
        environment=SPARK_ENVIRONMENT,
        mounts=SPARK_MOUNTS,
        working_dir="/workspace",
        user="0:0",
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=15),
    )


def record_dag_failure(context: dict[str, object]) -> None:
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance")

    if dag_run is None:
        return

    run_id = str(getattr(dag_run, "run_id", "unknown"))
    failed_task_id = str(
        getattr(task_instance, "task_id", "unknown")
    )
    error_message = str(
        context.get("exception") or "Airflow DAG run failed"
    )

    try:
        record_pipeline_failure(
            run_id=run_id,
            failed_task_id=failed_task_id,
            error_message=error_message,
            details={"callback": "dag_failure"},
        )
    except LookupError:
        print(
            f"No audit record exists for failed run {run_id}"
        )
    except Exception as audit_error:
        print(
            f"Unable to record failed run {run_id}: "
            f"{audit_error}"
        )


@dag(
    dag_id=DAG_ID,
    description="Incremental commerce CDC, Bronze and warehouse pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    default_args={
        "owner": "commerce-data-engineering",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["commerce", "cdc", "spark", "dbt", "m05"],
    on_failure_callback=record_dag_failure,
)
def build_commerce_incremental_pipeline():
    @task(
        task_id="start_run_audit",
        execution_timeout=timedelta(minutes=2),
    )
    def start_run_audit() -> dict[str, str | None]:
        context = get_current_context()
        dag_run = context["dag_run"]

        configuration = parse_run_configuration(
            getattr(dag_run, "conf", {}) or {}
        )

        record_pipeline_start(
            run_id=str(dag_run.run_id),
            dag_id=DAG_ID,
            configuration=configuration,
            details={
                "airflow_run_type": str(
                    getattr(dag_run, "run_type", "manual")
                ),
                "orchestrated_task_count": 8,
            },
        )

        return configuration.as_dict()

    @task(
        task_id="complete_run_audit",
        execution_timeout=timedelta(minutes=2),
    )
    def complete_run_audit() -> None:
        context = get_current_context()
        dag_run = context["dag_run"]

        configuration = parse_run_configuration(
            getattr(dag_run, "conf", {}) or {}
        )

        record_pipeline_success(
            run_id=str(dag_run.run_id),
            details={
                "configuration": configuration.as_dict(),
                "completed_by_task": "complete_run_audit",
                "orchestrated_task_count": 8,
            },
        )

    start_audit = start_run_audit()

    validate_services = BashOperator(
        task_id="validate_services",
        bash_command="""
            set -euo pipefail
            cd /opt/airflow/project
            python scripts/wait_for_postgres.py
            python scripts/wait_for_warehouse.py
            python scripts/register_connector.py
            python scripts/profile_cdc.py
        """,
        execution_timeout=timedelta(minutes=5),
    )

    run_bronze_stream = spark_task(
        task_id="run_bronze_stream",
        script_path="/workspace/scripts/run_bronze_stream.py",
        packages=(
            "org.apache.spark:"
            "spark-sql-kafka-0-10_2.13:4.2.0"
        ),
    )

    profile_bronze = spark_task(
        task_id="profile_bronze",
        script_path="/workspace/scripts/profile_bronze.py",
    )

    load_bronze_to_warehouse = spark_task(
        task_id="load_bronze_to_warehouse",
        script_path=(
            "/workspace/scripts/"
            "load_bronze_to_warehouse.py"
        ),
        packages="org.postgresql:postgresql:42.7.13",
    )

    run_dbt_build = DockerOperator(
        task_id="run_dbt_build",
        image="commerce-dbt:1.11",
        docker_url="unix://var/run/docker.sock",
        network_mode="real-time-commerce_default",
        api_version="auto",
        command=(
            """
{% if dag_run.conf.get('run_mode', 'incremental') == 'backfill' %}
build
--vars '{"backfill_start_date":"{{ dag_run.conf.get('backfill_start_date', dag_run.conf.get('start_date')) }}","backfill_end_date":"{{ dag_run.conf.get('backfill_end_date', dag_run.conf.get('end_date')) }}","orchestration_run_id":"{{ run_id }}"}'
{% else %}
build
--vars '{"orchestration_run_id":"{{ run_id }}"}'
{% endif %}
            """
        ),
        environment={
            "WAREHOUSE_POSTGRES_HOST": "warehouse",
            "WAREHOUSE_POSTGRES_PORT": "5432",
            "WAREHOUSE_POSTGRES_DB": "analytics",
            "WAREHOUSE_POSTGRES_USER": "warehouse_app",
            "WAREHOUSE_POSTGRES_PASSWORD": (
                "warehouse_dev_password"
            ),
        },
        mounts=[
            Mount(
                source=f"{PROJECT_HOST_PATH}/dbt",
                target="/workspace/dbt",
                type="bind",
            )
        ],
        working_dir="/workspace/dbt",
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=10),
    )

    profile_warehouse = BashOperator(
        task_id="profile_warehouse",
        bash_command="""
            set -euo pipefail
            cd /opt/airflow/project
            python scripts/profile_warehouse.py
        """,
        execution_timeout=timedelta(minutes=5),
    )

    complete_audit = complete_run_audit()

    (
        start_audit
        >> validate_services
        >> run_bronze_stream
        >> profile_bronze
        >> load_bronze_to_warehouse
        >> run_dbt_build
        >> profile_warehouse
        >> complete_audit
    )


commerce_incremental_pipeline = (
    build_commerce_incremental_pipeline()
)
