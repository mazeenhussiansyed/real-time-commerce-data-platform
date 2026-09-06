from __future__ import annotations

import argparse
import json
import os
import subprocess
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from commerce_pipeline.deployment import (
    assess_compose_model,
    assess_container_state,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]

def build_compose_command() -> list[str]:
    command = [
        "docker",
        "compose",
    ]

    project_name = os.getenv(
        "COMMERCE_COMPOSE_PROJECT_NAME"
    )

    if project_name:
        command.extend(["-p", project_name])

    command.extend(
        [
            "-f",
            "compose.yaml",
            "-f",
            "compose.airflow.yaml",
        ]
    )

    override_file = os.getenv(
        "COMMERCE_COMPOSE_OVERRIDE_FILE"
    )

    if override_file:
        command.extend(["-f", override_file])

    command.extend(["--profile", "spark"])

    return command


COMPOSE_COMMAND = build_compose_command()

LIVE_SERVICES = (
    "postgres",
    "warehouse",
    "kafka",
    "connect",
    "airflow-db",
    "airflow-apiserver",
    "airflow-scheduler",
    "airflow-dag-processor",
)

REQUIRED_PATHS = (
    ".env.example",
    ".github/workflows/ci.yml",
    "Dockerfile.airflow",
    "Dockerfile.dbt",
    "compose.airflow.yaml",
    "compose.yaml",
    "airflow/dags/commerce_incremental_pipeline.py",
    "dbt/dbt_project.yml",
    "scripts/run_bronze_stream.py",
    "scripts/load_bronze_to_warehouse.py",
    "scripts/profile_reliability.py",
)

EXPECTED_DAG_TASKS = {
    "start_run_audit",
    "validate_services",
    "run_bronze_stream",
    "profile_bronze",
    "load_bronze_to_warehouse",
    "run_dbt_build",
    "profile_warehouse",
    "complete_run_audit",
}


def run_command(
    arguments: list[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        arguments,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )

    if check and completed.returncode != 0:
        diagnostic = (
            completed.stdout
            + "\n"
            + completed.stderr
        )[-4000:]

        raise RuntimeError(
            "Command failed: "
            + " ".join(arguments)
            + "\n"
            + diagnostic
        )

    return completed


def http_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": (
                "commerce-deployment-validator"
            ),
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=15,
    ) as response:
        result = json.loads(
            response.read().decode("utf-8")
        )

    if not isinstance(result, dict):
        raise RuntimeError(
            f"Endpoint did not return an object: {url}"
        )

    return result


def load_compose_model() -> dict[str, Any]:
    completed = run_command(
        COMPOSE_COMMAND
        + [
            "config",
            "--format",
            "json",
        ]
    )

    model = json.loads(completed.stdout)

    if not isinstance(model, dict):
        raise RuntimeError(
            "Rendered Compose configuration "
            "was not an object"
        )

    return model


def inspect_container(
    container_name: str,
) -> dict[str, Any]:
    completed = run_command(
        [
            "docker",
            "inspect",
            container_name,
        ]
    )

    inspection = json.loads(completed.stdout)

    if (
        not isinstance(inspection, list)
        or not inspection
        or not isinstance(inspection[0], dict)
    ):
        raise RuntimeError(
            "Docker inspection returned an "
            f"invalid result for {container_name}"
        )

    state = inspection[0].get("State")

    if not isinstance(state, dict):
        raise RuntimeError(
            "Docker inspection did not contain "
            f"state for {container_name}"
        )

    return state


def inspect_service_container(
    service_name: str,
) -> dict[str, Any]:
    completed = run_command(
        COMPOSE_COMMAND
        + [
            "ps",
            "-q",
            service_name,
        ]
    )
    container_id = completed.stdout.strip()

    if not container_id:
        raise RuntimeError(
            "Compose did not return a container "
            f"for service {service_name}"
        )

    return inspect_container(container_id)


def configuration_result() -> dict[str, Any]:
    docker_version = run_command(
        [
            "docker",
            "version",
            "--format",
            "{{.Server.Version}}",
        ]
    ).stdout.strip()

    compose_version = run_command(
        [
            "docker",
            "compose",
            "version",
            "--short",
        ]
    ).stdout.strip()

    compose_assessment = assess_compose_model(
        load_compose_model()
    )

    required_paths = {
        relative_path: (
            PROJECT_ROOT / relative_path
        ).exists()
        for relative_path in REQUIRED_PATHS
    }

    checks = {
        "docker_is_available": bool(
            docker_version
        ),
        "compose_is_available": bool(
            compose_version
        ),
        "compose_contract_is_valid": (
            compose_assessment.valid
        ),
        "required_paths_exist": all(
            required_paths.values()
        ),
    }

    return {
        "docker_server_version": docker_version,
        "compose_version": compose_version,
        "compose": compose_assessment.as_dict(),
        "required_paths": required_paths,
        "checks": checks,
    }


def live_result() -> dict[str, Any]:
    container_states = {
        service_name: assess_container_state(
            inspect_service_container(
                service_name
            )
        )
        for service_name in LIVE_SERVICES
    }

    airflow_health_url = os.getenv(
        "AIRFLOW_HEALTH_URL",
        "http://127.0.0.1:8080/"
        "api/v2/monitor/health",
    )
    airflow_health = http_json(
        airflow_health_url
    )

    airflow_is_healthy = all(
        airflow_health.get(component, {}).get(
            "status"
        )
        == "healthy"
        for component in (
            "metadatabase",
            "scheduler",
            "dag_processor",
        )
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

    connector_is_healthy = (
        connector_status.get(
            "connector",
            {},
        ).get("state")
        == "RUNNING"
        and bool(connector_status.get("tasks"))
        and all(
            task.get("state") == "RUNNING"
            for task in connector_status.get(
                "tasks",
                []
            )
        )
    )

    import_result = run_command(
        COMPOSE_COMMAND
        + [
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "list-import-errors",
        ],
        check=False,
    )

    dag_imports_are_clean = (
        import_result.returncode == 0
        and "No data found"
        in import_result.stdout
    )

    task_result = run_command(
        COMPOSE_COMMAND
        + [
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "tasks",
            "list",
            "commerce_incremental_pipeline",
        ]
    )

    dag_tasks = {
        task_name.strip()
        for task_name in task_result.stdout.splitlines()
        if task_name.strip()
    }

    image_names = {
        "airflow": os.getenv(
            "AIRFLOW_IMAGE",
            "commerce-airflow:3.3.1",
        ),
        "dbt": os.getenv(
            "DBT_IMAGE",
            "commerce-dbt:1.11",
        ),
    }

    image_results = {
        image_name: (
            run_command(
                [
                    "docker",
                    "image",
                    "inspect",
                    image_reference,
                ],
                check=False,
            ).returncode
            == 0
        )
        for image_name, image_reference
        in image_names.items()
    }

    checks = {
        "long_running_containers_are_healthy": all(
            assessment.healthy
            for assessment
            in container_states.values()
        ),
        "airflow_components_are_healthy": (
            airflow_is_healthy
        ),
        "debezium_connector_is_healthy": (
            connector_is_healthy
        ),
        "dag_imports_are_clean": (
            dag_imports_are_clean
        ),
        "dag_task_contract_is_valid": (
            dag_tasks == EXPECTED_DAG_TASKS
        ),
        "required_images_exist": all(
            image_results.values()
        ),
    }

    return {
        "containers": {
            service_name: assessment.as_dict()
            for service_name, assessment
            in container_states.items()
        },
        "airflow": {
            "health_url": airflow_health_url,
            "metadatabase": airflow_health.get(
                "metadatabase",
                {},
            ).get("status"),
            "scheduler": airflow_health.get(
                "scheduler",
                {},
            ).get("status"),
            "dag_processor": airflow_health.get(
                "dag_processor",
                {},
            ).get("status"),
        },
        "debezium": {
            "connector": connector_status.get(
                "connector",
                {},
            ).get("state"),
            "tasks": [
                task.get("state")
                for task
                in connector_status.get(
                    "tasks",
                    []
                )
            ],
        },
        "dag": {
            "dag_id": (
                "commerce_incremental_pipeline"
            ),
            "task_count": len(dag_tasks),
            "tasks": sorted(dag_tasks),
            "import_errors": (
                0
                if dag_imports_are_clean
                else 1
            ),
        },
        "images": {
            image_name: {
                "reference": image_names[
                    image_name
                ],
                "exists": image_results[
                    image_name
                ],
            }
            for image_name in image_names
        },
        "checks": checks,
    }


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the local commerce-platform "
            "deployment contract and live health."
        )
    )
    parser.add_argument(
        "--mode",
        choices=(
            "configuration",
            "live",
        ),
        default="live",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()
    checked_at = datetime.now(
        timezone.utc
    ).isoformat()

    configuration = configuration_result()

    result: dict[str, Any] = {
        "checked_at": checked_at,
        "mode": arguments.mode,
        "configuration": configuration,
    }

    checks = dict(configuration["checks"])

    if arguments.mode == "live":
        live = live_result()
        result["live"] = live
        checks.update(live["checks"])

    result["checks"] = checks
    result["status"] = (
        "valid"
        if all(checks.values())
        else "failed"
    )

    print(
        "DEPLOYMENT_VALIDATION_RESULT="
        + json.dumps(result, sort_keys=True)
    )

    if result["status"] != "valid":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
