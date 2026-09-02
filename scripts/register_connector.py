from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = (
    PROJECT_ROOT
    / "infrastructure"
    / "debezium"
    / "commerce-postgres-connector.json"
)


def connect_url() -> str:
    return os.getenv(
        "KAFKA_CONNECT_URL",
        "http://127.0.0.1:8083",
    ).rstrip("/")


def request_json(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any]:
    body = None
    headers = {"Accept": "application/json"}

    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        f"{connect_url()}{path}",
        data=body,
        headers=headers,
        method=method,
    )

    with urlopen(request, timeout=10) as response:
        content = response.read().decode("utf-8")
        return json.loads(content) if content else {}


def load_connector_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))

    config = payload.get("config")
    if not isinstance(config, dict):
        raise ValueError("connector JSON requires a config object")

    config["database.user"] = os.getenv(
        "COMMERCE_POSTGRES_USER",
        "commerce_app",
    )
    config["database.password"] = os.getenv(
        "COMMERCE_POSTGRES_PASSWORD",
        "commerce_dev_password",
    )
    config["database.dbname"] = os.getenv(
        "COMMERCE_POSTGRES_DB",
        "commerce",
    )

    return payload


def wait_for_connect(timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            request_json("GET", "/connectors")
            return
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            time.sleep(2)

    raise RuntimeError(
        f"Kafka Connect did not become ready within {timeout} seconds: "
        f"{last_error}"
    )


def connector_exists(name: str) -> bool:
    try:
        request_json("GET", f"/connectors/{name}")
        return True
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def register_connector(
    payload: dict[str, Any],
) -> str:
    name = str(payload["name"])
    config = payload["config"]

    if connector_exists(name):
        request_json(
            "PUT",
            f"/connectors/{name}/config",
            config,
        )
        return "updated"

    request_json("POST", "/connectors", payload)
    return "created"


def wait_for_running(
    name: str,
    timeout: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_status: dict[str, Any] = {}

    while time.monotonic() < deadline:
        try:
            result = request_json(
                "GET",
                f"/connectors/{name}/status",
            )
        except (HTTPError, URLError, TimeoutError):
            time.sleep(2)
            continue

        if not isinstance(result, dict):
            time.sleep(2)
            continue

        last_status = result
        connector = result.get("connector", {})
        tasks = result.get("tasks", [])

        connector_running = (
            isinstance(connector, dict)
            and connector.get("state") == "RUNNING"
        )
        tasks_running = (
            isinstance(tasks, list)
            and len(tasks) > 0
            and all(
                isinstance(task, dict)
                and task.get("state") == "RUNNING"
                for task in tasks
            )
        )

        if connector_running and tasks_running:
            return result

        failed_tasks = [
            task
            for task in tasks
            if isinstance(task, dict)
            and task.get("state") == "FAILED"
        ]
        if failed_tasks:
            raise RuntimeError(
                "Debezium task failed: "
                + json.dumps(failed_tasks, indent=2)
            )

        time.sleep(2)

    raise RuntimeError(
        "connector did not reach RUNNING state: "
        + json.dumps(last_status, indent=2)
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create or update the commerce Debezium connector"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
    )
    args = parser.parse_args()

    try:
        payload = load_connector_config(args.config)
        wait_for_connect(args.timeout)
        action = register_connector(payload)
        status = wait_for_running(
            str(payload["name"]),
            args.timeout,
        )
    except (
        OSError,
        ValueError,
        RuntimeError,
        HTTPError,
        URLError,
    ) as exc:
        parser.exit(
            2,
            f"connector registration failed: {exc}\n",
        )

    tasks = status.get("tasks", [])
    result = {
        "status": "running",
        "action": action,
        "connector": payload["name"],
        "connector_state": status["connector"]["state"],
        "tasks": [
            {
                "id": task.get("id"),
                "state": task.get("state"),
                "worker_id": task.get("worker_id"),
            }
            for task in tasks
        ],
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()