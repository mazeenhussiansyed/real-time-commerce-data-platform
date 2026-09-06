from __future__ import annotations

from dataclasses import dataclass
from typing import Any


EXPECTED_SERVICES = frozenset(
    {
        "postgres",
        "warehouse",
        "kafka",
        "connect",
        "spark",
        "airflow-db",
        "airflow-init",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
    }
)

HEALTHCHECK_SERVICES = frozenset(
    {
        "postgres",
        "warehouse",
        "kafka",
        "connect",
        "airflow-db",
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
    }
)

REQUIRED_DEPENDENCIES = {
    "connect": frozenset(
        {
            "postgres",
            "kafka",
        }
    ),
    "spark": frozenset(
        {
            "kafka",
        }
    ),
    "airflow-apiserver": frozenset(
        {
            "airflow-db",
            "airflow-init",
        }
    ),
    "airflow-scheduler": frozenset(
        {
            "airflow-db",
            "airflow-init",
            "airflow-apiserver",
        }
    ),
    "airflow-dag-processor": frozenset(
        {
            "airflow-db",
            "airflow-init",
            "airflow-apiserver",
        }
    ),
}


@dataclass(frozen=True)
class ComposeAssessment:
    expected_services: tuple[str, ...]
    configured_services: tuple[str, ...]
    missing_services: tuple[str, ...]
    unexpected_services: tuple[str, ...]
    missing_healthchecks: tuple[str, ...]
    missing_dependencies: dict[str, tuple[str, ...]]
    valid: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "expected_services": list(
                self.expected_services
            ),
            "configured_services": list(
                self.configured_services
            ),
            "missing_services": list(
                self.missing_services
            ),
            "unexpected_services": list(
                self.unexpected_services
            ),
            "missing_healthchecks": list(
                self.missing_healthchecks
            ),
            "missing_dependencies": {
                service_name: list(dependencies)
                for service_name, dependencies
                in self.missing_dependencies.items()
            },
            "valid": self.valid,
        }


@dataclass(frozen=True)
class ContainerAssessment:
    running: bool
    health_status: str | None
    healthy: bool
    status: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "running": self.running,
            "health_status": self.health_status,
            "healthy": self.healthy,
            "status": self.status,
        }


def assess_compose_model(
    model: dict[str, Any],
) -> ComposeAssessment:
    services = model.get("services")

    if not isinstance(services, dict):
        raise ValueError(
            "Compose model must contain a services object"
        )

    configured_services = frozenset(
        str(service_name)
        for service_name in services
    )

    missing_services = (
        EXPECTED_SERVICES - configured_services
    )
    unexpected_services = (
        configured_services - EXPECTED_SERVICES
    )

    missing_healthchecks = {
        service_name
        for service_name in HEALTHCHECK_SERVICES
        if service_name not in services
        or not isinstance(
            services[service_name],
            dict,
        )
        or not services[service_name].get(
            "healthcheck"
        )
    }

    missing_dependencies: dict[
        str,
        tuple[str, ...],
    ] = {}

    for service_name, expected_dependencies in (
        REQUIRED_DEPENDENCIES.items()
    ):
        service = services.get(service_name)

        if not isinstance(service, dict):
            missing_dependencies[service_name] = (
                tuple(
                    sorted(expected_dependencies)
                )
            )
            continue

        configured_dependencies = service.get(
            "depends_on",
            {},
        )

        if isinstance(configured_dependencies, dict):
            dependency_names = frozenset(
                str(name)
                for name in configured_dependencies
            )
        elif isinstance(
            configured_dependencies,
            list,
        ):
            dependency_names = frozenset(
                str(name)
                for name in configured_dependencies
            )
        else:
            dependency_names = frozenset()

        missing = (
            expected_dependencies - dependency_names
        )

        if missing:
            missing_dependencies[service_name] = (
                tuple(sorted(missing))
            )

    valid = (
        not missing_services
        and not unexpected_services
        and not missing_healthchecks
        and not missing_dependencies
    )

    return ComposeAssessment(
        expected_services=tuple(
            sorted(EXPECTED_SERVICES)
        ),
        configured_services=tuple(
            sorted(configured_services)
        ),
        missing_services=tuple(
            sorted(missing_services)
        ),
        unexpected_services=tuple(
            sorted(unexpected_services)
        ),
        missing_healthchecks=tuple(
            sorted(missing_healthchecks)
        ),
        missing_dependencies={
            service_name: missing_dependencies[
                service_name
            ]
            for service_name
            in sorted(missing_dependencies)
        },
        valid=valid,
    )


def assess_container_state(
    state: dict[str, Any],
) -> ContainerAssessment:
    running = state.get("Status") == "running"

    health = state.get("Health")
    health_status = None

    if isinstance(health, dict):
        raw_health_status = health.get("Status")

        if raw_health_status is not None:
            health_status = str(raw_health_status)

    healthy = (
        running
        and health_status == "healthy"
    )

    if not running:
        status = "not_running"
    elif health_status is None:
        status = "healthcheck_missing"
    elif healthy:
        status = "healthy"
    else:
        status = "unhealthy"

    return ContainerAssessment(
        running=running,
        health_status=health_status,
        healthy=healthy,
        status=status,
    )
