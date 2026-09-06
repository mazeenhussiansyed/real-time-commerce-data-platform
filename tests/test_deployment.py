from __future__ import annotations

import unittest

from commerce_pipeline.deployment import (
    EXPECTED_SERVICES,
    HEALTHCHECK_SERVICES,
    REQUIRED_DEPENDENCIES,
    assess_compose_model,
    assess_container_state,
)


def valid_compose_model() -> dict:
    services = {
        service_name: {}
        for service_name in EXPECTED_SERVICES
    }

    for service_name in HEALTHCHECK_SERVICES:
        services[service_name]["healthcheck"] = {
            "test": [
                "CMD",
                "true",
            ]
        }

    for service_name, dependencies in (
        REQUIRED_DEPENDENCIES.items()
    ):
        services[service_name]["depends_on"] = {
            dependency: {
                "condition": "service_healthy",
            }
            for dependency in dependencies
        }

    return {"services": services}


class DeploymentContractTests(unittest.TestCase):
    def test_complete_compose_model_is_valid(
        self,
    ) -> None:
        result = assess_compose_model(
            valid_compose_model()
        )

        self.assertTrue(result.valid)
        self.assertEqual(result.missing_services, ())
        self.assertEqual(
            result.missing_healthchecks,
            (),
        )
        self.assertEqual(
            result.missing_dependencies,
            {},
        )

    def test_missing_service_is_detected(
        self,
    ) -> None:
        model = valid_compose_model()
        del model["services"]["warehouse"]

        result = assess_compose_model(model)

        self.assertFalse(result.valid)
        self.assertEqual(
            result.missing_services,
            ("warehouse",),
        )

    def test_unexpected_service_is_detected(
        self,
    ) -> None:
        model = valid_compose_model()
        model["services"]["unexpected"] = {}

        result = assess_compose_model(model)

        self.assertFalse(result.valid)
        self.assertEqual(
            result.unexpected_services,
            ("unexpected",),
        )

    def test_missing_healthcheck_is_detected(
        self,
    ) -> None:
        model = valid_compose_model()
        del model["services"]["kafka"][
            "healthcheck"
        ]

        result = assess_compose_model(model)

        self.assertFalse(result.valid)
        self.assertEqual(
            result.missing_healthchecks,
            ("kafka",),
        )

    def test_missing_dependency_is_detected(
        self,
    ) -> None:
        model = valid_compose_model()
        del model["services"]["connect"][
            "depends_on"
        ]["kafka"]

        result = assess_compose_model(model)

        self.assertFalse(result.valid)
        self.assertEqual(
            result.missing_dependencies,
            {
                "connect": ("kafka",),
            },
        )

    def test_healthy_container_is_identified(
        self,
    ) -> None:
        result = assess_container_state(
            {
                "Status": "running",
                "Health": {
                    "Status": "healthy",
                },
            }
        )

        self.assertTrue(result.healthy)
        self.assertEqual(result.status, "healthy")

    def test_unhealthy_container_is_identified(
        self,
    ) -> None:
        result = assess_container_state(
            {
                "Status": "running",
                "Health": {
                    "Status": "unhealthy",
                },
            }
        )

        self.assertFalse(result.healthy)
        self.assertEqual(result.status, "unhealthy")

    def test_stopped_container_is_identified(
        self,
    ) -> None:
        result = assess_container_state(
            {
                "Status": "exited",
                "Health": {
                    "Status": "healthy",
                },
            }
        )

        self.assertFalse(result.healthy)
        self.assertEqual(
            result.status,
            "not_running",
        )


if __name__ == "__main__":
    unittest.main()
