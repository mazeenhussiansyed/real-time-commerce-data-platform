from __future__ import annotations

import unittest

from commerce_pipeline.dashboard import (
    build_dashboard_model,
    render_dashboard_html,
)


def reliability_result() -> dict:
    return {
        "status": "valid",
        "checks": {
            "events_reconcile": True,
            "services_are_healthy": True,
        },
        "event_reconciliation": {
            "live_bronze_event_count": 26291,
            "warehouse_event_count": 26291,
            "difference": 0,
        },
        "warehouse_quality": {
            "duplicate_event_ids": 0,
        },
        "bronze_storage": {
            "status": "valid",
            "quarantine_records": 8,
            "quarantine_reason_counts": {
                "unsupported_operation": 8,
            },
        },
        "freshness_slo_seconds": 86400,
        "freshness": {
            "warehouse_data_age_seconds": 97.584,
            "pipeline_success_age_seconds": 25660.734,
        },
        "latency_ms": {
            "measured_events": 26291,
            "connector_average": 869.283,
            "connector_maximum": 1460,
            "bronze_processing_average": 28650808.15,
            "bronze_processing_maximum": 28681886,
            "warehouse_load_average": 4412038.688,
            "warehouse_load_maximum": 53507075.077,
            "end_to_end_average": 33063716.121,
            "end_to_end_maximum": 53519149.077,
        },
        "current_state_counts": {
            "source": {
                "customers": 1000,
                "orders": 5000,
            },
            "bronze_derived_warehouse_state": {
                "customers": 1000,
                "orders": 5000,
            },
        },
    }


def deployment_result() -> dict:
    return {
        "status": "valid",
        "checks": {
            "compose_contract_is_valid": True,
            "containers_are_healthy": True,
        },
        "live": {
            "containers": {
                "postgres": {
                    "status": "healthy",
                    "healthy": True,
                },
                "warehouse": {
                    "status": "healthy",
                    "healthy": True,
                },
            },
            "dag": {
                "dag_id": (
                    "commerce_incremental_pipeline"
                ),
                "task_count": 8,
                "import_errors": 0,
            },
        },
    }


class DashboardTests(unittest.TestCase):
    def test_valid_results_produce_healthy_dashboard(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
            generated_at="2026-09-04T02:00:00+00:00",
        )

        self.assertTrue(model["healthy"])
        self.assertEqual(
            model["status"],
            "healthy",
        )
        self.assertEqual(
            model["failed_checks"],
            [],
        )

    def test_event_metrics_are_extracted(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
        )

        cards = {
            card["label"]: card
            for card in model["cards"]
        }

        self.assertEqual(
            cards["Bronze events"]["value"],
            "26,291",
        )
        self.assertEqual(
            cards["Warehouse events"]["value"],
            "26,291",
        )
        self.assertEqual(
            cards["Event difference"]["value"],
            "0",
        )
        self.assertEqual(
            cards["Duplicate event IDs"]["value"],
            "0",
        )

    def test_current_state_rows_reconcile(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
        )

        self.assertEqual(
            len(model["reconciliation"]),
            2,
        )
        self.assertTrue(
            all(
                row["reconciled"]
                for row
                in model["reconciliation"]
            )
        )

    def test_failed_check_requires_attention(
        self,
    ) -> None:
        reliability = reliability_result()
        reliability["status"] = "failed"
        reliability["checks"][
            "events_reconcile"
        ] = False

        model = build_dashboard_model(
            reliability,
            deployment_result(),
        )

        self.assertFalse(model["healthy"])
        self.assertEqual(
            model["status"],
            "attention_required",
        )
        self.assertEqual(
            model["failed_checks"],
            [
                {
                    "group": "Reliability",
                    "check": "events_reconcile",
                }
            ],
        )

    def test_freshness_card_uses_slo(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
        )

        cards = {
            card["label"]: card
            for card in model["cards"]
        }

        self.assertTrue(
            cards["Warehouse data age"][
                "healthy"
            ]
        )
        self.assertEqual(
            cards["Warehouse data age"][
                "value"
            ],
            "97.584 seconds",
        )

    def test_service_health_is_extracted(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
        )

        self.assertEqual(
            [row["service"] for row in model["services"]],
            ["postgres", "warehouse"],
        )
        self.assertTrue(
            all(
                row["healthy"]
                for row in model["services"]
            )
        )

    def test_html_contains_operational_sections(
        self,
    ) -> None:
        model = build_dashboard_model(
            reliability_result(),
            deployment_result(),
            generated_at="2026-09-04T02:00:00+00:00",
        )

        rendered = render_dashboard_html(model)

        self.assertIn(
            "Commerce Platform Operational Dashboard",
            rendered,
        )
        self.assertIn(
            "Current-state reconciliation",
            rendered,
        )
        self.assertIn(
            "No active validation failures.",
            rendered,
        )
        self.assertIn("26,291", rendered)
        self.assertIn(
            "unsupported_operation",
            rendered,
        )

    def test_html_escapes_external_values(
        self,
    ) -> None:
        deployment = deployment_result()
        deployment["live"]["dag"]["dag_id"] = (
            "<script>alert(1)</script>"
        )

        model = build_dashboard_model(
            reliability_result(),
            deployment,
        )
        rendered = render_dashboard_html(model)

        self.assertNotIn(
            "<script>alert(1)</script>",
            rendered,
        )
        self.assertIn(
            "&lt;script&gt;alert(1)&lt;/script&gt;",
            rendered,
        )

    def test_invalid_input_types_are_rejected(
        self,
    ) -> None:
        with self.assertRaises(TypeError):
            build_dashboard_model(
                [],
                deployment_result(),
            )

        with self.assertRaises(TypeError):
            build_dashboard_model(
                reliability_result(),
                [],
            )


if __name__ == "__main__":
    unittest.main()
