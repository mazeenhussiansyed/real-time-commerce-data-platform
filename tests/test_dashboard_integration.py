from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any


RUN_INTEGRATION = (
    os.getenv("RUN_DASHBOARD_INTEGRATION")
    == "1"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULT_PREFIX = "DASHBOARD_BUILD_RESULT="


@unittest.skipUnless(
    RUN_INTEGRATION,
    "Set RUN_DASHBOARD_INTEGRATION=1 "
    "to run dashboard integration tests.",
)
class DashboardIntegrationTests(
    unittest.TestCase
):
    temporary_directory: (
        tempfile.TemporaryDirectory[str]
    )
    build_result: dict[str, Any]
    model: dict[str, Any]
    rendered_html: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary_directory = (
            tempfile.TemporaryDirectory(
                prefix="commerce-dashboard-"
            )
        )
        output_directory = Path(
            cls.temporary_directory.name
        )

        completed = subprocess.run(
            [
                sys.executable,
                "scripts/"
                "generate_operational_dashboard.py",
                "--output-dir",
                str(output_directory),
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=420,
            check=False,
        )

        if completed.returncode != 0:
            diagnostic = (
                completed.stdout
                + "\n"
                + completed.stderr
            )[-6000:]

            raise RuntimeError(
                "Live dashboard generation failed:\n"
                + diagnostic
            )

        build_result = None

        for line in completed.stdout.splitlines():
            prefix_position = line.find(
                RESULT_PREFIX
            )

            if prefix_position == -1:
                continue

            build_result = json.loads(
                line[
                    prefix_position
                    + len(RESULT_PREFIX):
                ]
            )

        if not isinstance(build_result, dict):
            raise RuntimeError(
                "DASHBOARD_BUILD_RESULT was not "
                "found in generator output"
            )

        json_path = (
            output_directory
            / "operational_dashboard.json"
        )
        html_path = (
            output_directory
            / "operational_dashboard.html"
        )

        if (
            not json_path.is_file()
            or not html_path.is_file()
        ):
            raise RuntimeError(
                "Dashboard output files were "
                "not generated"
            )

        model = json.loads(
            json_path.read_text(
                encoding="utf-8"
            )
        )

        if not isinstance(model, dict):
            raise RuntimeError(
                "Dashboard model was not an object"
            )

        cls.build_result = build_result
        cls.model = model
        cls.rendered_html = (
            html_path.read_text(
                encoding="utf-8"
            )
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temporary_directory.cleanup()

    def test_dashboard_build_is_valid(
        self,
    ) -> None:
        self.assertEqual(
            self.build_result["status"],
            "valid",
        )
        self.assertEqual(
            self.build_result[
                "dashboard_status"
            ],
            "healthy",
        )
        self.assertEqual(
            self.build_result[
                "failed_check_count"
            ],
            0,
        )

    def test_dashboard_model_is_healthy(
        self,
    ) -> None:
        self.assertTrue(
            self.model["healthy"]
        )
        self.assertEqual(
            self.model["status"],
            "healthy",
        )
        self.assertEqual(
            self.model["failed_checks"],
            [],
        )

    def test_all_current_state_tables_reconcile(
        self,
    ) -> None:
        rows = self.model["reconciliation"]

        self.assertEqual(len(rows), 6)
        self.assertTrue(
            all(
                row["reconciled"]
                for row in rows
            )
        )
        self.assertTrue(
            all(
                row["difference"] == 0
                for row in rows
            )
        )

    def test_live_services_are_healthy(
        self,
    ) -> None:
        services = self.model["services"]

        self.assertEqual(len(services), 8)
        self.assertTrue(
            all(
                service["healthy"]
                for service in services
            )
        )

    def test_pipeline_contract_is_visible(
        self,
    ) -> None:
        dag = self.model["dag"]

        self.assertEqual(
            dag["dag_id"],
            "commerce_incremental_pipeline",
        )
        self.assertEqual(
            dag["task_count"],
            8,
        )
        self.assertEqual(
            dag["import_errors"],
            0,
        )

    def test_html_contains_required_sections(
        self,
    ) -> None:
        self.assertIn(
            "<!doctype html>",
            self.rendered_html,
        )
        self.assertIn(
            "Commerce Platform "
            "Operational Dashboard",
            self.rendered_html,
        )
        self.assertIn(
            "Service health",
            self.rendered_html,
        )
        self.assertIn(
            "Current-state reconciliation",
            self.rendered_html,
        )
        self.assertIn(
            "Quarantine reasons",
            self.rendered_html,
        )
        self.assertIn(
            "No active validation failures.",
            self.rendered_html,
        )


if __name__ == "__main__":
    unittest.main()
