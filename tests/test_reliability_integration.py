from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any


RUN_INTEGRATION = (
    os.getenv("RUN_RELIABILITY_INTEGRATION") == "1"
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESULT_PREFIX = "RELIABILITY_PROFILE_RESULT="


@unittest.skipUnless(
    RUN_INTEGRATION,
    "Set RUN_RELIABILITY_INTEGRATION=1 "
    "to run reliability integration tests.",
)
class ReliabilityIntegrationTests(unittest.TestCase):
    profile: dict[str, Any]

    @classmethod
    def setUpClass(cls) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                "scripts/profile_reliability.py",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        if completed.returncode != 0:
            diagnostic = (
                completed.stdout
                + "\n"
                + completed.stderr
            )[-5000:]

            raise RuntimeError(
                "Reliability profiling failed:\n"
                + diagnostic
            )

        result = None

        for line in completed.stdout.splitlines():
            if line.startswith(RESULT_PREFIX):
                result = json.loads(
                    line[len(RESULT_PREFIX):]
                )

        if not isinstance(result, dict):
            raise RuntimeError(
                "RELIABILITY_PROFILE_RESULT was not "
                "found in profiler output"
            )

        cls.profile = result

    def test_all_operational_checks_pass(self) -> None:
        self.assertEqual(
            self.profile["status"],
            "valid",
        )

        failed_checks = [
            check_name
            for check_name, passed
            in self.profile["checks"].items()
            if not passed
        ]

        self.assertEqual(failed_checks, [])

    def test_live_bronze_and_warehouse_reconcile(
        self,
    ) -> None:
        reconciliation = self.profile[
            "event_reconciliation"
        ]

        self.assertTrue(reconciliation["reconciled"])
        self.assertEqual(
            reconciliation["difference"],
            0,
        )
        self.assertEqual(
            reconciliation["missing_records"],
            0,
        )
        self.assertEqual(
            reconciliation["unexpected_records"],
            0,
        )
        self.assertEqual(
            reconciliation["live_bronze_event_count"],
            reconciliation["warehouse_event_count"],
        )
        self.assertEqual(
            reconciliation[
                "latest_load_source_record_count"
            ],
            reconciliation[
                "live_bronze_event_count"
            ],
        )
        self.assertGreater(
            reconciliation["warehouse_event_count"],
            0,
        )

    def test_current_state_counts_reconcile(
        self,
    ) -> None:
        counts = self.profile[
            "current_state_counts"
        ]

        self.assertEqual(
            counts["source"],
            counts["bronze_derived_warehouse_state"],
        )

        unreconciled_tables = [
            table_name
            for table_name, assessment
            in self.profile[
                "current_state_reconciliation"
            ].items()
            if not assessment["reconciled"]
        ]

        self.assertEqual(unreconciled_tables, [])

    def test_quarantine_records_have_reasons(
        self,
    ) -> None:
        bronze_storage = self.profile[
            "bronze_storage"
        ]

        self.assertEqual(
            bronze_storage["status"],
            "valid",
        )
        self.assertGreaterEqual(
            bronze_storage["quarantine_records"],
            1,
        )
        self.assertEqual(
            sum(
                bronze_storage[
                    "quarantine_reason_counts"
                ].values()
            ),
            bronze_storage["quarantine_records"],
        )

    def test_required_services_are_healthy(
        self,
    ) -> None:
        health = self.profile["service_health"]

        self.assertEqual(
            health["postgres_source"],
            "healthy",
        )
        self.assertEqual(
            health["postgres_warehouse"],
            "healthy",
        )
        self.assertEqual(
            health["kafka"],
            "healthy",
        )
        self.assertEqual(
            health["debezium_connector"],
            "RUNNING",
        )
        self.assertTrue(
            health["debezium_tasks"]
        )
        self.assertTrue(
            all(
                state == "RUNNING"
                for state
                in health["debezium_tasks"]
            )
        )
        self.assertEqual(
            health["airflow"]["metadatabase"],
            "healthy",
        )
        self.assertEqual(
            health["airflow"]["scheduler"],
            "healthy",
        )
        self.assertEqual(
            health["airflow"]["dag_processor"],
            "healthy",
        )

    def test_freshness_and_latency_are_measured(
        self,
    ) -> None:
        freshness = self.profile["freshness"]
        slo_seconds = self.profile[
            "freshness_slo_seconds"
        ]
        latency = self.profile["latency_ms"]

        self.assertLessEqual(
            freshness["warehouse_data_age_seconds"],
            slo_seconds,
        )
        self.assertLessEqual(
            freshness["pipeline_success_age_seconds"],
            slo_seconds,
        )
        self.assertGreater(
            latency["measured_events"],
            0,
        )
        self.assertGreaterEqual(
            latency["connector_average"],
            0,
        )
        self.assertGreaterEqual(
            latency["bronze_processing_average"],
            0,
        )
        self.assertGreaterEqual(
            latency["warehouse_load_average"],
            0,
        )
        self.assertGreaterEqual(
            latency["end_to_end_average"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
