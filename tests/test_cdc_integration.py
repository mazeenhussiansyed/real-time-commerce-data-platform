from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_CDC_INTEGRATION = (
    os.getenv("RUN_CDC_INTEGRATION") == "1"
)

EXPECTED_SOURCE_COUNTS = {
    "customers": 1000,
    "products": 250,
    "orders": 5000,
    "order_items": 12500,
    "payments": 5000,
    "shipments": 2499,
}


def run_json_script(
    script_name: str,
    *arguments: str,
    timeout: float = 90.0,
) -> dict[str, object]:
    completed = subprocess.run(
        [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / script_name),
            *arguments,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )

    if completed.returncode != 0:
        raise AssertionError(
            f"{script_name} failed with exit code "
            f"{completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"{script_name} did not return valid JSON\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        ) from exc

    if not isinstance(payload, dict):
        raise AssertionError(
            f"{script_name} did not return a JSON object"
        )

    return payload


@unittest.skipUnless(
    RUN_CDC_INTEGRATION,
    "Set RUN_CDC_INTEGRATION=1 to run CDC integration tests.",
)
class CDCIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.profile = run_json_script(
            "profile_cdc.py",
            timeout=60.0,
        )

    def test_connector_and_replication_are_healthy(
        self,
    ) -> None:
        self.assertEqual(
            self.profile["status"],
            "valid",
        )
        self.assertEqual(
            self.profile["connector_state"],
            "RUNNING",
        )
        self.assertTrue(
            all(
                state == "RUNNING"
                for state in self.profile["task_states"]
            )
        )

        replication = self.profile["replication"]
        slot = replication["slot"]

        self.assertTrue(slot["exists"])
        self.assertTrue(slot["active"])
        self.assertEqual(
            replication["publication_table_count"],
            6,
        )

    def test_all_source_rows_are_represented_in_kafka(
        self,
    ) -> None:
        source_counts = self.profile["source_counts"]
        topic_counts = self.profile["topic_event_counts"]

        self.assertEqual(
            source_counts,
            EXPECTED_SOURCE_COUNTS,
        )
        self.assertEqual(
            self.profile["total_source_rows"],
            26249,
        )
        self.assertEqual(
            self.profile["minimum_completeness_rate"],
            1.0,
        )

        for table, source_count in source_counts.items():
            self.assertGreaterEqual(
                topic_counts[table],
                source_count,
                msg=(
                    f"{table} topic contains fewer events "
                    f"than the source table"
                ),
            )

    def test_live_create_update_delete_events(
        self,
    ) -> None:
        result = run_json_script(
            "verify_cdc_operations.py",
            "--timeout",
            "60",
            timeout=90.0,
        )

        self.assertEqual(result["status"], "verified")
        self.assertEqual(
            result["operations"],
            ["c", "u", "d"],
        )
        self.assertEqual(result["events_received"], 3)
        self.assertEqual(
            result["remaining_source_rows"],
            0,
        )


if __name__ == "__main__":
    unittest.main()