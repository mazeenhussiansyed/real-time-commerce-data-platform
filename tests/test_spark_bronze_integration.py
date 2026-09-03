from __future__ import annotations

import json
import os
import subprocess
import time
import unittest
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]

RUN_SPARK_INTEGRATION = (
    os.getenv("RUN_SPARK_INTEGRATION") == "1"
)


def run_command(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 240,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )


def parse_result(
    completed: subprocess.CompletedProcess[str],
    marker: str,
) -> dict[str, Any]:
    combined_output = (
        completed.stdout + "\n" + completed.stderr
    )

    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with code "
            f"{completed.returncode}:\n"
            f"{combined_output[-4000:]}"
        )

    for line in combined_output.splitlines():
        if line.startswith(marker):
            return json.loads(
                line.removeprefix(marker)
            )

    raise RuntimeError(
        f"result marker {marker!r} was not found:\n"
        f"{combined_output[-4000:]}"
    )


@unittest.skipUnless(
    RUN_SPARK_INTEGRATION,
    "Set RUN_SPARK_INTEGRATION=1 "
    "to run Spark Bronze integration tests.",
)
class SparkBronzeIntegrationTests(unittest.TestCase):
    stream_result: dict[str, Any]
    profile_result: dict[str, Any]

    @classmethod
    def setUpClass(cls) -> None:
        timestamp_ms = int(time.time() * 1000)

        invalid_event = json.dumps(
            {
                "payload": {
                    "before": None,
                    "after": {
                        "customer_id": timestamp_ms,
                    },
                    "source": {
                        "table": "customers",
                        "ts_ms": timestamp_ms,
                    },
                    "op": "x",
                    "ts_ms": timestamp_ms,
                }
            }
        )

        producer_result = run_command(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/"
                "kafka-console-producer.sh",
                "--bootstrap-server",
                "kafka:9092",
                "--topic",
                "commerce.commerce.customers",
            ],
            input_text=invalid_event + "\n",
            timeout=60,
        )

        if producer_result.returncode != 0:
            raise RuntimeError(
                "failed to publish the invalid test event:\n"
                + producer_result.stdout
                + producer_result.stderr
            )

        stream_command = [
            "docker",
            "compose",
            "--profile",
            "spark",
            "run",
            "--rm",
            "spark",
        ]

        stream_completed = run_command(
            stream_command,
            timeout=300,
        )

        cls.stream_result = parse_result(
            stream_completed,
            "BRONZE_STREAM_RESULT=",
        )

        profile_command = [
            "docker",
            "compose",
            "--profile",
            "spark",
            "run",
            "--rm",
            "spark",
            "/workspace/scripts/profile_bronze.py",
        ]

        profile_completed = run_command(
            profile_command,
            timeout=300,
        )

        cls.profile_result = parse_result(
            profile_completed,
            "BRONZE_PROFILE_RESULT=",
        )

    def test_stream_uses_checkpoint_without_duplicates(
        self,
    ) -> None:
        self.assertEqual(
            self.stream_result["status"],
            "valid",
        )
        self.assertEqual(
            self.stream_result["duplicate_event_ids"],
            0,
        )
        self.assertGreaterEqual(
            self.stream_result[
                "new_quarantine_events"
            ],
            1,
        )

    def test_bronze_has_expected_tables_and_metadata(
        self,
    ) -> None:
        self.assertEqual(
            self.profile_result["status"],
            "valid",
        )
        self.assertGreaterEqual(
            self.profile_result["bronze_records"],
            26249,
        )
        self.assertEqual(
            self.profile_result[
                "source_table_count"
            ],
            6,
        )
        self.assertEqual(
            self.profile_result["topic_count"],
            6,
        )
        self.assertEqual(
            self.profile_result[
                "topic_partition_count"
            ],
            18,
        )
        self.assertEqual(
            self.profile_result[
                "duplicate_event_ids"
            ],
            0,
        )
        self.assertEqual(
            self.profile_result[
                "total_null_metadata"
            ],
            0,
        )
        self.assertEqual(
            self.profile_result["missing_tables"],
            [],
        )
        self.assertEqual(
            self.profile_result[
                "unexpected_tables"
            ],
            [],
        )

    def test_invalid_event_is_quarantined(
        self,
    ) -> None:
        self.assertGreaterEqual(
            self.profile_result[
                "quarantine_records"
            ],
            1,
        )
        self.assertGreaterEqual(
            self.profile_result[
                "quarantine_reason_counts"
            ].get("unsupported_operation", 0),
            1,
        )
        self.assertEqual(
            self.profile_result[
                "quarantine_duplicate_event_ids"
            ],
            0,
        )


if __name__ == "__main__":
    unittest.main()
