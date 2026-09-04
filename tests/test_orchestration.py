from __future__ import annotations

import unittest

from commerce_pipeline.orchestration import (
    MAX_BACKFILL_DAYS,
    parse_run_configuration,
)


class RunConfigurationTests(unittest.TestCase):
    def test_default_configuration_is_incremental(self) -> None:
        configuration = parse_run_configuration(None)

        self.assertEqual(configuration.run_mode, "incremental")
        self.assertIsNone(configuration.backfill_start_date)
        self.assertIsNone(configuration.backfill_end_date)

    def test_valid_backfill_configuration(self) -> None:
        configuration = parse_run_configuration(
            {
                "run_mode": "backfill",
                "start_date": "2026-01-05",
                "end_date": "2026-01-07",
            }
        )

        self.assertEqual(
            configuration.as_dict(),
            {
                "run_mode": "backfill",
                "backfill_start_date": "2026-01-05",
                "backfill_end_date": "2026-01-07",
            },
        )

    def test_explicit_backfill_field_names_are_supported(self) -> None:
        configuration = parse_run_configuration(
            {
                "run_mode": "backfill",
                "backfill_start_date": "2026-01-10",
                "backfill_end_date": "2026-01-12",
            }
        )

        self.assertEqual(
            configuration.backfill_start_date.isoformat(),
            "2026-01-10",
        )
        self.assertEqual(
            configuration.backfill_end_date.isoformat(),
            "2026-01-12",
        )

    def test_backfill_requires_both_dates(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "require both start_date and end_date",
        ):
            parse_run_configuration(
                {
                    "run_mode": "backfill",
                    "start_date": "2026-01-05",
                }
            )

    def test_backfill_rejects_reversed_dates(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "must not be after",
        ):
            parse_run_configuration(
                {
                    "run_mode": "backfill",
                    "start_date": "2026-01-08",
                    "end_date": "2026-01-05",
                }
            )

    def test_backfill_rejects_excessive_window(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            f"cannot exceed {MAX_BACKFILL_DAYS} days",
        ):
            parse_run_configuration(
                {
                    "run_mode": "backfill",
                    "start_date": "2026-01-01",
                    "end_date": "2026-02-01",
                }
            )

    def test_incremental_run_rejects_dates(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "must not include backfill dates",
        ):
            parse_run_configuration(
                {
                    "run_mode": "incremental",
                    "start_date": "2026-01-05",
                    "end_date": "2026-01-07",
                }
            )

    def test_invalid_run_mode_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "incremental or backfill",
        ):
            parse_run_configuration(
                {
                    "run_mode": "full-refresh",
                }
            )

    def test_invalid_date_format_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "YYYY-MM-DD",
        ):
            parse_run_configuration(
                {
                    "run_mode": "backfill",
                    "start_date": "01/05/2026",
                    "end_date": "2026-01-07",
                }
            )


if __name__ == "__main__":
    unittest.main()
