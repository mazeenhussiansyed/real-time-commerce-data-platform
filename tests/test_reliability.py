from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from commerce_pipeline.reliability import (
    assess_count_reconciliation,
    assess_event_order,
    assess_record_schema,
    freshness_seconds,
    recovery_seconds,
)


class ReliabilityPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.customer_record = {
            "customer_id": 1,
            "first_name": "Ava",
            "last_name": "Anderson",
            "email": "ava@example.test",
            "customer_status": "active",
            "city": "Boston",
            "state_code": "MA",
            "created_at": "2025-12-31T08:00:00Z",
            "updated_at": "2026-09-03T17:00:00Z",
        }
        self.watermark = datetime(
            2026,
            9,
            3,
            18,
            0,
            tzinfo=timezone.utc,
        )

    def test_unchanged_schema_is_compatible(self) -> None:
        result = assess_record_schema(
            "customers",
            self.customer_record,
        )

        self.assertTrue(result.compatible)
        self.assertEqual(result.change_type, "unchanged")
        self.assertEqual(result.extra_fields, ())

    def test_additive_schema_change_is_compatible(self) -> None:
        record = {
            **self.customer_record,
            "loyalty_tier": "gold",
        }

        result = assess_record_schema("customers", record)

        self.assertTrue(result.compatible)
        self.assertEqual(result.change_type, "additive")
        self.assertEqual(
            result.extra_fields,
            ("loyalty_tier",),
        )

    def test_missing_primary_key_is_incompatible(self) -> None:
        record = dict(self.customer_record)
        del record["customer_id"]

        result = assess_record_schema("customers", record)

        self.assertFalse(result.compatible)
        self.assertEqual(result.reason, "missing_primary_key")

    def test_primary_key_type_change_is_incompatible(self) -> None:
        record = {
            **self.customer_record,
            "customer_id": "1",
        }

        result = assess_record_schema("customers", record)

        self.assertFalse(result.compatible)
        self.assertEqual(
            result.reason,
            "incompatible_primary_key_type",
        )

    def test_removed_expected_field_is_incompatible(self) -> None:
        record = dict(self.customer_record)
        del record["email"]

        result = assess_record_schema("customers", record)

        self.assertFalse(result.compatible)
        self.assertEqual(
            result.reason,
            "missing_expected_fields",
        )
        self.assertEqual(result.missing_fields, ("email",))

    def test_unexpected_table_is_incompatible(self) -> None:
        result = assess_record_schema(
            "unknown_table",
            self.customer_record,
        )

        self.assertFalse(result.compatible)
        self.assertEqual(
            result.reason,
            "unexpected_source_table",
        )

    def test_on_time_event_is_not_late(self) -> None:
        result = assess_event_order(
            self.watermark + timedelta(seconds=1),
            self.watermark,
        )

        self.assertEqual(result.status, "on_time")
        self.assertFalse(result.is_out_of_order)
        self.assertFalse(result.requires_backfill)

    def test_out_of_order_event_within_tolerance(self) -> None:
        result = assess_event_order(
            self.watermark - timedelta(seconds=60),
            self.watermark,
            allowed_lateness_seconds=300,
        )

        self.assertEqual(result.status, "out_of_order")
        self.assertTrue(result.is_out_of_order)
        self.assertFalse(result.is_late)
        self.assertFalse(result.requires_backfill)

    def test_late_event_requires_backfill(self) -> None:
        result = assess_event_order(
            self.watermark - timedelta(hours=2),
            self.watermark,
            allowed_lateness_seconds=300,
        )

        self.assertEqual(result.status, "late")
        self.assertEqual(result.event_delay_seconds, 7200.0)
        self.assertTrue(result.requires_backfill)

    def test_naive_event_timestamp_is_rejected(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "timezone",
        ):
            assess_event_order(
                datetime(2026, 9, 3, 17, 0),
                self.watermark,
            )

    def test_equal_counts_reconcile(self) -> None:
        result = assess_count_reconciliation(1000, 1000)

        self.assertTrue(result.reconciled)
        self.assertEqual(result.difference, 0)

    def test_missing_target_records_are_reported(self) -> None:
        result = assess_count_reconciliation(1000, 997)

        self.assertFalse(result.reconciled)
        self.assertEqual(result.missing_records, 3)
        self.assertEqual(result.unexpected_records, 0)

    def test_unexpected_target_records_are_reported(self) -> None:
        result = assess_count_reconciliation(1000, 1002)

        self.assertFalse(result.reconciled)
        self.assertEqual(result.missing_records, 0)
        self.assertEqual(result.unexpected_records, 2)

    def test_freshness_is_measured_in_seconds(self) -> None:
        observed = datetime(
            2026,
            9,
            3,
            17,
            55,
            tzinfo=timezone.utc,
        )

        result = freshness_seconds(
            observed,
            checked_at=self.watermark,
        )

        self.assertEqual(result, 300.0)

    def test_recovery_duration_is_measured(self) -> None:
        failed_at = datetime(
            2026,
            9,
            3,
            17,
            45,
            tzinfo=timezone.utc,
        )
        recovered_at = failed_at + timedelta(
            seconds=45.464
        )

        self.assertEqual(
            recovery_seconds(failed_at, recovered_at),
            45.464,
        )


if __name__ == "__main__":
    unittest.main()
