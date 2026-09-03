from __future__ import annotations

import json
import unittest

from commerce_pipeline.bronze import (
    build_event_id,
    classify_debezium_value,
)


class BronzeHelperTests(unittest.TestCase):
    def test_event_id_is_deterministic(self) -> None:
        first = build_event_id(
            "commerce.commerce.customers",
            1,
            25,
        )
        second = build_event_id(
            "commerce.commerce.customers",
            1,
            25,
        )

        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)

    def test_event_id_changes_with_kafka_offset(self) -> None:
        first = build_event_id(
            "commerce.commerce.customers",
            1,
            25,
        )
        second = build_event_id(
            "commerce.commerce.customers",
            1,
            26,
        )

        self.assertNotEqual(first, second)

    def test_wrapped_snapshot_event_is_valid(self) -> None:
        raw_value = json.dumps(
            {
                "schema": {"type": "struct"},
                "payload": {
                    "before": None,
                    "after": {
                        "customer_id": 101,
                        "email": "customer@example.com",
                    },
                    "source": {
                        "table": "customers",
                    },
                    "op": "r",
                },
            }
        )

        result = classify_debezium_value(raw_value)

        self.assertTrue(result.valid)
        self.assertIsNone(result.reason)
        self.assertEqual(result.operation, "r")
        self.assertEqual(
            result.source_table,
            "customers",
        )
        self.assertEqual(
            result.record,
            {
                "customer_id": 101,
                "email": "customer@example.com",
            },
        )

    def test_unwrapped_update_event_is_valid(self) -> None:
        raw_value = json.dumps(
            {
                "before": {
                    "order_id": 500,
                    "status": "created",
                },
                "after": {
                    "order_id": 500,
                    "status": "confirmed",
                },
                "source": {
                    "table": "orders",
                },
                "op": "u",
            }
        )

        result = classify_debezium_value(raw_value)

        self.assertTrue(result.valid)
        self.assertEqual(result.operation, "u")
        self.assertEqual(
            result.record["status"],
            "confirmed",
        )

    def test_delete_event_uses_before_record(self) -> None:
        raw_value = json.dumps(
            {
                "before": {
                    "customer_id": 700,
                },
                "after": None,
                "source": {
                    "table": "customers",
                },
                "op": "d",
            }
        )

        result = classify_debezium_value(raw_value)

        self.assertTrue(result.valid)
        self.assertEqual(result.operation, "d")
        self.assertEqual(
            result.record,
            {"customer_id": 700},
        )

    def test_unsupported_operation_is_quarantined(self) -> None:
        raw_value = json.dumps(
            {
                "payload": {
                    "before": None,
                    "after": {
                        "customer_id": 999,
                    },
                    "source": {
                        "table": "customers",
                    },
                    "op": "x",
                }
            }
        )

        result = classify_debezium_value(raw_value)

        self.assertFalse(result.valid)
        self.assertEqual(
            result.reason,
            "unsupported_operation",
        )

    def test_invalid_json_is_quarantined(self) -> None:
        result = classify_debezium_value(
            "this is not valid JSON"
        )

        self.assertFalse(result.valid)
        self.assertEqual(
            result.reason,
            "invalid_json",
        )


if __name__ == "__main__":
    unittest.main()
