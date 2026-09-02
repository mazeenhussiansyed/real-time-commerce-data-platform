from __future__ import annotations

import unittest

from commerce_pipeline.cdc import TABLE_TOPICS
from scripts.verify_cdc_operations import (
    customer_id_from_event,
    debezium_payload,
)


class CDCHelperTests(unittest.TestCase):
    def test_expected_table_topic_mapping(self) -> None:
        self.assertEqual(
            TABLE_TOPICS,
            {
                "customers": "commerce.commerce.customers",
                "products": "commerce.commerce.products",
                "orders": "commerce.commerce.orders",
                "order_items": "commerce.commerce.order_items",
                "payments": "commerce.commerce.payments",
                "shipments": "commerce.commerce.shipments",
            },
        )
        self.assertEqual(len(set(TABLE_TOPICS.values())), 6)

    def test_schema_wrapped_debezium_payload_is_unwrapped(
        self,
    ) -> None:
        event = {
            "schema": {"type": "struct"},
            "payload": {
                "before": None,
                "after": {"customer_id": 123},
                "op": "c",
            },
        }

        payload = debezium_payload(event)

        self.assertEqual(payload["op"], "c")
        self.assertEqual(
            payload["after"]["customer_id"],
            123,
        )

    def test_unwrapped_debezium_payload_is_supported(
        self,
    ) -> None:
        event = {
            "before": None,
            "after": {"customer_id": 456},
            "op": "c",
        }

        self.assertIs(debezium_payload(event), event)
        self.assertEqual(customer_id_from_event(event), 456)

    def test_delete_event_uses_before_record(
        self,
    ) -> None:
        event = {
            "schema": {"type": "struct"},
            "payload": {
                "before": {"customer_id": 789},
                "after": None,
                "op": "d",
            },
        }

        self.assertEqual(customer_id_from_event(event), 789)


if __name__ == "__main__":
    unittest.main()