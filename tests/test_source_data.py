from __future__ import annotations

import unittest

from commerce_pipeline.source_data import (
    SourceDataConfig,
    dataset_fingerprint,
    generate_dataset,
    summarize_dataset,
    validate_dataset,
)


class SourceDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dataset = generate_dataset()

    def test_default_dataset_has_expected_shape(self) -> None:
        summary = summarize_dataset(self.dataset)

        self.assertEqual(summary["customers"], 1_000)
        self.assertEqual(summary["products"], 250)
        self.assertEqual(summary["orders"], 5_000)
        self.assertEqual(summary["order_items"], 12_500)
        self.assertEqual(summary["payments"], 5_000)
        self.assertEqual(summary["shipments"], 2_499)

    def test_dataset_passes_integrity_validation(self) -> None:
        self.assertEqual(validate_dataset(self.dataset), [])

    def test_generation_is_deterministic(self) -> None:
        second_dataset = generate_dataset()

        self.assertEqual(
            dataset_fingerprint(self.dataset),
            dataset_fingerprint(second_dataset),
        )

    def test_different_seed_changes_the_dataset(self) -> None:
        different_dataset = generate_dataset(
            SourceDataConfig(seed=20260903)
        )

        self.assertNotEqual(
            dataset_fingerprint(self.dataset),
            dataset_fingerprint(different_dataset),
        )

    def test_every_order_has_one_payment(self) -> None:
        order_ids = {
            int(row["order_id"])
            for row in self.dataset.orders
        }
        payment_order_ids = {
            int(row["order_id"])
            for row in self.dataset.payments
        }

        self.assertEqual(order_ids, payment_order_ids)


if __name__ == "__main__":
    unittest.main()