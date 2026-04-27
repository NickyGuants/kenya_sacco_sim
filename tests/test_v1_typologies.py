from __future__ import annotations

import unittest

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.generators.typologies import _target_counts


class V1TypologyTargetTests(unittest.TestCase):
    def test_zero_suspicious_ratio_keeps_all_typology_targets_zero(self) -> None:
        counts = _target_counts(WorldConfig(member_count=10_000, suspicious_ratio=0.0))

        self.assertEqual(
            counts,
            {
                "STRUCTURING": 0,
                "RAPID_PASS_THROUGH": 0,
                "FAKE_AFFORDABILITY_BEFORE_LOAN": 0,
                "DEVICE_SHARING_MULE_NETWORK": 0,
                "GUARANTOR_FRAUD_RING": 0,
            },
        )

    def test_sub_1000_smoke_run_does_not_request_partial_device_group(self) -> None:
        counts = _target_counts(WorldConfig(member_count=500, suspicious_ratio=0.01))

        self.assertEqual(sum(counts.values()), 5)
        self.assertEqual(counts["DEVICE_SHARING_MULE_NETWORK"], 0)

    def test_device_typology_is_never_requested_below_group_size(self) -> None:
        counts = _target_counts(WorldConfig(member_count=1_000, suspicious_ratio=0.01))

        self.assertEqual(sum(counts.values()), 10)
        self.assertGreaterEqual(counts["DEVICE_SHARING_MULE_NETWORK"], 3)

    def test_full_benchmark_allocates_minimum_labels_for_five_typologies(self) -> None:
        counts = _target_counts(WorldConfig(member_count=10_000, suspicious_ratio=0.01))

        self.assertEqual(sum(counts.values()), 150)
        self.assertEqual(counts["GUARANTOR_FRAUD_RING"], 30)


if __name__ == "__main__":
    unittest.main()
