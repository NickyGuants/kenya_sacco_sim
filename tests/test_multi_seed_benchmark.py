from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from kenya_sacco_sim.benchmark.multi_seed import _stability_report, _validate_seeds, run_multi_seed_benchmark
from kenya_sacco_sim.core.config import load_world_config, with_cli_overrides


class MultiSeedBenchmarkTests(unittest.TestCase):
    def test_seed_validation_rejects_empty_seed_list(self) -> None:
        with self.assertRaisesRegex(ValueError, "At least one seed"):
            _validate_seeds([])

    def test_seed_validation_rejects_duplicates(self) -> None:
        with self.assertRaisesRegex(ValueError, "Duplicate seeds"):
            _validate_seeds([42, 1337, 42])

    def test_stability_report_flags_precision_recall_range(self) -> None:
        report = _stability_report(
            [
                _seed_result("STRUCTURING", precision=0.80, recall=1.00),
                _seed_result("STRUCTURING", precision=0.70, recall=0.95),
            ]
        )

        section = report["typology_precision_recall"]["STRUCTURING"]
        self.assertEqual(section["precision"]["range"], 0.1)
        self.assertEqual(section["recall"]["range"], 0.05)
        self.assertTrue(section["within_threshold"])
        self.assertTrue(report["acceptance"]["precision_recall_variance_within_threshold"])

    def test_stability_report_fails_above_threshold(self) -> None:
        report = _stability_report(
            [
                _seed_result("RAPID_PASS_THROUGH", precision=0.90, recall=0.95),
                _seed_result("RAPID_PASS_THROUGH", precision=0.70, recall=0.70),
            ]
        )

        self.assertFalse(report["typology_precision_recall"]["RAPID_PASS_THROUGH"]["within_threshold"])
        self.assertFalse(report["acceptance"]["precision_recall_variance_within_threshold"])

    def test_multi_seed_benchmark_writes_summary(self) -> None:
        config = with_cli_overrides(load_world_config(Path("missing-config-dir")), member_count=30, suspicious_ratio=0.03)
        with tempfile.TemporaryDirectory() as temp_dir:
            result = run_multi_seed_benchmark(config, [42], Path(temp_dir))

            self.assertTrue((Path(temp_dir) / "multi_seed_results.json").exists())
            self.assertEqual(result["seed_count"], 1)
            self.assertTrue(result["acceptance"]["validation_error_free"])


def _seed_result(typology: str, precision: float, recall: float) -> dict[str, object]:
    return {
        "validation_error_count": 0,
        "distribution_metrics": {"cash_rail_share": 0.15, "active_member_share": 0.9},
        "device_metrics": {"digital_device_coverage": 1.0, "shared_device_member_share": 0.04},
        "credit_metrics": {"loan_active_member_ratio": 0.23, "arrears_share": 0.08, "default_share": 0.02},
        "baseline_metrics": {
            "per_typology": {
                typology: {
                    "precision": precision,
                    "recall": recall,
                }
            }
        },
    }


if __name__ == "__main__":
    unittest.main()
