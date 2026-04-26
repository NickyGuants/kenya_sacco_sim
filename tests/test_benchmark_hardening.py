from __future__ import annotations

import unittest

from kenya_sacco_sim.benchmark.artifacts import build_benchmark_artifacts
from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.validation.labels import validate_labels


class BenchmarkHardeningTests(unittest.TestCase):
    def test_feature_documentation_exposes_label_file_role(self) -> None:
        artifacts = build_benchmark_artifacts(
            {"members.csv": [], "transactions.csv": [], "alerts_truth.csv": []},
            {},
            WorldConfig(member_count=0, suspicious_ratio=0),
        )

        docs = artifacts["feature_documentation.json"]

        self.assertEqual(docs["files"]["alerts_truth.csv"]["file_role"], "label")
        self.assertTrue(docs["files"]["alerts_truth.csv"]["label_file"])
        self.assertEqual(docs["feature_files"]["transactions.csv"]["file_role"], "feature")
        self.assertEqual(docs["feature_files"]["transactions.csv"]["split_key"], "member_id_primary")

    def test_split_manifest_reports_unassigned_member_references(self) -> None:
        artifacts = build_benchmark_artifacts(
            {
                "members.csv": [{"member_id": "MEM0000001"}],
                "transactions.csv": [],
                "alerts_truth.csv": [
                    {
                        "pattern_id": "PAT00000001",
                        "member_id": "MEM9999999",
                    }
                ],
            },
            {},
            WorldConfig(member_count=1),
        )

        checks = artifacts["split_manifest.json"]["checks"]

        self.assertFalse(checks["no_member_id_split_leakage"])
        self.assertFalse(checks["no_pattern_id_split_leakage"])
        self.assertEqual(checks["unassigned_member_reference_count"], 1)
        self.assertEqual(checks["unassigned_pattern_count"], 1)

    def test_label_validation_uses_half_up_target_count(self) -> None:
        rows_by_file = {
            "members.csv": [{"member_id": f"MEM{i:07d}"} for i in range(1, 101)],
            "alerts_truth.csv": [],
        }

        findings, label_section, _ = validate_labels(rows_by_file, suspicious_ratio=0.005)

        self.assertEqual(label_section["target_suspicious_member_count"], 1)
        self.assertEqual(label_section["status"], "labels_missing")
        self.assertEqual([finding.code for finding in findings], ["label.alerts_missing"])


if __name__ == "__main__":
    unittest.main()
