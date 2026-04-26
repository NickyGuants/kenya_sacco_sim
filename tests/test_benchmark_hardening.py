from __future__ import annotations

import unittest

from kenya_sacco_sim.benchmark.artifacts import build_benchmark_artifacts
from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.validation.distribution import validate_distribution
from kenya_sacco_sim.validation.labels import validate_labels
from kenya_sacco_sim.validation.report import build_validation_report


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

    def test_church_org_median_gate_is_annualized(self) -> None:
        rows_by_file = {
            "members.csv": [{"member_id": "MEM0000001", "persona_type": "CHURCH_ORG"}],
            "accounts.csv": [],
            "transactions.csv": [
                _transaction("TXN000000000001", "2024-01-07T10:00:00+03:00"),
                _transaction("TXN000000000002", "2024-01-14T10:00:00+03:00"),
            ],
        }

        findings, section = validate_distribution(rows_by_file, WorldConfig(member_count=1, months=1))

        self.assertNotIn("distribution.church_org_median_txns_low", {finding.code for finding in findings})
        self.assertEqual(section["persona_summary"]["CHURCH_ORG"]["median_txns_per_member"], 2)
        self.assertEqual(section["persona_summary"]["CHURCH_ORG"]["median_txns_per_member_annualized"], 24)

    def test_benchmark_validation_failures_promote_to_errors(self) -> None:
        report = build_validation_report(
            {
                "members.csv": [],
                "accounts.csv": [],
                "nodes.csv": [],
                "graph_edges.csv": [],
            },
            WorldConfig(member_count=0),
            benchmark_validation={
                "no_member_id_split_leakage": False,
                "no_pattern_id_split_leakage": False,
                "reference_leakage": {"mirrored_reference_count": 1},
                "txn_id_leakage": {
                    "best_txn_id_threshold_rule": {
                        "precision": 0.8,
                        "recall": 0.8,
                    }
                },
            },
        )

        self.assertEqual(
            {error["code"] for error in report["errors"]},
            {
                "benchmark.member_split_leakage",
                "benchmark.pattern_split_leakage",
                "benchmark.reference_leakage",
                "benchmark.txn_id_threshold_leakage",
            },
        )


def _transaction(txn_id: str, timestamp: str) -> dict[str, object]:
    return {
        "txn_id": txn_id,
        "member_id_primary": "MEM0000001",
        "rail": "MPESA",
        "txn_type": "CHURCH_COLLECTION_IN",
        "timestamp": timestamp,
        "counterparty_type": "CHURCH",
        "counterparty_id_hash": "CP",
        "account_id_dr": "SRC",
        "account_id_cr": "ACC",
    }


if __name__ == "__main__":
    unittest.main()
