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

    def test_split_manifest_stratifies_labeled_members_for_valid_benchmark(self) -> None:
        members = [{"member_id": f"MEM{index:07d}"} for index in range(1, 10_001)]
        alerts = []
        alert_index = 1
        member_number = 1
        for typology, typology_count in [
            ("STRUCTURING", 30),
            ("RAPID_PASS_THROUGH", 30),
            ("FAKE_AFFORDABILITY_BEFORE_LOAN", 30),
            ("DEVICE_SHARING_MULE_NETWORK", 30),
        ]:
            for _ in range(typology_count):
                member_id = f"MEM{member_number:07d}"
                pattern_id = f"PAT{member_number:08d}"
                alerts.append(
                    {
                        "alert_id": f"ALT{alert_index:08d}",
                        "pattern_id": pattern_id,
                        "typology": typology,
                        "entity_type": "PATTERN_SUMMARY",
                        "member_id": member_id,
                    }
                )
                alert_index += 1
                for txn_offset in range(10):
                    alerts.append(
                        {
                            "alert_id": f"ALT{alert_index:08d}",
                            "pattern_id": pattern_id,
                            "typology": typology,
                            "entity_type": "TRANSACTION",
                            "member_id": member_id,
                            "txn_id": f"TXN{member_number:08d}{txn_offset:04d}",
                        }
                    )
                    alert_index += 1
                member_number += 1

        artifacts = build_benchmark_artifacts(
            {"members.csv": members, "transactions.csv": [], "alerts_truth.csv": alerts},
            {},
            WorldConfig(member_count=10_000, suspicious_ratio=0.01),
        )

        validity = artifacts["split_manifest.json"]["checks"]["evaluation_validity"]

        self.assertEqual(validity["status"], "valid")
        self.assertGreaterEqual(validity["min_positive_labels_per_split"], 5)
        self.assertGreaterEqual(validity["min_patterns_per_split"], 5)
        self.assertGreaterEqual(validity["min_txns_per_typology_per_split"], 10)

    def test_rule_vs_ml_comparison_artifact_is_emitted(self) -> None:
        artifacts = build_benchmark_artifacts(
            {"members.csv": [], "transactions.csv": [], "alerts_truth.csv": []},
            {},
            WorldConfig(member_count=0, suspicious_ratio=0),
        )

        comparison = artifacts["rule_vs_ml_comparison.json"]

        self.assertEqual(comparison["status"], "available")
        self.assertIn("ml_outperforms_rules", comparison)
        self.assertIn("rules_dominate", comparison)
        self.assertIn("ml_leakage_ablation.json", artifacts)

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

    def test_benchmark_evaluation_density_failure_is_error_for_full_benchmark(self) -> None:
        report = build_validation_report(
            {
                "members.csv": [],
                "accounts.csv": [],
                "nodes.csv": [],
                "graph_edges.csv": [],
            },
            WorldConfig(member_count=10_000),
            benchmark_validation={
                "evaluation_validity": {
                    "valid_for_ml_evaluation": False,
                    "smoke_only": False,
                }
            },
        )

        self.assertIn("benchmark.evaluation_label_density_low", {error["code"] for error in report["errors"]})


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
