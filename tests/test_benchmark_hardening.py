from __future__ import annotations

import unittest

from kenya_sacco_sim.benchmark.artifacts import build_benchmark_artifacts
from kenya_sacco_sim.benchmark.baseline_rules import guarantor_fraud_ring_candidates, wallet_funneling_candidates
from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.validation.distribution import validate_distribution
from kenya_sacco_sim.validation.labels import validate_labels
from kenya_sacco_sim.validation.report import build_validation_report


class BenchmarkHardeningTests(unittest.TestCase):
    def test_wallet_funneling_rule_detects_multi_wallet_fan_in_and_dispersion(self) -> None:
        account_id = "ACC00000001"
        accounts_by_member = {"MEM0000001": {account_id}}
        transactions = []
        for index in range(6):
            transactions.append(
                {
                    "txn_id": f"TXN{index + 1:012d}",
                    "timestamp": f"2024-04-{index + 1:02d}T10:00:00+03:00",
                    "member_id_primary": "MEM0000001",
                    "account_id_dr": "SRC",
                    "account_id_cr": account_id,
                    "txn_type": "MPESA_PAYBILL_IN",
                    "amount_kes": 70_000,
                    "counterparty_id_hash": f"CP_IN_{index}",
                }
            )
        for index in range(2):
            transactions.append(
                {
                    "txn_id": f"TXN{index + 7:012d}",
                    "timestamp": f"2024-04-08T1{index}:00:00+03:00",
                    "member_id_primary": "MEM0000001",
                    "account_id_dr": account_id,
                    "account_id_cr": "SINK",
                    "txn_type": "MPESA_WALLET_TOPUP" if index == 0 else "PESALINK_OUT",
                    "amount_kes": 130_000,
                    "counterparty_id_hash": f"CP_OUT_{index}",
                }
            )

        candidates = wallet_funneling_candidates(transactions, accounts_by_member)

        self.assertEqual(candidates, {"MEM0000001": True})

    def test_wallet_funneling_rule_ignores_low_fanout_near_miss(self) -> None:
        account_id = "ACC00000001"
        accounts_by_member = {"MEM0000001": {account_id}}
        transactions = []
        for index in range(6):
            transactions.append(
                {
                    "txn_id": f"TXN{index + 1:012d}",
                    "timestamp": f"2024-04-{index + 1:02d}T10:00:00+03:00",
                    "member_id_primary": "MEM0000001",
                    "account_id_dr": "SRC",
                    "account_id_cr": account_id,
                    "txn_type": "MPESA_PAYBILL_IN",
                    "amount_kes": 70_000,
                    "counterparty_id_hash": "CP_SHARED" if index < 4 else f"CP_IN_{index}",
                }
            )
        transactions.append(
            {
                "txn_id": "TXN000000000007",
                "timestamp": "2024-04-08T10:00:00+03:00",
                "member_id_primary": "MEM0000001",
                "account_id_dr": account_id,
                "account_id_cr": "SINK",
                "txn_type": "MPESA_WALLET_TOPUP",
                "amount_kes": 290_000,
                "counterparty_id_hash": "CP_OUT_ONLY",
            }
        )

        candidates = wallet_funneling_candidates(transactions, accounts_by_member)

        self.assertEqual(candidates, {})

    def test_wallet_funneling_rule_flags_legitimate_chama_payout_false_positive(self) -> None:
        account_id = "ACC00000001"
        accounts_by_member = {"MEM0000001": {account_id}}
        transactions = []
        for index in range(7):
            transactions.append(
                {
                    "txn_id": f"TXN{index + 1:012d}",
                    "timestamp": f"2024-06-{index + 1:02d}T10:00:00+03:00",
                    "member_id_primary": "MEM0000001",
                    "account_id_dr": "SRC",
                    "account_id_cr": account_id,
                    "txn_type": ["MPESA_PAYBILL_IN", "WALLET_P2P_IN", "BUSINESS_SETTLEMENT_IN"][index % 3],
                    "amount_kes": 75_000,
                    "counterparty_id_hash": f"CHAMA_PAYER_{index}",
                }
            )
        for index in range(3):
            transactions.append(
                {
                    "txn_id": f"TXN{index + 8:012d}",
                    "timestamp": f"2024-06-08T1{index}:00:00+03:00",
                    "member_id_primary": "MEM0000001",
                    "account_id_dr": account_id,
                    "account_id_cr": "SINK",
                    "txn_type": ["SUPPLIER_PAYMENT_OUT", "PESALINK_OUT", "WALLET_P2P_OUT"][index],
                    "amount_kes": 110_000,
                    "counterparty_id_hash": f"LEGIT_PAYOUT_{index}",
                }
            )

        candidates = wallet_funneling_candidates(transactions, accounts_by_member)

        self.assertEqual(candidates, {"MEM0000001": True})

    def test_guarantor_fraud_ring_rule_detects_directed_cycle(self) -> None:
        loans = [
            _loan("LOAN000001", "MEM0000001"),
            _loan("LOAN000002", "MEM0000002"),
            _loan("LOAN000003", "MEM0000003"),
        ]
        guarantors = [
            _guarantee("GUA000001", "LOAN000002", "MEM0000002", "MEM0000001"),
            _guarantee("GUA000002", "LOAN000003", "MEM0000003", "MEM0000002"),
            _guarantee("GUA000003", "LOAN000001", "MEM0000001", "MEM0000003"),
        ]

        candidates = guarantor_fraud_ring_candidates(guarantors, loans)

        self.assertEqual(set(candidates), {"MEM0000001", "MEM0000002", "MEM0000003"})

    def test_guarantor_fraud_ring_rule_ignores_two_member_reciprocal_near_miss(self) -> None:
        loans = [
            _loan("LOAN000001", "MEM0000001"),
            _loan("LOAN000002", "MEM0000002"),
        ]
        guarantors = [
            _guarantee("GUA000001", "LOAN000002", "MEM0000002", "MEM0000001"),
            _guarantee("GUA000002", "LOAN000001", "MEM0000001", "MEM0000002"),
        ]

        candidates = guarantor_fraud_ring_candidates(guarantors, loans)

        self.assertEqual(candidates, {})

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
        rule_results = {
            "near_miss_disclosure": {
                "status": "available",
                "near_miss_member_count": 2,
                "near_miss_transaction_count": 9,
                "families": {
                    "legitimate_structuring_like": {
                        "target_typology": "STRUCTURING",
                        "expected_rule_effect": "false_positive_pressure",
                        "member_count": 2,
                        "transaction_count": 9,
                    }
                },
            }
        }
        artifacts = build_benchmark_artifacts(
            {"members.csv": [], "transactions.csv": [], "alerts_truth.csv": []},
            rule_results,
            WorldConfig(member_count=0, suspicious_ratio=0),
        )

        baseline = artifacts["baseline_model_results.json"]
        comparison = artifacts["rule_vs_ml_comparison.json"]

        self.assertEqual(baseline["near_miss_disclosure"]["near_miss_member_count"], 2)
        self.assertEqual(comparison["status"], "available")
        self.assertEqual(comparison["claim_status"], "descriptive_not_ml_superiority_evidence")
        self.assertIn("ablation_risk_summary", comparison)
        self.assertIn("confounder_risk_summary", comparison)
        self.assertIn("ml_outperforms_rules", comparison)
        self.assertIn("rules_dominate", comparison)
        self.assertIn("benchmark_confounder_diagnostics.json", artifacts)
        self.assertIn("ml_leakage_ablation.json", artifacts)
        self.assertIn("Rule-Proxy Dependence", artifacts["dataset_card.md"])
        self.assertIn("Near-Miss And Negative-Control Coverage", artifacts["dataset_card.md"])

    def test_temporal_confounder_flags_month_share_above_40pct(self) -> None:
        transactions = [
            {
                "txn_id": f"TXN{index:012d}",
                "member_id_primary": "MEM0000001",
                "timestamp": f"2024-07-{index:02d}T10:00:00+03:00",
                "account_id_dr": "SRC",
                "account_id_cr": "ACC00000001",
                "amount_kes": 50_000,
                "rail": "CASH_BRANCH",
                "channel": "BRANCH",
                "txn_type": "FOSA_CASH_DEPOSIT",
                "counterparty_id_hash": f"CP{index:04d}",
            }
            for index in range(1, 12)
        ]
        alerts = [
            {
                "alert_id": f"ALT{index:08d}",
                "typology": "STRUCTURING",
                "member_id": "MEM0000001",
                "txn_id": transaction["txn_id"],
                "pattern_id": "PAT00000001",
                "entity_type": "TRANSACTION",
            }
            for index, transaction in enumerate(transactions, start=1)
        ]

        artifacts = build_benchmark_artifacts(
            {
                "members.csv": [{"member_id": "MEM0000001", "persona_type": "SME_OWNER"}],
                "accounts.csv": [{"account_id": "ACC00000001", "member_id": "MEM0000001"}],
                "transactions.csv": transactions,
                "alerts_truth.csv": alerts,
            },
            {},
            WorldConfig(member_count=1, suspicious_ratio=1),
        )

        temporal = artifacts["benchmark_confounder_diagnostics.json"]["temporal_label_concentration"]

        self.assertTrue(temporal["review_required"])
        self.assertIn("max_month_share > 0.40", temporal["review_rule"])
        self.assertIn("STRUCTURING", temporal["flagged_typologies"])

    def test_temporal_confounder_flags_too_few_active_months(self) -> None:
        transactions = []
        for index, month in enumerate([1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2], start=1):
            transactions.append(
                {
                    "txn_id": f"TXN{index:012d}",
                    "member_id_primary": "MEM0000001",
                    "timestamp": f"2024-{month:02d}-05T10:00:00+03:00",
                    "account_id_dr": "SRC",
                    "account_id_cr": "ACC00000001",
                    "amount_kes": 50_000,
                    "rail": "MPESA",
                    "channel": "PAYBILL",
                    "txn_type": "MPESA_PAYBILL_IN",
                    "counterparty_id_hash": f"CP{index:04d}",
                }
            )
        alerts = [
            {
                "alert_id": f"ALT{index:08d}",
                "typology": "WALLET_FUNNELING",
                "member_id": "MEM0000001",
                "txn_id": transaction["txn_id"],
                "pattern_id": "PAT00000001",
                "entity_type": "TRANSACTION",
            }
            for index, transaction in enumerate(transactions, start=1)
        ]

        artifacts = build_benchmark_artifacts(
            {
                "members.csv": [{"member_id": "MEM0000001", "persona_type": "SME_OWNER"}],
                "accounts.csv": [{"account_id": "ACC00000001", "member_id": "MEM0000001"}],
                "transactions.csv": transactions,
                "alerts_truth.csv": alerts,
            },
            {},
            WorldConfig(member_count=1, suspicious_ratio=1),
        )

        temporal = artifacts["benchmark_confounder_diagnostics.json"]["temporal_label_concentration"]

        self.assertTrue(temporal["review_required"])
        self.assertIn("active_month_count < 10", temporal["review_rule"])
        self.assertIn("WALLET_FUNNELING", temporal["flagged_typologies"])

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

    def test_validation_report_surfaces_near_miss_disclosure(self) -> None:
        report = build_validation_report(
            {
                "members.csv": [],
                "accounts.csv": [],
                "nodes.csv": [],
                "graph_edges.csv": [],
            },
            WorldConfig(member_count=0),
            typology_runtime_metrics={
                "near_miss_disclosure": {
                    "status": "available",
                    "family_count": 1,
                    "near_miss_member_count": 3,
                    "near_miss_transaction_count": 12,
                    "families": {
                        "near_rapid_low_exit": {
                            "target_typology": "RAPID_PASS_THROUGH",
                            "expected_rule_effect": "negative_control",
                            "member_count": 3,
                            "transaction_count": 12,
                        }
                    },
                }
            },
        )

        near_miss = report["near_miss_validation"]

        self.assertEqual(near_miss["status"], "available")
        self.assertEqual(near_miss["near_miss_member_count"], 3)
        self.assertIn("near_rapid_low_exit", near_miss["families"])


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


def _loan(loan_id: str, member_id: str) -> dict[str, object]:
    return {
        "loan_id": loan_id,
        "member_id": member_id,
        "product_code": "DEVELOPMENT_LOAN",
        "performing_status": "CURRENT",
    }


def _guarantee(guarantee_id: str, loan_id: str, borrower_member_id: str, guarantor_member_id: str) -> dict[str, object]:
    return {
        "guarantee_id": guarantee_id,
        "loan_id": loan_id,
        "borrower_member_id": borrower_member_id,
        "guarantor_member_id": guarantor_member_id,
    }


if __name__ == "__main__":
    unittest.main()
