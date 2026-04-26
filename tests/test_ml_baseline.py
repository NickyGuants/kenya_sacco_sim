from __future__ import annotations

import unittest

from kenya_sacco_sim.benchmark.ml_baseline import (
    BLOCKED_FEATURE_TOKENS,
    build_member_feature_table,
    build_ml_baseline_artifacts,
    build_ml_leakage_ablation_artifact,
    member_labels_by_typology,
)
from kenya_sacco_sim.core.config import WorldConfig


class MlBaselineTests(unittest.TestCase):
    def test_feature_builder_excludes_blocked_identifiers_and_labels(self) -> None:
        feature_table = build_member_feature_table(_ml_rows())

        for feature_name in feature_table["feature_names"]:
            lowered = feature_name.lower()
            self.assertFalse(any(token in lowered for token in BLOCKED_FEATURE_TOKENS), feature_name)
        self.assertIn("max_txns_24h", feature_table["feature_names"])
        self.assertIn("max_48h_exit_ratio", feature_table["feature_names"])
        self.assertIn("distinct_counterparty_count", feature_table["feature_names"])
        self.assertIn("persona_txn_count_ratio", feature_table["feature_names"])
        self.assertIn("shared_device_txn_share", feature_table["feature_names"])
        self.assertIn("max_members_per_used_device", feature_table["feature_names"])

    def test_member_labels_are_derived_from_alerts_truth(self) -> None:
        labels = member_labels_by_typology(_ml_rows()["alerts_truth.csv"])

        self.assertEqual(labels["STRUCTURING"], {"MEM0000001", "MEM0000002"})
        self.assertEqual(labels["RAPID_PASS_THROUGH"], set())
        self.assertEqual(labels["DEVICE_SHARING_MULE_NETWORK"], {"MEM0000003"})

    def test_ml_baseline_trains_with_member_split_and_exports_importance(self) -> None:
        results, importance = build_ml_baseline_artifacts(_ml_rows(), _split_manifest(), WorldConfig(seed=42))

        logistic = results["models"]["LogisticRegression"]["STRUCTURING"]
        forest = results["models"]["RandomForestClassifier"]["STRUCTURING"]

        self.assertEqual(logistic["status"], "trained")
        self.assertEqual(logistic["train_member_count"], 4)
        self.assertIn("train", logistic["splits"])
        self.assertIn("validation", logistic["splits"])
        self.assertEqual(forest["status"], "trained")
        self.assertEqual(importance["rankings"]["LogisticRegression"]["STRUCTURING"]["status"], "trained")
        self.assertGreater(len(importance["rankings"]["RandomForestClassifier"]["STRUCTURING"]["top_features"]), 0)

    def test_ml_baseline_skips_insufficient_labels(self) -> None:
        rows = _ml_rows()
        rows["alerts_truth.csv"] = []

        results, importance = build_ml_baseline_artifacts(rows, _split_manifest(), WorldConfig(seed=42))

        section = results["models"]["LogisticRegression"]["STRUCTURING"]
        self.assertEqual(section["status"], "skipped_insufficient_labels")
        self.assertEqual(importance["rankings"]["RandomForestClassifier"]["STRUCTURING"]["status"], "skipped_insufficient_labels")

    def test_ml_leakage_ablation_reports_rule_proxy_drops(self) -> None:
        rows = _ml_rows()
        results, _ = build_ml_baseline_artifacts(rows, _split_manifest(), WorldConfig(seed=42))

        ablation = build_ml_leakage_ablation_artifact(rows, _split_manifest(), WorldConfig(seed=42), results)

        section = ablation["models"]["LogisticRegression"]["STRUCTURING"]
        self.assertEqual(section["status"], "evaluated")
        self.assertEqual(section["ablation"], "without_typology_rule_proxy_features")
        self.assertIn("max_sub_100k_deposits_7d", ablation["rule_proxy_features_by_typology"]["STRUCTURING"])
        self.assertIn("shared_device_txn_share", ablation["rule_proxy_features_by_typology"]["DEVICE_SHARING_MULE_NETWORK"])
        self.assertIn("risk_summary", ablation)


def _split_manifest() -> dict[str, object]:
    return {
        "member_id_to_split": {
            "MEM0000001": "train",
            "MEM0000002": "train",
            "MEM0000003": "train",
            "MEM0000004": "train",
            "MEM0000005": "validation",
            "MEM0000006": "test",
        }
    }


def _ml_rows() -> dict[str, list[dict[str, object]]]:
    members = [{"member_id": f"MEM{index:07d}", "persona_type": "SME_OWNER"} for index in range(1, 7)]
    accounts = [
        {
            "account_id": f"ACC{index:08d}",
            "member_id": f"MEM{index:07d}",
            "account_owner_type": "MEMBER",
            "account_type": "FOSA_CURRENT",
        }
        for index in range(1, 7)
    ]
    transactions = []
    for index in range(1, 7):
        member_id = f"MEM{index:07d}"
        account_id = f"ACC{index:08d}"
        amount = 10_000.0 * index
        transactions.append(_txn(index * 2 - 1, member_id, "SRC", account_id, "PESALINK_IN", "PESALINK", "BANK_TRANSFER", amount))
        transactions.append(_txn(index * 2, member_id, account_id, "SINK", "SUPPLIER_PAYMENT_OUT", "MPESA", "PAYBILL", amount / 2))
    return {
        "members.csv": members,
        "accounts.csv": accounts,
        "transactions.csv": transactions,
        "devices.csv": [
            {"device_id": "DEVICE000001", "member_id": "MEM0000001", "shared_device_group": "SHARED_DEVICE_GROUP_00001"},
            {"device_id": "DEVICE000002", "member_id": "MEM0000002", "shared_device_group": None},
            {"device_id": "DEVICE000003", "member_id": "MEM0000003", "shared_device_group": "SHARED_DEVICE_GROUP_00001"},
        ],
        "loans.csv": [
            {"loan_id": "LOAN000001", "member_id": "MEM0000001", "application_date": "2024-06-15"},
            {"loan_id": "LOAN000002", "member_id": "MEM0000003", "application_date": "2024-06-20"},
        ],
        "nodes.csv": [
            {"node_id": "NODE000001", "entity_id": "MEM0000001"},
            {"node_id": "NODE000002", "entity_id": "MEM0000002"},
        ],
        "graph_edges.csv": [{"src_node_id": "NODE000001", "dst_node_id": "NODE000002"}],
        "guarantors.csv": [
            {"borrower_member_id": "MEM0000001", "guarantor_member_id": "MEM0000003"},
            {"borrower_member_id": "MEM0000002", "guarantor_member_id": "MEM0000003"},
        ],
        "alerts_truth.csv": [
            {"typology": "STRUCTURING", "member_id": "MEM0000001", "entity_type": "PATTERN"},
            {"typology": "STRUCTURING", "member_id": "MEM0000002", "entity_type": "PATTERN"},
            {"typology": "DEVICE_SHARING_MULE_NETWORK", "member_id": "MEM0000003", "entity_type": "PATTERN"},
        ],
    }


def _txn(index: int, member_id: str, account_id_dr: str, account_id_cr: str, txn_type: str, rail: str, channel: str, amount: float) -> dict[str, object]:
    return {
        "txn_id": f"TXN{index:012d}",
        "timestamp": f"2024-06-{min(index, 28):02d}T09:00:00+03:00",
        "member_id_primary": member_id,
        "account_id_dr": account_id_dr,
        "account_id_cr": account_id_cr,
        "txn_type": txn_type,
        "rail": rail,
        "channel": channel,
        "device_id": "DEVICE000001",
        "amount_kes": amount,
    }


if __name__ == "__main__":
    unittest.main()
