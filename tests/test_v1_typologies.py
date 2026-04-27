from __future__ import annotations

import random
import unittest

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.generators.typologies import (
    _distributed_pattern_start,
    _ensure_member_alert_rows,
    _target_counts,
    _wallet_funnel_inbound_counterparty_type,
    _wallet_funnel_outbound_counterparty_type,
)


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
                "WALLET_FUNNELING": 0,
                "DORMANT_REACTIVATION_ABUSE": 0,
                "REMITTANCE_LAYERING": 0,
                "CHURCH_CHARITY_MISUSE": 0,
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

    def test_full_benchmark_allocates_minimum_labels_for_active_typologies(self) -> None:
        counts = _target_counts(WorldConfig(member_count=10_000, suspicious_ratio=0.01))

        self.assertEqual(sum(counts.values()), 270)
        self.assertEqual(counts["GUARANTOR_FRAUD_RING"], 30)
        self.assertEqual(counts["WALLET_FUNNELING"], 30)
        self.assertEqual(counts["DORMANT_REACTIVATION_ABUSE"], 30)
        self.assertEqual(counts["REMITTANCE_LAYERING"], 30)
        self.assertEqual(counts["CHURCH_CHARITY_MISUSE"], 30)

    def test_member_alert_rows_are_backfilled_per_pattern_member_pair(self) -> None:
        alerts = [
            _alert("ALT00000001", "PAT00000001", "STRUCTURING", "TRANSACTION", "TXN000000000001", "MEM0000001", "ACC00000001"),
            _alert("ALT00000002", "PAT00000001", "STRUCTURING", "PATTERN", "PAT00000001", "MEM0000001", "ACC00000001"),
            _alert("ALT00000003", "PAT00000002", "WALLET_FUNNELING", "MEMBER", "MEM0000002", "MEM0000002", "ACC00000002"),
            _alert("ALT00000004", "PAT00000002", "WALLET_FUNNELING", "TRANSACTION", "TXN000000000002", "MEM0000002", "ACC00000002"),
        ]

        _ensure_member_alert_rows(alerts)

        member_rows = [
            (alert["pattern_id"], alert["member_id"], alert["entity_id"])
            for alert in alerts
            if alert["entity_type"] == "MEMBER"
        ]
        self.assertIn(("PAT00000001", "MEM0000001", "MEM0000001"), member_rows)
        self.assertEqual(member_rows.count(("PAT00000002", "MEM0000002", "MEM0000002")), 1)

    def test_wallet_funnel_counterparty_types_match_transaction_semantics(self) -> None:
        self.assertEqual(_wallet_funnel_inbound_counterparty_type("BUSINESS_SETTLEMENT_IN"), "MERCHANT")
        self.assertEqual(_wallet_funnel_inbound_counterparty_type("MPESA_PAYBILL_IN"), "WALLET_USER")
        self.assertEqual(_wallet_funnel_outbound_counterparty_type("PESALINK_OUT"), "BANK")
        self.assertEqual(_wallet_funnel_outbound_counterparty_type("SUPPLIER_PAYMENT_OUT"), "MERCHANT")
        self.assertEqual(_wallet_funnel_outbound_counterparty_type("WALLET_P2P_OUT"), "WALLET_USER")

    def test_distributed_pattern_start_keeps_large_decoy_sets_inside_window(self) -> None:
        config = WorldConfig(member_count=100_000)
        for index in range(1, 80):
            start = _distributed_pattern_start(config, random.Random(42 + index), index, 79, max_duration_days=2)

            self.assertGreaterEqual(start.date().isoformat(), config.start_date)
            self.assertLessEqual((start.date()).isoformat(), config.end_date)

def _alert(
    alert_id: str,
    pattern_id: str,
    typology: str,
    entity_type: str,
    entity_id: str,
    member_id: str,
    account_id: str,
) -> dict[str, object]:
    return {
        "alert_id": alert_id,
        "pattern_id": pattern_id,
        "typology": typology,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "member_id": member_id,
        "account_id": account_id,
        "txn_id": entity_id if entity_type == "TRANSACTION" else None,
        "edge_id": None,
        "start_timestamp": "2024-01-01T09:00:00+03:00",
        "end_timestamp": "2024-01-01T10:00:00+03:00",
        "severity": "HIGH",
        "truth_label": True,
        "stage": "LAYERING",
        "explanation_code": "TEST_PATTERN",
    }


if __name__ == "__main__":
    unittest.main()
