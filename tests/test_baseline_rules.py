from __future__ import annotations

import unittest

from kenya_sacco_sim.benchmark.baseline_rules import build_rule_results


class BaselineRuleTests(unittest.TestCase):
    def test_structuring_rule_detects_truth_member(self) -> None:
        accounts = [_account("ACC00000001", "MEM0000001")]
        transactions = [
            _txn(f"TXN{i:012d}", "MEM0000001", "SRC", "ACC00000001", "MPESA_PAYBILL_IN", 75_000, f"2024-05-0{i}T09:00:00+03:00")
            for i in range(1, 6)
        ]
        alerts = [_pattern_alert("ALT00000001", "PAT00000001", "STRUCTURING", "MEM0000001")]

        results = build_rule_results(transactions, accounts, alerts)

        structuring = results["STRUCTURING"]
        self.assertEqual(structuring["true_positive_count"], 1)
        self.assertEqual(structuring["false_negative_count"], 0)
        self.assertEqual(structuring["precision"], 1.0)
        self.assertEqual(structuring["recall"], 1.0)

    def test_rapid_pass_through_requires_same_account_exit(self) -> None:
        accounts = [_account("ACC00000002", "MEM0000002"), _account("ACC00000003", "MEM0000002")]
        transactions = [
            _txn("TXN000000000001", "MEM0000002", "SRC", "ACC00000002", "PESALINK_IN", 200_000, "2024-05-01T09:00:00+03:00"),
            _txn("TXN000000000002", "MEM0000002", "ACC00000002", "SINK1", "PESALINK_OUT", 90_000, "2024-05-01T12:00:00+03:00", "CP1"),
            _txn("TXN000000000003", "MEM0000002", "ACC00000002", "SINK2", "SUPPLIER_PAYMENT_OUT", 70_000, "2024-05-01T15:00:00+03:00", "CP2"),
            _txn("TXN000000000004", "MEM0000002", "ACC00000003", "SINK3", "PESALINK_OUT", 100_000, "2024-05-01T16:00:00+03:00", "CP3"),
        ]
        alerts = [_pattern_alert("ALT00000002", "PAT00000002", "RAPID_PASS_THROUGH", "MEM0000002")]

        results = build_rule_results(transactions, accounts, alerts)

        rapid = results["RAPID_PASS_THROUGH"]
        self.assertEqual(rapid["true_positive_count"], 1)
        self.assertEqual(rapid["false_negative_count"], 0)
        self.assertEqual(rapid["precision"], 1.0)
        self.assertEqual(rapid["recall"], 1.0)


def _account(account_id: str, member_id: str) -> dict[str, object]:
    return {"account_id": account_id, "member_id": member_id, "account_owner_type": "MEMBER"}


def _txn(
    txn_id: str,
    member_id: str,
    account_id_dr: str,
    account_id_cr: str,
    txn_type: str,
    amount: float,
    timestamp: str,
    counterparty_id_hash: str = "CP",
) -> dict[str, object]:
    return {
        "txn_id": txn_id,
        "member_id_primary": member_id,
        "account_id_dr": account_id_dr,
        "account_id_cr": account_id_cr,
        "txn_type": txn_type,
        "amount_kes": amount,
        "timestamp": timestamp,
        "counterparty_id_hash": counterparty_id_hash,
    }


def _pattern_alert(alert_id: str, pattern_id: str, typology: str, member_id: str) -> dict[str, object]:
    return {"alert_id": alert_id, "pattern_id": pattern_id, "typology": typology, "entity_type": "PATTERN", "member_id": member_id}


if __name__ == "__main__":
    unittest.main()
