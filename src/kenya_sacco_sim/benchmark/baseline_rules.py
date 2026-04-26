from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from kenya_sacco_sim.core.rules import RAPID_PASS_THROUGH_RULE_CONFIG, STRUCTURING_RULE_CONFIG


def build_rule_results(transactions: list[dict[str, object]], accounts: list[dict[str, object]], alerts: list[dict[str, object]]) -> dict[str, object]:
    member_accounts = member_account_ids(accounts)
    structuring = structuring_candidates(transactions, member_accounts)
    rapid = rapid_pass_through_candidates(transactions, member_accounts)
    truth_by_typology: dict[str, list[dict[str, object]]] = defaultdict(list)
    for alert in alerts:
        if alert["entity_type"] == "PATTERN":
            truth_by_typology[str(alert["typology"])].append(alert)
    return {
        "STRUCTURING": rule_section(
            ">=5 counted inbound deposits under KES 100,000 within 7 days with counted total >= KES 300,000",
            STRUCTURING_RULE_CONFIG,
            structuring,
            truth_by_typology["STRUCTURING"],
        ),
        "RAPID_PASS_THROUGH": rule_section(
            "Same-account inbound >= KES 100,000 followed within 48 hours by configured outbound types totaling >=75% to >=2 counterparties",
            RAPID_PASS_THROUGH_RULE_CONFIG,
            rapid,
            truth_by_typology["RAPID_PASS_THROUGH"],
        ),
    }


def member_account_ids(accounts: list[dict[str, object]]) -> dict[str, set[str]]:
    by_member: dict[str, set[str]] = defaultdict(set)
    for account in accounts:
        member_id = account.get("member_id")
        if member_id and account["account_owner_type"] == "MEMBER":
            by_member[str(member_id)].add(str(account["account_id"]))
    return by_member


def structuring_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    deposits_by_member: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    allowed_types = set(STRUCTURING_RULE_CONFIG["inbound_txn_types"])
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if not member_id or str(txn["account_id_cr"]) not in member_accounts[member_id]:
            continue
        if str(txn["txn_type"]) not in allowed_types:
            continue
        amount = float(txn["amount_kes"])
        if amount >= float(STRUCTURING_RULE_CONFIG["max_counted_deposit_amount_kes"]):
            continue
        deposits_by_member[member_id].append((datetime.fromisoformat(str(txn["timestamp"])), amount))
    return {member_id: True for member_id, deposits in deposits_by_member.items() if has_structuring_window(deposits)}


def rapid_pass_through_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        if has_rapid_pass_through(rows, member_accounts[member_id]):
            candidates[member_id] = True
    return candidates


def has_structuring_window(deposits: list[tuple[datetime, float]]) -> bool:
    deposits.sort(key=lambda item: item[0])
    window_days = int(STRUCTURING_RULE_CONFIG["window_days"])
    for start_index, (start_ts, _) in enumerate(deposits):
        window = [(ts, amount) for ts, amount in deposits[start_index:] if ts <= start_ts + timedelta(days=window_days)]
        if len(window) >= int(STRUCTURING_RULE_CONFIG["min_deposit_count"]) and sum(amount for _, amount in window) >= float(STRUCTURING_RULE_CONFIG["min_total_deposit_kes"]):
            return True
    return False


def has_rapid_pass_through(rows: list[dict[str, object]], accounts: set[str]) -> bool:
    inbound_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["outbound_txn_types"])
    excluded_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["excluded_txn_types"])
    for inbound in rows:
        inbound_amount = float(inbound["amount_kes"])
        inbound_account = str(inbound["account_id_cr"])
        if inbound_amount < float(RAPID_PASS_THROUGH_RULE_CONFIG["min_inbound"]) or inbound_account not in accounts or str(inbound["txn_type"]) not in inbound_types:
            continue
        inbound_ts = datetime.fromisoformat(str(inbound["timestamp"]))
        cutoff = inbound_ts + timedelta(hours=int(RAPID_PASS_THROUGH_RULE_CONFIG["window_hours"]))
        outbound_amount = 0.0
        counterparties: set[str] = set()
        for outbound in rows:
            outbound_ts = datetime.fromisoformat(str(outbound["timestamp"]))
            if outbound_ts <= inbound_ts or outbound_ts > cutoff:
                continue
            if str(outbound["txn_type"]) in excluded_types or str(outbound["txn_type"]) not in outbound_types:
                continue
            if str(outbound["account_id_dr"]) not in accounts:
                continue
            if RAPID_PASS_THROUGH_RULE_CONFIG["same_account_only"] and str(outbound["account_id_dr"]) != inbound_account:
                continue
            outbound_amount += float(outbound["amount_kes"])
            if outbound.get("counterparty_id_hash"):
                counterparties.add(str(outbound["counterparty_id_hash"]))
        retained_balance_ratio = max(0.0, (inbound_amount - outbound_amount) / inbound_amount)
        if (
            outbound_amount / inbound_amount >= float(RAPID_PASS_THROUGH_RULE_CONFIG["min_exit_ratio"])
            and retained_balance_ratio <= float(RAPID_PASS_THROUGH_RULE_CONFIG["max_retained_balance_ratio"])
            and len(counterparties) >= int(RAPID_PASS_THROUGH_RULE_CONFIG["min_counterparties"])
        ):
            return True
    return False


def rule_section(rule_definition: str, rule_config: dict[str, object], candidates: dict[str, object], truth_alerts: list[dict[str, object]]) -> dict[str, object]:
    truth_member_ids = sorted({str(alert["member_id"]) for alert in truth_alerts})
    detected = sorted(member_id for member_id in truth_member_ids if member_id in candidates)
    missed = sorted(member_id for member_id in truth_member_ids if member_id not in candidates)
    false_positives = sorted(member_id for member_id in candidates if member_id not in truth_member_ids)
    precision = len(detected) / len(candidates) if candidates else 0.0
    recall = len(detected) / len(truth_member_ids) if truth_member_ids else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "rule_definition": rule_definition,
        "rule_config": rule_config,
        "candidate_count": len(candidates),
        "candidate_member_ids": sorted(candidates),
        "truth_pattern_alert_ids": sorted(str(alert["alert_id"]) for alert in truth_alerts),
        "truth_member_ids": truth_member_ids,
        "truth_members_detected": detected,
        "truth_members_missed": missed,
        "false_positive_member_ids": false_positives,
        "true_positive_count": len(detected),
        "false_positive_count": len(false_positives),
        "false_negative_count": len(missed),
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
    }


__all__ = [
    "build_rule_results",
    "has_rapid_pass_through",
    "has_structuring_window",
    "member_account_ids",
    "rapid_pass_through_candidates",
    "rule_section",
    "structuring_candidates",
]
