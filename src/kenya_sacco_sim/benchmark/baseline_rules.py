from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from kenya_sacco_sim.core.rules import (
    DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG,
    FAKE_AFFORDABILITY_RULE_CONFIG,
    GUARANTOR_FRAUD_RING_RULE_CONFIG,
    RAPID_PASS_THROUGH_RULE_CONFIG,
    STRUCTURING_RULE_CONFIG,
)


def build_rule_results(
    transactions: list[dict[str, object]],
    accounts: list[dict[str, object]],
    alerts: list[dict[str, object]],
    loans: list[dict[str, object]] | None = None,
    guarantors: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    member_accounts = member_account_ids(accounts)
    structuring = structuring_candidates(transactions, member_accounts)
    rapid = rapid_pass_through_candidates(transactions, member_accounts)
    fake_affordability = fake_affordability_candidates(transactions, member_accounts, loans or [])
    device_sharing = device_sharing_mule_candidates(transactions, member_accounts)
    guarantor_ring = guarantor_fraud_ring_candidates(guarantors or [], loans or [])
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
        "FAKE_AFFORDABILITY_BEFORE_LOAN": rule_section(
            "Loan application preceded within 30 days by high non-salary external credit share and balance growth",
            FAKE_AFFORDABILITY_RULE_CONFIG,
            fake_affordability,
            truth_by_typology["FAKE_AFFORDABILITY_BEFORE_LOAN"],
        ),
        "DEVICE_SHARING_MULE_NETWORK": rule_section(
            "Shared digital device used by >=3 members in 30 days with coordinated inbound/outbound value",
            DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG,
            device_sharing,
            truth_by_typology["DEVICE_SHARING_MULE_NETWORK"],
        ),
        "GUARANTOR_FRAUD_RING": rule_section(
            "Directed guarantee cycle among >=3 members with active guaranteed loans",
            GUARANTOR_FRAUD_RING_RULE_CONFIG,
            guarantor_ring,
            truth_by_typology["GUARANTOR_FRAUD_RING"],
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
        if not member_id or str(txn["account_id_cr"]) not in member_accounts.get(member_id, set()):
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
        if has_rapid_pass_through(rows, member_accounts.get(member_id, set())):
            candidates[member_id] = True
    return candidates


def fake_affordability_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]], loans: list[dict[str, object]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    eligible_products = set(FAKE_AFFORDABILITY_RULE_CONFIG["eligible_loan_products"])
    for loan in loans:
        member_id = str(loan["member_id"])
        if str(loan["product_code"]) not in eligible_products:
            continue
        application_ts = datetime.fromisoformat(f"{loan['application_date']}T00:00:00+03:00")
        rows = by_member.get(member_id, [])
        if has_fake_affordability_window(rows, member_accounts.get(member_id, set()), application_ts):
            candidates[member_id] = True
    return candidates


def device_sharing_mule_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_device: dict[str, list[dict[str, object]]] = defaultdict(list)
    digital_channels = set(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["digital_channels"])
    for txn in transactions:
        device_id = str(txn.get("device_id") or "")
        member_id = str(txn.get("member_id_primary") or "")
        if not device_id or not member_id or str(txn.get("channel") or "") not in digital_channels:
            continue
        by_device[device_id].append(txn)

    candidates: dict[str, object] = {}
    for rows in by_device.values():
        rows.sort(key=lambda row: str(row["timestamp"]))
        for members in _device_mule_windows(rows, member_accounts):
            for member_id in members:
                candidates[member_id] = True
    return candidates


def guarantor_fraud_ring_candidates(guarantors: list[dict[str, object]], loans: list[dict[str, object]]) -> dict[str, object]:
    active_statuses = set(GUARANTOR_FRAUD_RING_RULE_CONFIG["active_loan_statuses"])
    products = set(GUARANTOR_FRAUD_RING_RULE_CONFIG["guaranteed_products"])
    loan_by_id = {str(loan["loan_id"]): loan for loan in loans}
    graph: dict[str, set[str]] = defaultdict(set)
    for guarantee in guarantors:
        loan = loan_by_id.get(str(guarantee.get("loan_id") or ""))
        if not loan:
            continue
        if str(loan.get("performing_status") or "") not in active_statuses:
            continue
        if str(loan.get("product_code") or "") not in products:
            continue
        guarantor = str(guarantee.get("guarantor_member_id") or "")
        borrower = str(guarantee.get("borrower_member_id") or "")
        if not guarantor or not borrower or guarantor == borrower:
            continue
        graph[guarantor].add(borrower)
        graph.setdefault(borrower, set())

    min_members = int(GUARANTOR_FRAUD_RING_RULE_CONFIG["min_members_per_ring"])
    max_members = int(GUARANTOR_FRAUD_RING_RULE_CONFIG["max_members_per_ring"])
    min_edges = int(GUARANTOR_FRAUD_RING_RULE_CONFIG["min_cycle_edges"])
    candidates: dict[str, object] = {}
    for component in _strongly_connected_components(graph):
        if len(component) < min_members or len(component) > max_members:
            continue
        edge_count = sum(1 for src in component for dst in graph.get(src, set()) if dst in component)
        if edge_count < min_edges:
            continue
        for member_id in component:
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


def has_fake_affordability_window(rows: list[dict[str, object]], accounts: set[str], application_ts: datetime) -> bool:
    inbound_types = set(FAKE_AFFORDABILITY_RULE_CONFIG["inbound_txn_types"])
    excluded_stable = set(FAKE_AFFORDABILITY_RULE_CONFIG["excluded_stable_income_types"])
    start_ts = application_ts - timedelta(days=int(FAKE_AFFORDABILITY_RULE_CONFIG["lookback_days"]))
    external_inbound = 0.0
    total_inbound = 0.0
    outbound = 0.0
    for row in rows:
        timestamp = datetime.fromisoformat(str(row["timestamp"]))
        if timestamp < start_ts or timestamp >= application_ts:
            continue
        amount = float(row["amount_kes"])
        txn_type = str(row["txn_type"])
        if str(row["account_id_cr"]) in accounts:
            total_inbound += amount
            if txn_type in inbound_types and txn_type not in excluded_stable:
                external_inbound += amount
        if str(row["account_id_dr"]) in accounts:
            outbound += amount
    if total_inbound <= 0:
        return False
    external_share = external_inbound / total_inbound
    balance_growth = external_inbound - outbound
    return external_share >= float(FAKE_AFFORDABILITY_RULE_CONFIG["min_external_credit_share"]) and balance_growth >= float(FAKE_AFFORDABILITY_RULE_CONFIG["min_balance_growth_kes"])


def _device_mule_windows(rows: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> list[set[str]]:
    windows: list[set[str]] = []
    window_days = int(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["window_days"])
    inbound_types = set(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["outbound_txn_types"])
    min_members = int(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["min_members_per_device"])
    min_txns = int(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["min_device_txn_count"])
    min_total = float(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["min_total_value_kes"])
    min_outbound_share = float(DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG["min_outbound_share"])
    for start_index, start_row in enumerate(rows):
        start_ts = datetime.fromisoformat(str(start_row["timestamp"]))
        cutoff = start_ts + timedelta(days=window_days)
        window = [row for row in rows[start_index:] if datetime.fromisoformat(str(row["timestamp"])) <= cutoff]
        members = {str(row.get("member_id_primary") or "") for row in window if row.get("member_id_primary")}
        if len(members) < min_members or len(window) < min_txns:
            continue
        inbound = 0.0
        outbound = 0.0
        for row in window:
            member_id = str(row.get("member_id_primary") or "")
            accounts = member_accounts.get(member_id, set())
            amount = float(row.get("amount_kes") or 0)
            txn_type = str(row.get("txn_type") or "")
            if txn_type in inbound_types and str(row.get("account_id_cr") or "") in accounts:
                inbound += amount
            if txn_type in outbound_types and str(row.get("account_id_dr") or "") in accounts:
                outbound += amount
        total_value = inbound + outbound
        if total_value >= min_total and inbound > 0 and outbound / inbound >= min_outbound_share:
            windows.append(members)
    return windows


def _strongly_connected_components(graph: dict[str, set[str]]) -> list[set[str]]:
    index = 0
    stack: list[str] = []
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    on_stack: set[str] = set()
    components: list[set[str]] = []

    def strongconnect(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)

        for neighbor in graph.get(node, set()):
            if neighbor not in indices:
                strongconnect(neighbor)
                lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
            elif neighbor in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[neighbor])

        if lowlinks[node] == indices[node]:
            component: set[str] = set()
            while stack:
                member = stack.pop()
                on_stack.remove(member)
                component.add(member)
                if member == node:
                    break
            components.append(component)

    for node in sorted(graph):
        if node not in indices:
            strongconnect(node)
    return components


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
    "has_fake_affordability_window",
    "has_structuring_window",
    "member_account_ids",
    "fake_affordability_candidates",
    "guarantor_fraud_ring_candidates",
    "device_sharing_mule_candidates",
    "rapid_pass_through_candidates",
    "rule_section",
    "structuring_candidates",
]
