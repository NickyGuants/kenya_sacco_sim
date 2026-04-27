from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from statistics import mean
from typing import Any

from kenya_sacco_sim.core.config import WorldConfig


DIGITAL_CHANNELS = {"MOBILE_APP", "USSD", "PAYBILL", "TILL", "BANK_TRANSFER"}
EXTERNAL_CREDIT_TYPES = {"PESALINK_IN", "MPESA_PAYBILL_IN", "BUSINESS_SETTLEMENT_IN", "FOSA_CASH_DEPOSIT", "CHURCH_COLLECTION_IN"}
WALLET_FUNNEL_INBOUND_TYPES = {"MPESA_PAYBILL_IN", "WALLET_P2P_IN", "BUSINESS_SETTLEMENT_IN"}
WALLET_FUNNEL_OUTBOUND_TYPES = {"MPESA_WALLET_TOPUP", "WALLET_P2P_OUT", "PESALINK_OUT", "SUPPLIER_PAYMENT_OUT"}
TYPOLOGY_NAMES = ("STRUCTURING", "RAPID_PASS_THROUGH", "FAKE_AFFORDABILITY_BEFORE_LOAN", "DEVICE_SHARING_MULE_NETWORK", "GUARANTOR_FRAUD_RING", "WALLET_FUNNELING")
BLOCKED_FEATURE_TOKENS = ("member_id", "txn_id", "reference", "pattern_id", "alert_id", "account_id", "device_id", "node_id", "edge_id", "typology", "label")
RULE_PROXY_FEATURES_BY_TYPOLOGY = {
    "STRUCTURING": {
        "sub_100k_inbound_deposit_count",
        "max_sub_100k_deposits_7d",
        "cash_deposit_count",
        "max_inflow_7d_kes",
    },
    "RAPID_PASS_THROUGH": {
        "max_48h_exit_ratio",
        "min_inbound_to_outbound_hours",
        "max_outbound_counterparties_48h",
        "max_outflow_7d_kes",
        "pesalink_share",
    },
    "FAKE_AFFORDABILITY_BEFORE_LOAN": {
        "external_credit_share_before_loan",
        "external_credit_30d_before_loan_kes",
        "balance_growth_30d_before_loan_kes",
        "days_from_latest_activity_to_loan_application",
        "has_loan_application",
        "loan_count",
    },
    "DEVICE_SHARING_MULE_NETWORK": {
        "shared_device_flag",
        "transaction_device_count",
        "shared_device_txn_share",
        "max_members_per_used_device",
        "device_peer_member_count",
        "device_network_value_kes",
    },
    "GUARANTOR_FRAUD_RING": {
        "guarantor_out_degree",
        "guarantor_in_degree",
        "graph_degree",
    },
    "WALLET_FUNNELING": {
        "wallet_inbound_count",
        "max_wallet_fan_in_counterparties_7d",
        "max_wallet_fan_in_value_7d_kes",
        "max_wallet_funnel_exit_ratio_7d",
        "counterparty_diversity_ratio",
    },
}


def build_ml_baseline_artifacts(
    rows_by_file: dict[str, list[dict[str, object]]],
    split_manifest: dict[str, object],
    config: WorldConfig,
    feature_table: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    feature_table = feature_table or build_member_feature_table(rows_by_file)
    labels_by_typology = member_labels_by_typology(rows_by_file.get("alerts_truth.csv", []))
    member_split = {str(member_id): str(split) for member_id, split in dict(split_manifest.get("member_id_to_split", {})).items()}
    ml_results, feature_importance = _train_models(feature_table, labels_by_typology, member_split, config)
    return ml_results, feature_importance


def build_ml_leakage_ablation_artifact(
    rows_by_file: dict[str, list[dict[str, object]]],
    split_manifest: dict[str, object],
    config: WorldConfig,
    full_results: dict[str, object],
    feature_table: dict[str, object] | None = None,
) -> dict[str, object]:
    feature_table = feature_table or build_member_feature_table(rows_by_file)
    labels_by_typology = member_labels_by_typology(rows_by_file.get("alerts_truth.csv", []))
    member_split = {str(member_id): str(split) for member_id, split in dict(split_manifest.get("member_id_to_split", {})).items()}
    return _rule_proxy_ablation(feature_table, labels_by_typology, member_split, config, full_results)


def build_member_feature_table(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    members = rows_by_file.get("members.csv", [])
    persona_by_member = {str(member["member_id"]): str(member.get("persona_type") or "UNKNOWN") for member in members}
    accounts_by_member: dict[str, set[str]] = defaultdict(set)
    account_owner_by_id: dict[str, str] = {}
    for account in rows_by_file.get("accounts.csv", []):
        account_id = str(account["account_id"])
        member_id = str(account.get("member_id") or "")
        account_owner_by_id[account_id] = member_id
        if member_id and account.get("account_owner_type") == "MEMBER":
            accounts_by_member[member_id].add(account_id)

    feature_names = [
        "txn_count",
        "inbound_count",
        "outbound_count",
        "total_inflow_kes",
        "total_outflow_kes",
        "inflow_outflow_ratio",
        "net_flow_kes",
        "cash_share",
        "mpesa_share",
        "pesalink_share",
        "digital_txn_count",
        "digital_device_coverage",
        "device_count",
        "shared_device_flag",
        "transaction_device_count",
        "shared_device_txn_share",
        "max_members_per_used_device",
        "device_peer_member_count",
        "device_network_value_kes",
        "distinct_counterparty_count",
        "counterparty_diversity_ratio",
        "counterparty_concentration",
        "max_txns_24h",
        "max_txns_7d",
        "avg_hours_between_txns",
        "max_inflow_7d_kes",
        "max_outflow_7d_kes",
        "max_48h_exit_ratio",
        "min_inbound_to_outbound_hours",
        "max_outbound_counterparties_48h",
        "cash_deposit_count",
        "cash_withdrawal_count",
        "sub_100k_inbound_deposit_count",
        "max_sub_100k_deposits_7d",
        "external_credit_share",
        "salary_income_share",
        "income_to_outflow_ratio",
        "round_amount_share",
        "loan_count",
        "has_loan_application",
        "days_from_latest_activity_to_loan_application",
        "external_credit_share_before_loan",
        "external_credit_30d_before_loan_kes",
        "balance_growth_30d_before_loan_kes",
        "loan_to_income_ratio_proxy",
        "graph_degree",
        "account_degree",
        "guarantor_out_degree",
        "guarantor_in_degree",
        "wallet_inbound_count",
        "max_wallet_fan_in_counterparties_7d",
        "max_wallet_fan_in_value_7d_kes",
        "max_wallet_funnel_exit_ratio_7d",
        "persona_txn_count_ratio",
        "persona_inflow_ratio",
        "persona_outflow_ratio",
        "persona_cash_share_delta",
    ]
    _assert_no_blocked_features(feature_names)
    raw: dict[str, dict[str, float]] = {str(member["member_id"]): {name: 0.0 for name in feature_names} for member in members}
    txn_timestamps_by_member: dict[str, list[datetime]] = defaultdict(list)
    txn_events_by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    counterparties_by_member: dict[str, Counter[str]] = defaultdict(Counter)
    preloan_credit_totals: dict[str, float] = defaultdict(float)
    preloan_balance_growth: dict[str, float] = defaultdict(float)
    preloan_external_credit_totals: dict[str, float] = defaultdict(float)
    salary_income_totals: dict[str, float] = defaultdict(float)
    external_credit_totals: dict[str, float] = defaultdict(float)
    round_amount_counts: Counter[str] = Counter()
    devices_used_by_member: dict[str, set[str]] = defaultdict(set)
    users_by_device: dict[str, set[str]] = defaultdict(set)
    device_events_by_member: dict[str, list[tuple[str, float]]] = defaultdict(list)

    for txn in rows_by_file.get("transactions.csv", []):
        member_id = str(txn.get("member_id_primary") or "")
        if member_id not in raw:
            continue
        features = raw[member_id]
        amount = float(txn["amount_kes"])
        member_accounts = accounts_by_member.get(member_id, set())
        debit_owned = str(txn.get("account_id_dr") or "") in member_accounts
        credit_owned = str(txn.get("account_id_cr") or "") in member_accounts
        features["txn_count"] += 1.0
        if credit_owned:
            features["inbound_count"] += 1.0
            features["total_inflow_kes"] += amount
            if str(txn.get("txn_type") or "") in {"SALARY_IN", "CHECKOFF_DEPOSIT"}:
                salary_income_totals[member_id] += amount
            if str(txn.get("txn_type") or "") in EXTERNAL_CREDIT_TYPES:
                external_credit_totals[member_id] += amount
            if str(txn.get("txn_type") or "") in WALLET_FUNNEL_INBOUND_TYPES:
                features["wallet_inbound_count"] += 1.0
            if _is_counted_deposit(txn) and amount < 100_000:
                features["sub_100k_inbound_deposit_count"] += 1.0
        if debit_owned:
            features["outbound_count"] += 1.0
            features["total_outflow_kes"] += amount
        if str(txn.get("txn_type") or "") == "FOSA_CASH_DEPOSIT":
            features["cash_deposit_count"] += 1.0
        if str(txn.get("txn_type") or "") == "FOSA_CASH_WITHDRAWAL":
            features["cash_withdrawal_count"] += 1.0
        if str(txn.get("rail") or "") in {"CASH_AGENT", "CASH_BRANCH"}:
            features["cash_share"] += 1.0
        if str(txn.get("rail") or "") == "MPESA":
            features["mpesa_share"] += 1.0
        if str(txn.get("rail") or "") == "PESALINK":
            features["pesalink_share"] += 1.0
        if str(txn.get("channel") or "") in DIGITAL_CHANNELS:
            features["digital_txn_count"] += 1.0
            if txn.get("device_id"):
                features["digital_device_coverage"] += 1.0
        device_id = str(txn.get("device_id") or "")
        if device_id:
            devices_used_by_member[member_id].add(device_id)
            users_by_device[device_id].add(member_id)
            device_events_by_member[member_id].append((device_id, amount))
        if _is_round_100(amount):
            round_amount_counts[member_id] += 1
        counterparty = str(txn.get("counterparty_id_hash") or "")
        if counterparty:
            counterparties_by_member[member_id][counterparty] += 1
        try:
            timestamp = datetime.fromisoformat(str(txn["timestamp"]))
            txn_timestamps_by_member[member_id].append(timestamp)
            txn_events_by_member[member_id].append(
                {
                    "timestamp": timestamp,
                    "amount": amount,
                    "txn_type": str(txn.get("txn_type") or ""),
                    "counterparty": counterparty,
                    "owned_account_id": str(txn.get("account_id_cr") if credit_owned else txn.get("account_id_dr") if debit_owned else ""),
                    "credit_owned": credit_owned,
                    "debit_owned": debit_owned,
                    "counted_deposit": credit_owned and _is_counted_deposit(txn) and amount < 100_000,
                    "wallet_funnel_inbound": credit_owned and str(txn.get("txn_type") or "") in WALLET_FUNNEL_INBOUND_TYPES,
                    "wallet_funnel_outbound": debit_owned and str(txn.get("txn_type") or "") in WALLET_FUNNEL_OUTBOUND_TYPES,
                }
            )
        except ValueError:
            pass

    devices_by_member = Counter(str(device.get("member_id") or "") for device in rows_by_file.get("devices.csv", []))
    shared_device_members = {str(device.get("member_id") or "") for device in rows_by_file.get("devices.csv", []) if device.get("shared_device_group")}
    for member_id, count in devices_by_member.items():
        if member_id in raw:
            raw[member_id]["device_count"] = float(count)
            raw[member_id]["shared_device_flag"] = 1.0 if member_id in shared_device_members else 0.0
    shared_transaction_devices = {device_id for device_id, users in users_by_device.items() if len(users) > 1}
    for member_id, device_ids in devices_used_by_member.items():
        if member_id not in raw:
            continue
        raw[member_id]["transaction_device_count"] = float(len(device_ids))
        raw[member_id]["max_members_per_used_device"] = float(max((len(users_by_device[device_id]) for device_id in device_ids), default=0))
        peer_members = {
            peer_member_id
            for device_id in device_ids
            for peer_member_id in users_by_device[device_id]
            if peer_member_id != member_id
        }
        raw[member_id]["device_peer_member_count"] = float(len(peer_members))
        member_device_events = device_events_by_member.get(member_id, [])
        shared_events = [(device_id, amount) for device_id, amount in member_device_events if device_id in shared_transaction_devices]
        raw[member_id]["shared_device_txn_share"] = _safe_ratio(float(len(shared_events)), float(len(member_device_events)))
        raw[member_id]["device_network_value_kes"] = sum(amount for _, amount in shared_events)

    loans_by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for loan in rows_by_file.get("loans.csv", []):
        member_id = str(loan.get("member_id") or "")
        if member_id in raw:
            loans_by_member[member_id].append(loan)
            raw[member_id]["loan_count"] += 1.0
            raw[member_id]["has_loan_application"] = 1.0

    loan_windows_by_member: dict[str, list[tuple[datetime, datetime]]] = defaultdict(list)
    for member_id, loans in loans_by_member.items():
        application_dates = [datetime.fromisoformat(f"{loan['application_date']}T00:00:00+03:00") for loan in loans]
        latest_activity_gap = _latest_activity_gap(txn_timestamps_by_member.get(member_id, []), application_dates)
        raw[member_id]["days_from_latest_activity_to_loan_application"] = float(latest_activity_gap)
        for application_ts in application_dates:
            loan_windows_by_member[member_id].append((application_ts - timedelta(days=30), application_ts))

    for txn in rows_by_file.get("transactions.csv", []):
        member_id = str(txn.get("member_id_primary") or "")
        if member_id not in loan_windows_by_member:
            continue
        try:
            timestamp = datetime.fromisoformat(str(txn["timestamp"]))
        except ValueError:
            continue
        if not any(start <= timestamp < end for start, end in loan_windows_by_member[member_id]):
            continue
        if str(txn.get("account_id_cr") or "") not in accounts_by_member.get(member_id, set()):
            continue
        amount = float(txn["amount_kes"])
        preloan_credit_totals[member_id] += amount
        preloan_balance_growth[member_id] += amount if str(txn.get("account_id_cr") or "") in accounts_by_member.get(member_id, set()) else 0.0
        if str(txn.get("txn_type") or "") in EXTERNAL_CREDIT_TYPES:
            preloan_external_credit_totals[member_id] += amount
    for txn in rows_by_file.get("transactions.csv", []):
        member_id = str(txn.get("member_id_primary") or "")
        if member_id not in loan_windows_by_member:
            continue
        try:
            timestamp = datetime.fromisoformat(str(txn["timestamp"]))
        except ValueError:
            continue
        if not any(start <= timestamp < end for start, end in loan_windows_by_member[member_id]):
            continue
        if str(txn.get("account_id_dr") or "") in accounts_by_member.get(member_id, set()):
            preloan_balance_growth[member_id] -= float(txn["amount_kes"])

    node_by_entity = {str(node["entity_id"]): str(node["node_id"]) for node in rows_by_file.get("nodes.csv", [])}
    edge_degree = Counter()
    for edge in rows_by_file.get("graph_edges.csv", []):
        edge_degree[str(edge["src_node_id"])] += 1
        edge_degree[str(edge["dst_node_id"])] += 1
    account_degree_by_member = Counter(account_owner_by_id[account_id] for account_id in account_owner_by_id if account_owner_by_id[account_id])
    for member_id in raw:
        raw[member_id]["graph_degree"] = float(edge_degree[node_by_entity.get(member_id, "")])
        raw[member_id]["account_degree"] = float(account_degree_by_member[member_id])

    for guarantee in rows_by_file.get("guarantors.csv", []):
        guarantor = str(guarantee.get("guarantor_member_id") or "")
        borrower = str(guarantee.get("borrower_member_id") or "")
        if guarantor in raw:
            raw[guarantor]["guarantor_out_degree"] += 1.0
        if borrower in raw:
            raw[borrower]["guarantor_in_degree"] += 1.0

    for member_id, events in txn_events_by_member.items():
        if member_id not in raw:
            continue
        events.sort(key=lambda event: event["timestamp"])
        raw[member_id].update(_temporal_features(events))

    finalized: dict[str, dict[str, float]] = {}
    rows: list[dict[str, object]] = []
    matrix: list[list[float]] = []
    member_ids = sorted(raw)
    for member_id in member_ids:
        features = raw[member_id]
        txn_count = features["txn_count"]
        counterparty_counts = counterparties_by_member[member_id]
        features["inflow_outflow_ratio"] = _safe_ratio(features["total_inflow_kes"], features["total_outflow_kes"])
        features["net_flow_kes"] = features["total_inflow_kes"] - features["total_outflow_kes"]
        features["cash_share"] = _safe_ratio(features["cash_share"], txn_count)
        features["mpesa_share"] = _safe_ratio(features["mpesa_share"], txn_count)
        features["pesalink_share"] = _safe_ratio(features["pesalink_share"], txn_count)
        features["digital_device_coverage"] = _safe_ratio(features["digital_device_coverage"], features["digital_txn_count"])
        features["distinct_counterparty_count"] = float(len(counterparty_counts))
        features["counterparty_diversity_ratio"] = _safe_ratio(float(len(counterparty_counts)), txn_count)
        features["counterparty_concentration"] = _safe_ratio(float(max(counterparty_counts.values())) if counterparty_counts else 0.0, txn_count)
        features["external_credit_share"] = _safe_ratio(external_credit_totals[member_id], features["total_inflow_kes"])
        features["salary_income_share"] = _safe_ratio(salary_income_totals[member_id], features["total_inflow_kes"])
        features["income_to_outflow_ratio"] = _safe_ratio(salary_income_totals[member_id], features["total_outflow_kes"])
        features["round_amount_share"] = _safe_ratio(float(round_amount_counts[member_id]), txn_count)
        features["external_credit_share_before_loan"] = _safe_ratio(preloan_external_credit_totals[member_id], preloan_credit_totals[member_id])
        features["external_credit_30d_before_loan_kes"] = preloan_external_credit_totals[member_id]
        features["balance_growth_30d_before_loan_kes"] = preloan_balance_growth[member_id]
        features["loan_to_income_ratio_proxy"] = _safe_ratio(features["loan_count"] * 50_000.0, salary_income_totals[member_id] / 12.0)
        finalized[member_id] = features

    persona_baselines = _persona_baselines(finalized, persona_by_member)
    for member_id in member_ids:
        features = finalized[member_id]
        baseline = persona_baselines.get(persona_by_member.get(member_id, "UNKNOWN"), {})
        features["persona_txn_count_ratio"] = _safe_ratio(features["txn_count"], baseline.get("txn_count", 0.0))
        features["persona_inflow_ratio"] = _safe_ratio(features["total_inflow_kes"], baseline.get("total_inflow_kes", 0.0))
        features["persona_outflow_ratio"] = _safe_ratio(features["total_outflow_kes"], baseline.get("total_outflow_kes", 0.0))
        features["persona_cash_share_delta"] = features["cash_share"] - baseline.get("cash_share", 0.0)
        row = {"member_id": member_id, **{name: round(float(features[name]), 6) for name in feature_names}}
        rows.append(row)
        matrix.append([float(row[name]) for name in feature_names])
    return {"feature_names": feature_names, "member_ids": member_ids, "rows": rows, "matrix": matrix}


def member_labels_by_typology(alerts_truth: list[dict[str, object]]) -> dict[str, set[str]]:
    labels: dict[str, set[str]] = {typology: set() for typology in TYPOLOGY_NAMES}
    for alert in alerts_truth:
        typology = str(alert.get("typology") or "")
        member_id = str(alert.get("member_id") or "")
        if typology in labels and member_id:
            labels[typology].add(member_id)
    return labels


def _temporal_features(events: list[dict[str, object]]) -> dict[str, float]:
    return {
        "max_txns_24h": float(_max_event_count(events, timedelta(hours=24))),
        "max_txns_7d": float(_max_event_count(events, timedelta(days=7))),
        "avg_hours_between_txns": _avg_hours_between(events),
        "max_inflow_7d_kes": _max_amount_window(events, timedelta(days=7), "credit_owned"),
        "max_outflow_7d_kes": _max_amount_window(events, timedelta(days=7), "debit_owned"),
        "max_48h_exit_ratio": _max_exit_ratio(events, timedelta(hours=48)),
        "min_inbound_to_outbound_hours": _min_inbound_to_outbound_hours(events),
        "max_outbound_counterparties_48h": float(_max_outbound_counterparties(events, timedelta(hours=48))),
        "max_sub_100k_deposits_7d": float(_max_flagged_count_window(events, timedelta(days=7), "counted_deposit")),
        **_wallet_funnel_features(events, timedelta(days=7), timedelta(hours=72)),
    }


def _max_event_count(events: list[dict[str, object]], window: timedelta) -> int:
    timestamps = [event["timestamp"] for event in events]
    max_count = 0
    left = 0
    for right, timestamp in enumerate(timestamps):
        while timestamp - timestamps[left] > window:
            left += 1
        max_count = max(max_count, right - left + 1)
    return max_count


def _avg_hours_between(events: list[dict[str, object]]) -> float:
    if len(events) < 2:
        return -1.0
    deltas = [
        (events[index]["timestamp"] - events[index - 1]["timestamp"]).total_seconds() / 3600.0
        for index in range(1, len(events))
    ]
    return mean(deltas)


def _max_amount_window(events: list[dict[str, object]], window: timedelta, direction_key: str) -> float:
    max_amount = 0.0
    total = 0.0
    left = 0
    for right, event in enumerate(events):
        if event.get(direction_key):
            total += float(event["amount"])
        while event["timestamp"] - events[left]["timestamp"] > window:
            if events[left].get(direction_key):
                total -= float(events[left]["amount"])
            left += 1
        max_amount = max(max_amount, total)
    return max_amount


def _max_flagged_count_window(events: list[dict[str, object]], window: timedelta, flag_key: str) -> int:
    max_count = 0
    count = 0
    left = 0
    for right, event in enumerate(events):
        if event.get(flag_key):
            count += 1
        while event["timestamp"] - events[left]["timestamp"] > window:
            if events[left].get(flag_key):
                count -= 1
            left += 1
        max_count = max(max_count, count)
    return max_count


def _max_exit_ratio(events: list[dict[str, object]], window: timedelta) -> float:
    max_ratio = 0.0
    outbound_total = 0.0
    right = 0
    for index, event in enumerate(events):
        while right < len(events) and events[right]["timestamp"] <= event["timestamp"] + window:
            if events[right].get("debit_owned"):
                outbound_total += float(events[right]["amount"])
            right += 1
        if not event.get("credit_owned") or float(event["amount"]) <= 0:
            if event.get("debit_owned"):
                outbound_total -= float(event["amount"])
            continue
        max_ratio = max(max_ratio, outbound_total / float(event["amount"]))
        if event.get("debit_owned"):
            outbound_total -= float(event["amount"])
    return max_ratio


def _min_inbound_to_outbound_hours(events: list[dict[str, object]]) -> float:
    best: float | None = None
    next_debit_ts: datetime | None = None
    for event in reversed(events):
        if event.get("debit_owned"):
            next_debit_ts = event["timestamp"]
        if event.get("credit_owned") and next_debit_ts is not None and next_debit_ts >= event["timestamp"]:
            hours = (next_debit_ts - event["timestamp"]).total_seconds() / 3600.0
            best = hours if best is None else min(best, hours)
    return best if best is not None else -1.0


def _max_outbound_counterparties(events: list[dict[str, object]], window: timedelta) -> int:
    max_count = 0
    right = 0
    counterparties: Counter[str] = Counter()
    for event in events:
        while right < len(events) and events[right]["timestamp"] <= event["timestamp"] + window:
            if events[right].get("debit_owned") and events[right].get("counterparty"):
                counterparties[str(events[right]["counterparty"])] += 1
            right += 1
        if event.get("credit_owned"):
            max_count = max(max_count, len(counterparties))
        if event.get("debit_owned") and event.get("counterparty"):
            counterparty = str(event["counterparty"])
            counterparties[counterparty] -= 1
            if counterparties[counterparty] <= 0:
                del counterparties[counterparty]
    return max_count


def _wallet_funnel_features(events: list[dict[str, object]], fan_in_window: timedelta, dispersion_window: timedelta) -> dict[str, float]:
    max_counterparties = 0
    max_value = 0.0
    max_exit_ratio = 0.0
    for account_events in _events_by_owned_account(events).values():
        outflow_times, outflow_prefix = _outbound_prefix(account_events)
        inbound_counterparties: Counter[str] = Counter()
        inbound_value = 0.0
        last_inbound_ts: datetime | None = None
        right = 0
        for left, start_event in enumerate(account_events):
            start_ts = start_event["timestamp"]
            while right < len(account_events) and account_events[right]["timestamp"] <= start_ts + fan_in_window:
                event = account_events[right]
                if event.get("wallet_funnel_inbound"):
                    inbound_value += float(event["amount"])
                    last_inbound_ts = event["timestamp"]
                    if event.get("counterparty"):
                        inbound_counterparties[str(event["counterparty"])] += 1
                right += 1
            if inbound_value > 0 and last_inbound_ts is not None:
                outbound_value = _prefix_sum_between(outflow_times, outflow_prefix, last_inbound_ts, last_inbound_ts + dispersion_window)
                max_counterparties = max(max_counterparties, len(inbound_counterparties))
                max_value = max(max_value, inbound_value)
                max_exit_ratio = max(max_exit_ratio, _safe_ratio(outbound_value, inbound_value))
            if start_event.get("wallet_funnel_inbound"):
                inbound_value -= float(start_event["amount"])
                if start_event.get("counterparty"):
                    counterparty = str(start_event["counterparty"])
                    inbound_counterparties[counterparty] -= 1
                    if inbound_counterparties[counterparty] <= 0:
                        del inbound_counterparties[counterparty]
                if start_event["timestamp"] == last_inbound_ts:
                    remaining_inbounds = [
                        event["timestamp"]
                        for event in account_events[left + 1 : right]
                        if event.get("wallet_funnel_inbound")
                    ]
                    last_inbound_ts = max(remaining_inbounds) if remaining_inbounds else None
    return {
        "max_wallet_fan_in_counterparties_7d": float(max_counterparties),
        "max_wallet_fan_in_value_7d_kes": max_value,
        "max_wallet_funnel_exit_ratio_7d": max_exit_ratio,
    }


def _events_by_owned_account(events: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for event in events:
        account_id = str(event.get("owned_account_id") or "")
        if account_id:
            grouped[account_id].append(event)
    return grouped


def _outbound_prefix(events: list[dict[str, object]]) -> tuple[list[datetime], list[float]]:
    times: list[datetime] = []
    prefix = [0.0]
    for event in events:
        if event.get("wallet_funnel_outbound"):
            times.append(event["timestamp"])
            prefix.append(prefix[-1] + float(event["amount"]))
    return times, prefix


def _prefix_sum_between(times: list[datetime], prefix: list[float], start: datetime, end: datetime) -> float:
    left = bisect_left(times, start)
    right = bisect_right(times, end)
    return prefix[right] - prefix[left]


def _persona_baselines(features_by_member: dict[str, dict[str, float]], persona_by_member: dict[str, str]) -> dict[str, dict[str, float]]:
    buckets: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for member_id, features in features_by_member.items():
        persona = persona_by_member.get(member_id, "UNKNOWN")
        for feature_name in ("txn_count", "total_inflow_kes", "total_outflow_kes", "cash_share"):
            buckets[persona][feature_name].append(float(features[feature_name]))
    return {
        persona: {feature_name: mean(values) if values else 0.0 for feature_name, values in feature_values.items()}
        for persona, feature_values in buckets.items()
    }


def _train_models(feature_table: dict[str, object], labels_by_typology: dict[str, set[str]], member_split: dict[str, str], config: WorldConfig) -> tuple[dict[str, object], dict[str, object]]:
    sklearn = _load_sklearn()
    feature_names = list(feature_table["feature_names"])
    member_ids = list(feature_table["member_ids"])
    matrix = list(feature_table["matrix"])
    results: dict[str, object] = {
        "baseline_name": "member_level_ml_v1",
        "task": "member_level_one_vs_rest_typology_detection",
        "split_key": "member_id",
        "models": {},
    }
    importances: dict[str, object] = {
        "baseline_name": "member_level_ml_v1",
        "feature_names": feature_names,
        "rankings": {},
    }
    for model_name in ("LogisticRegression", "RandomForestClassifier"):
        results["models"][model_name] = {}
        importances["rankings"][model_name] = {}
        for typology in TYPOLOGY_NAMES:
            y = [1 if member_id in labels_by_typology.get(typology, set()) else 0 for member_id in member_ids]
            split_indices = _split_indices(member_ids, member_split)
            train_indices = split_indices["train"]
            if not _has_both_classes([y[index] for index in train_indices]):
                skipped = {
                    "status": "skipped_insufficient_labels",
                    "reason": "train split must contain positive and negative examples",
                    "positive_count": sum(y[index] for index in train_indices),
                    "negative_count": len(train_indices) - sum(y[index] for index in train_indices),
                }
                results["models"][model_name][typology] = skipped
                importances["rankings"][model_name][typology] = skipped
                continue
            estimator = _make_estimator(model_name, sklearn, config.seed)
            x_train = [matrix[index] for index in train_indices]
            y_train = [y[index] for index in train_indices]
            estimator.fit(x_train, y_train)
            split_metrics = {}
            for split, indices in split_indices.items():
                split_y = [y[index] for index in indices]
                if not indices or not _has_both_classes(split_y):
                    split_metrics[split] = {
                        "status": "skipped_insufficient_labels",
                        "positive_count": sum(split_y),
                        "negative_count": len(split_y) - sum(split_y),
                    }
                    continue
                split_metrics[split] = _evaluate(estimator, [matrix[index] for index in indices], split_y, sklearn)
            results["models"][model_name][typology] = {
                "status": "trained",
                "train_member_count": len(train_indices),
                "train_positive_count": sum(y_train),
                "splits": split_metrics,
            }
            importances["rankings"][model_name][typology] = {
                "status": "trained",
                "top_features": _feature_importance(model_name, estimator, feature_names),
            }
    return results, importances


def _rule_proxy_ablation(
    feature_table: dict[str, object],
    labels_by_typology: dict[str, set[str]],
    member_split: dict[str, str],
    config: WorldConfig,
    full_results: dict[str, object],
) -> dict[str, object]:
    sklearn = _load_sklearn()
    feature_names = list(feature_table["feature_names"])
    member_ids = list(feature_table["member_ids"])
    matrix = list(feature_table["matrix"])
    split_indices = _split_indices(member_ids, member_split)
    models: dict[str, object] = {}
    max_eval_f1_drop = 0.0
    high_dependency_cases: list[dict[str, object]] = []

    for model_name in ("LogisticRegression", "RandomForestClassifier"):
        models[model_name] = {}
        for typology in TYPOLOGY_NAMES:
            removed_features = [feature for feature in feature_names if feature in RULE_PROXY_FEATURES_BY_TYPOLOGY[typology]]
            keep_indices = [index for index, feature in enumerate(feature_names) if feature not in set(removed_features)]
            y = [1 if member_id in labels_by_typology.get(typology, set()) else 0 for member_id in member_ids]
            train_indices = split_indices["train"]
            if not _has_both_classes([y[index] for index in train_indices]):
                models[model_name][typology] = {
                    "status": "skipped_insufficient_labels",
                    "removed_features": removed_features,
                }
                continue
            estimator = _make_estimator(model_name, sklearn, config.seed)
            x_train = [_select_features(matrix[index], keep_indices) for index in train_indices]
            y_train = [y[index] for index in train_indices]
            estimator.fit(x_train, y_train)
            split_rows: dict[str, object] = {}
            for split, indices in split_indices.items():
                split_y = [y[index] for index in indices]
                if not indices or not _has_both_classes(split_y):
                    split_rows[split] = {
                        "status": "skipped_insufficient_labels",
                        "positive_count": sum(split_y),
                        "negative_count": len(split_y) - sum(split_y),
                    }
                    continue
                ablated = _evaluate(estimator, [_select_features(matrix[index], keep_indices) for index in indices], split_y, sklearn)
                full = _full_split_metrics(full_results, model_name, typology, split)
                full_f1 = float(full.get("f1") or 0.0) if full else 0.0
                ablated_f1 = float(ablated.get("f1") or 0.0)
                f1_drop = max(0.0, full_f1 - ablated_f1)
                if split in {"validation", "test"}:
                    max_eval_f1_drop = max(max_eval_f1_drop, f1_drop)
                    if f1_drop >= 0.25:
                        high_dependency_cases.append(
                            {
                                "model": model_name,
                                "typology": typology,
                                "split": split,
                                "full_f1": round(full_f1, 4),
                                "ablated_f1": round(ablated_f1, 4),
                                "f1_drop": round(f1_drop, 4),
                            }
                        )
                split_rows[split] = {
                    "status": "evaluated",
                    "positive_count": ablated["positive_count"],
                    "full_precision": full.get("precision") if full else None,
                    "ablated_precision": ablated["precision"],
                    "precision_delta": _delta(ablated.get("precision"), full.get("precision") if full else None),
                    "full_recall": full.get("recall") if full else None,
                    "ablated_recall": ablated["recall"],
                    "recall_delta": _delta(ablated.get("recall"), full.get("recall") if full else None),
                    "full_f1": round(full_f1, 4),
                    "ablated_f1": round(ablated_f1, 4),
                    "f1_delta": round(ablated_f1 - full_f1, 4),
                    "f1_drop": round(f1_drop, 4),
                }
            models[model_name][typology] = {
                "status": "evaluated",
                "ablation": "without_typology_rule_proxy_features",
                "removed_features": removed_features,
                "kept_feature_count": len(keep_indices),
                "splits": split_rows,
            }

    return {
        "baseline_name": "ml_rule_proxy_ablation_v1",
        "purpose": "Estimate whether ML performance depends on direct typology rule-proxy features.",
        "interpretation": "Large validation/test F1 drops after removing rule-proxy features indicate possible overreliance on injected-rule proxies.",
        "rule_proxy_features_by_typology": {typology: sorted(features) for typology, features in RULE_PROXY_FEATURES_BY_TYPOLOGY.items()},
        "models": models,
        "risk_summary": {
            "max_validation_or_test_f1_drop": round(max_eval_f1_drop, 4),
            "high_rule_proxy_dependency": max_eval_f1_drop >= 0.25,
            "high_dependency_cases": high_dependency_cases,
        },
    }


def _full_split_metrics(full_results: dict[str, object], model_name: str, typology: str, split: str) -> dict[str, object]:
    models = full_results.get("models", {})
    if not isinstance(models, dict):
        return {}
    model = models.get(model_name, {})
    if not isinstance(model, dict):
        return {}
    section = model.get(typology, {})
    if not isinstance(section, dict):
        return {}
    splits = section.get("splits", {})
    if not isinstance(splits, dict):
        return {}
    row = splits.get(split, {})
    return row if isinstance(row, dict) and row.get("status") == "evaluated" else {}


def _select_features(values: list[float], indices: list[int]) -> list[float]:
    return [values[index] for index in indices]


def _delta(new_value: object, old_value: object) -> float | None:
    if not isinstance(new_value, (int, float)) or not isinstance(old_value, (int, float)):
        return None
    return round(float(new_value) - float(old_value), 4)


def _load_sklearn() -> dict[str, Any]:
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
        from sklearn.pipeline import make_pipeline
        from sklearn.preprocessing import StandardScaler
    except ModuleNotFoundError as exc:
        raise RuntimeError("scikit-learn is required to build ML baseline artifacts. Install project dependencies first.") from exc
    return {
        "RandomForestClassifier": RandomForestClassifier,
        "LogisticRegression": LogisticRegression,
        "StandardScaler": StandardScaler,
        "make_pipeline": make_pipeline,
        "accuracy_score": accuracy_score,
        "f1_score": f1_score,
        "precision_score": precision_score,
        "recall_score": recall_score,
        "roc_auc_score": roc_auc_score,
    }


def _make_estimator(model_name: str, sklearn: dict[str, Any], seed: int):
    if model_name == "LogisticRegression":
        return sklearn["make_pipeline"](
            sklearn["StandardScaler"](),
            sklearn["LogisticRegression"](class_weight="balanced", max_iter=1000, random_state=seed),
        )
    return sklearn["RandomForestClassifier"](n_estimators=200, class_weight="balanced", random_state=seed, n_jobs=1)


def _evaluate(estimator, x_values: list[list[float]], y_true: list[int], sklearn: dict[str, Any]) -> dict[str, object]:
    y_pred = [int(value) for value in estimator.predict(x_values)]
    probabilities = estimator.predict_proba(x_values)[:, 1]
    return {
        "status": "evaluated",
        "member_count": len(y_true),
        "positive_count": sum(y_true),
        "negative_count": len(y_true) - sum(y_true),
        "accuracy": round(float(sklearn["accuracy_score"](y_true, y_pred)), 4),
        "precision": round(float(sklearn["precision_score"](y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(sklearn["recall_score"](y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(sklearn["f1_score"](y_true, y_pred, zero_division=0)), 4),
        "roc_auc": round(float(sklearn["roc_auc_score"](y_true, probabilities)), 4),
    }


def _feature_importance(model_name: str, estimator, feature_names: list[str]) -> list[dict[str, object]]:
    if model_name == "LogisticRegression":
        coefficients = estimator.named_steps["logisticregression"].coef_[0]
        ranked = [
            {"feature": feature, "importance": round(abs(float(coef)), 6), "coefficient": round(float(coef), 6)}
            for feature, coef in zip(feature_names, coefficients)
        ]
    else:
        ranked = [
            {"feature": feature, "importance": round(float(importance), 6)}
            for feature, importance in zip(feature_names, estimator.feature_importances_)
        ]
    ranked.sort(key=lambda row: float(row["importance"]), reverse=True)
    return ranked


def _split_indices(member_ids: list[str], member_split: dict[str, str]) -> dict[str, list[int]]:
    split_indices = {"train": [], "validation": [], "test": []}
    for index, member_id in enumerate(member_ids):
        split = member_split.get(member_id)
        if split in split_indices:
            split_indices[split].append(index)
    return split_indices


def _has_both_classes(values: list[int]) -> bool:
    return bool(values) and len(set(values)) == 2


def _latest_activity_gap(txn_timestamps: list[datetime], application_dates: list[datetime]) -> int:
    gaps: list[int] = []
    for application_ts in application_dates:
        prior = [timestamp for timestamp in txn_timestamps if timestamp < application_ts]
        if prior:
            gaps.append(max(0, (application_ts - max(prior)).days))
    return min(gaps) if gaps else -1


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _is_counted_deposit(txn: dict[str, object]) -> bool:
    return str(txn.get("txn_type") or "") in {"FOSA_CASH_DEPOSIT", "BUSINESS_SETTLEMENT_IN", "MPESA_PAYBILL_IN", "PESALINK_IN"}


def _is_round_100(amount: float) -> bool:
    return abs(amount % 100.0) < 0.0001 or abs((amount % 100.0) - 100.0) < 0.0001


def _assert_no_blocked_features(feature_names: list[str]) -> None:
    for feature_name in feature_names:
        lowered = feature_name.lower()
        if any(token in lowered for token in BLOCKED_FEATURE_TOKENS):
            raise ValueError(f"Blocked leakage-prone feature name: {feature_name}")


__all__ = [
    "BLOCKED_FEATURE_TOKENS",
    "RULE_PROXY_FEATURES_BY_TYPOLOGY",
    "TYPOLOGY_NAMES",
    "build_member_feature_table",
    "build_ml_baseline_artifacts",
    "build_ml_leakage_ablation_artifact",
    "member_labels_by_typology",
]
