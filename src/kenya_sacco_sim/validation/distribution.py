from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from statistics import median

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.core.models import ValidationFinding


def validate_distribution(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> tuple[list[ValidationFinding], dict[str, object]]:
    transactions = rows_by_file.get("transactions.csv")
    members = rows_by_file.get("members.csv", [])
    if not transactions:
        return [], {"status": "not_applicable_milestone_1"}

    findings: list[ValidationFinding] = []
    member_by_id = {str(row["member_id"]): row for row in members}
    active_members = {str(row["member_id_primary"]) for row in transactions if row.get("member_id_primary")}
    rail_counts = Counter(str(row["rail"]) for row in transactions)
    txn_type_counts = Counter(str(row["txn_type"]) for row in transactions)
    month_counts = Counter(datetime.fromisoformat(str(row["timestamp"])).month for row in transactions)
    cash_count = rail_counts["CASH_BRANCH"] + rail_counts["CASH_AGENT"]
    total_txns = len(transactions)
    cash_share = cash_count / total_txns if total_txns else 0.0
    active_member_share = len(active_members) / len(members) if members else 0.0
    counterparty_hash_eligible = [row for row in transactions if row.get("counterparty_type") not in {"SOURCE", "SINK"}]
    counterparty_hash_count = sum(1 for row in counterparty_hash_eligible if row.get("counterparty_id_hash"))
    counterparty_hash_coverage = counterparty_hash_count / len(counterparty_hash_eligible) if counterparty_hash_eligible else 1.0
    source_concentration = _max_external_concentration(transactions, "account_id_dr", "SOURCE_ACCOUNT", rows_by_file)
    sink_concentration = _max_external_concentration(transactions, "account_id_cr", "SINK_ACCOUNT", rows_by_file)

    persona_summary = _persona_summary(transactions, member_by_id, max(1, config.months))
    dormant_summary = _dormant_lifecycle_summary(rows_by_file, member_by_id)
    if cash_share < 0.10:
        findings.append(ValidationFinding("warning", "distribution.cash_share_low", f"Cash rail share {cash_share:.3f} is below 0.10 target", "transactions.csv"))
    elif cash_share > 0.20:
        findings.append(ValidationFinding("warning", "distribution.cash_share_high", f"Cash rail share {cash_share:.3f} is above 0.20 target", "transactions.csv"))
    if active_member_share < 0.60:
        findings.append(ValidationFinding("warning", "distribution.active_member_share_low", f"Active member share {active_member_share:.3f} is below 0.60 review threshold", "transactions.csv"))
    if counterparty_hash_coverage < 0.70:
        findings.append(ValidationFinding("error", "distribution.counterparty_id_coverage_low", f"Counterparty hash coverage {counterparty_hash_coverage:.3f} is below 0.70", "transactions.csv"))
    if source_concentration > 0.80:
        findings.append(ValidationFinding("warning", "distribution.source_concentration_high", f"One source account handles {source_concentration:.3f} of source-funded txns", "transactions.csv"))
    if sink_concentration > 0.80:
        findings.append(ValidationFinding("warning", "distribution.sink_concentration_high", f"One sink account handles {sink_concentration:.3f} of sink-credit txns", "transactions.csv"))
    church_summary = persona_summary.get("CHURCH_ORG")
    if church_summary and int(church_summary["member_count"]) > 0:
        if float(church_summary["active_member_share"]) < 0.60:
            findings.append(ValidationFinding("error", "distribution.church_org_active_share_low", f"CHURCH_ORG active share {church_summary['active_member_share']:.3f} is below 0.60", "transactions.csv"))
        if float(church_summary["median_txns_per_member_annualized"]) < 20:
            findings.append(ValidationFinding("error", "distribution.church_org_median_txns_low", f"CHURCH_ORG annualized median txns/member {church_summary['median_txns_per_member_annualized']:.2f} is below 20", "transactions.csv"))
    if len(members) >= 1_000 and dormant_summary["dormant_member_count"]:
        dormant_share = float(dormant_summary["dormant_member_share"])
        active_dormant_share = float(dormant_summary["active_dormant_member_share"])
        if dormant_share < 0.05:
            findings.append(ValidationFinding("error", "distribution.dormant_share_low", f"Dormant member share {dormant_share:.3f} is below 0.05 target", "members.csv"))
        if dormant_share > 0.15:
            findings.append(ValidationFinding("error", "distribution.dormant_share_high", f"Dormant member share {dormant_share:.3f} is above 0.15 target", "members.csv"))
        if active_dormant_share > 0.25:
            findings.append(ValidationFinding("error", "distribution.active_dormant_share_high", f"Active dormant member share {active_dormant_share:.3f} exceeds 0.25", "transactions.csv"))
        if int(dormant_summary["dormant_transactions_without_reactivation_count"]) > 0:
            findings.append(ValidationFinding("error", "distribution.dormant_txns_without_reactivation", "Dormant members must have KYC_REFRESH and ACCOUNT_REACTIVATION before post-dormancy activity", "transactions.csv"))
        if int(dormant_summary["high_throughput_unlabeled_dormant_member_count"]) > 0:
            findings.append(ValidationFinding("error", "distribution.high_throughput_unlabeled_dormant", "Unlabeled dormant members must not show high-velocity reactivation behavior", "transactions.csv"))

    return findings, {
        "transaction_count": total_txns,
        "active_member_count": len(active_members),
        "active_member_share": round(active_member_share, 4),
        "cash_rail_count": cash_count,
        "cash_rail_share": round(cash_share, 4),
        "counterparty_id_hash_coverage": round(counterparty_hash_coverage, 4),
        "source_account_max_concentration": round(source_concentration, 4),
        "sink_account_max_concentration": round(sink_concentration, 4),
        "rail_counts": dict(sorted(rail_counts.items())),
        "txn_type_counts": dict(sorted(txn_type_counts.items())),
        "monthly_transaction_counts": {str(month): month_counts[month] for month in range(1, 13)},
        "persona_summary": persona_summary,
        "dormant_lifecycle": dormant_summary,
    }


def _max_external_concentration(transactions: list[dict[str, object]], column: str, account_type: str, rows_by_file: dict[str, list[dict[str, object]]]) -> float:
    account_types = {str(row["account_id"]): str(row["account_type"]) for row in rows_by_file.get("accounts.csv", [])}
    counts = Counter(str(row[column]) for row in transactions if account_types.get(str(row[column])) == account_type)
    total = sum(counts.values())
    if not total:
        return 0.0
    return max(counts.values()) / total


def _persona_summary(transactions: list[dict[str, object]], member_by_id: dict[str, dict[str, object]], months: int) -> dict[str, dict[str, object]]:
    by_persona: dict[str, dict[str, object]] = defaultdict(lambda: {"txns": 0, "members": set(), "cash_txns": 0, "wallet_txns": 0})
    member_counts = Counter(str(member["persona_type"]) for member in member_by_id.values())
    txns_by_member = Counter(str(txn.get("member_id_primary") or "") for txn in transactions if txn.get("member_id_primary"))
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        member = member_by_id.get(member_id)
        if not member:
            continue
        persona = str(member["persona_type"])
        row = by_persona[persona]
        row["txns"] = int(row["txns"]) + 1
        row["members"].add(member_id)
        if txn["rail"] in {"CASH_AGENT", "CASH_BRANCH"}:
            row["cash_txns"] = int(row["cash_txns"]) + 1
        if txn["rail"] in {"MPESA", "AIRTEL_MONEY"}:
            row["wallet_txns"] = int(row["wallet_txns"]) + 1

    summary: dict[str, dict[str, object]] = {}
    for persona in sorted(set(member_counts) | set(by_persona)):
        row = by_persona[persona]
        txns = int(row["txns"])
        members = row["members"]
        member_count = member_counts[persona]
        member_txn_counts = [txns_by_member[member_id] for member_id, member in member_by_id.items() if str(member["persona_type"]) == persona]
        median_txns = round(median(member_txn_counts), 2) if member_txn_counts else 0
        summary[persona] = {
            "member_count": member_count,
            "active_members": len(members),
            "active_member_share": round(len(members) / member_count, 4) if member_count else 0,
            "txns": txns,
            "txns_per_active_member": round(txns / len(members), 2) if members else 0,
            "median_txns_per_member": median_txns,
            "median_txns_per_member_annualized": round(median_txns * 12 / months, 2) if months else median_txns,
            "cash_share": round(int(row["cash_txns"]) / txns, 4) if txns else 0,
            "wallet_share": round(int(row["wallet_txns"]) / txns, 4) if txns else 0,
        }
    return summary


def _dormant_lifecycle_summary(rows_by_file: dict[str, list[dict[str, object]]], member_by_id: dict[str, dict[str, object]]) -> dict[str, object]:
    dormant_members = {member_id for member_id, member in member_by_id.items() if _truthy(member.get("dormant_flag"))}
    transactions_by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in rows_by_file.get("transactions.csv", []):
        member_id = str(txn.get("member_id_primary") or "")
        if member_id in dormant_members:
            transactions_by_member[member_id].append(txn)
    active_dormant_members = {member_id for member_id, rows in transactions_by_member.items() if rows}
    suspicious_dormant_members = {
        str(alert.get("member_id") or "")
        for alert in rows_by_file.get("alerts_truth.csv", [])
        if alert.get("member_id") and str(alert.get("member_id")) in dormant_members
    }
    without_reactivation: set[str] = set()
    high_throughput_unlabeled: set[str] = set()
    reactivated_dormant_members: set[str] = set()
    for member_id, rows in transactions_by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        seen_kyc = False
        seen_reactivation = False
        total_value = 0.0
        for row in rows:
            txn_type = str(row.get("txn_type") or "")
            if txn_type == "KYC_REFRESH":
                seen_kyc = True
                total_value += float(row.get("amount_kes") or 0)
                continue
            if txn_type == "ACCOUNT_REACTIVATION":
                seen_reactivation = True
                total_value += float(row.get("amount_kes") or 0)
                continue
            total_value += float(row.get("amount_kes") or 0)
            if not (seen_kyc and seen_reactivation):
                without_reactivation.add(member_id)
        if seen_kyc and seen_reactivation:
            reactivated_dormant_members.add(member_id)
        if member_id not in suspicious_dormant_members and len(rows) > 15 and total_value > 300_000:
            high_throughput_unlabeled.add(member_id)
    dormant_count = len(dormant_members)
    return {
        "dormant_member_count": dormant_count,
        "dormant_member_share": round(dormant_count / len(member_by_id), 4) if member_by_id else 0.0,
        "active_dormant_member_count": len(active_dormant_members),
        "active_dormant_member_share": round(len(active_dormant_members) / dormant_count, 4) if dormant_count else 0.0,
        "reactivated_dormant_member_count": len(reactivated_dormant_members),
        "dormant_transactions_without_reactivation_count": len(without_reactivation),
        "dormant_transactions_without_reactivation_member_ids": sorted(without_reactivation)[:50],
        "high_throughput_unlabeled_dormant_member_count": len(high_throughput_unlabeled),
        "high_throughput_unlabeled_dormant_member_ids": sorted(high_throughput_unlabeled)[:50],
    }


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}
