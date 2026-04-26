from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime

from kenya_sacco_sim.core.models import ValidationFinding


def validate_distribution(rows_by_file: dict[str, list[dict[str, object]]]) -> tuple[list[ValidationFinding], dict[str, object]]:
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

    persona_summary = _persona_summary(transactions, member_by_id)
    if cash_share < 0.10:
        findings.append(ValidationFinding("warning", "distribution.cash_share_low", f"Cash rail share {cash_share:.3f} is below 0.10 target", "transactions.csv"))
    elif cash_share > 0.20:
        findings.append(ValidationFinding("warning", "distribution.cash_share_high", f"Cash rail share {cash_share:.3f} is above 0.20 target", "transactions.csv"))
    if active_member_share < 0.60:
        findings.append(ValidationFinding("warning", "distribution.active_member_share_low", f"Active member share {active_member_share:.3f} is below 0.60 review threshold", "transactions.csv"))

    return findings, {
        "transaction_count": total_txns,
        "active_member_count": len(active_members),
        "active_member_share": round(active_member_share, 4),
        "cash_rail_count": cash_count,
        "cash_rail_share": round(cash_share, 4),
        "rail_counts": dict(sorted(rail_counts.items())),
        "txn_type_counts": dict(sorted(txn_type_counts.items())),
        "monthly_transaction_counts": {str(month): month_counts[month] for month in range(1, 13)},
        "persona_summary": persona_summary,
    }


def _persona_summary(transactions: list[dict[str, object]], member_by_id: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
    by_persona: dict[str, dict[str, object]] = defaultdict(lambda: {"txns": 0, "members": set(), "cash_txns": 0, "wallet_txns": 0})
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
    for persona, row in sorted(by_persona.items()):
        txns = int(row["txns"])
        members = row["members"]
        summary[persona] = {
            "active_members": len(members),
            "txns": txns,
            "txns_per_active_member": round(txns / len(members), 2) if members else 0,
            "cash_share": round(int(row["cash_txns"]) / txns, 4) if txns else 0,
            "wallet_share": round(int(row["wallet_txns"]) / txns, 4) if txns else 0,
        }
    return summary
