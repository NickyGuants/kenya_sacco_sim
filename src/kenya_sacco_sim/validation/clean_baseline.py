from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta


def clean_baseline_metrics(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    members = rows_by_file.get("members.csv", [])
    transactions = rows_by_file.get("transactions.csv", [])
    loans = {str(row["loan_id"]): row for row in rows_by_file.get("loans.csv", [])}
    guarantors = rows_by_file.get("guarantors.csv", [])
    if not transactions:
        return {"status": "not_applicable_milestone_1"}

    member_count = len(members)
    structuring_members = _structuring_candidates(transactions, rows_by_file)
    rapid_members = _rapid_pass_through_candidates(transactions, rows_by_file)
    guarantor_counts = Counter(
        str(row["guarantor_member_id"])
        for row in guarantors
        if loans.get(str(row["loan_id"]), {}).get("performing_status") != "CLOSED"
    )
    guarantor_ring_members = {member_id for member_id, count in guarantor_counts.items() if count > 5}

    return {
        "clean_structuring_candidate_count": len(structuring_members),
        "clean_structuring_candidate_share": round(len(structuring_members) / member_count, 4) if member_count else 0,
        "clean_rapid_pass_through_candidate_count": len(rapid_members),
        "clean_rapid_pass_through_candidate_share": round(len(rapid_members) / member_count, 4) if member_count else 0,
        "clean_guarantor_ring_candidate_count": len(guarantor_ring_members),
        "clean_guarantor_ring_candidate_share": round(len(guarantor_ring_members) / member_count, 4) if member_count else 0,
    }


def _structuring_candidates(transactions: list[dict[str, object]], rows_by_file: dict[str, list[dict[str, object]]]) -> set[str]:
    member_accounts = _member_accounts(rows_by_file)
    deposits_by_member: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if not member_id or str(txn["account_id_cr"]) not in member_accounts[member_id]:
            continue
        amount = float(txn["amount_kes"])
        if amount >= 100_000:
            continue
        if str(txn["txn_type"]) not in {"FOSA_CASH_DEPOSIT", "BUSINESS_SETTLEMENT_IN", "MPESA_PAYBILL_IN", "PESALINK_IN"}:
            continue
        deposits_by_member[member_id].append((datetime.fromisoformat(str(txn["timestamp"])), amount))

    candidates: set[str] = set()
    for member_id, deposits in deposits_by_member.items():
        deposits.sort(key=lambda item: item[0])
        for start_index, (start_ts, _) in enumerate(deposits):
            window = [(ts, amount) for ts, amount in deposits[start_index:] if ts <= start_ts + timedelta(days=7)]
            if len(window) >= 5 and sum(amount for _, amount in window) >= 300_000:
                candidates.add(member_id)
                break
    return candidates


def _rapid_pass_through_candidates(transactions: list[dict[str, object]], rows_by_file: dict[str, list[dict[str, object]]]) -> set[str]:
    member_accounts = _member_accounts(rows_by_file)
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)

    candidates: set[str] = set()
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        accounts = member_accounts[member_id]
        for inbound in rows:
            inbound_amount = float(inbound["amount_kes"])
            if inbound_amount < 100_000 or str(inbound["account_id_cr"]) not in accounts:
                continue
            inbound_ts = datetime.fromisoformat(str(inbound["timestamp"]))
            cutoff = inbound_ts + timedelta(hours=48)
            outbound_amount = 0.0
            counterparties: set[str] = set()
            for outbound in rows:
                outbound_ts = datetime.fromisoformat(str(outbound["timestamp"]))
                if outbound_ts <= inbound_ts or outbound_ts > cutoff:
                    continue
                if str(outbound["account_id_dr"]) not in accounts:
                    continue
                outbound_amount += float(outbound["amount_kes"])
                if outbound.get("counterparty_id_hash"):
                    counterparties.add(str(outbound["counterparty_id_hash"]))
            if outbound_amount >= inbound_amount * 0.75 and len(counterparties) >= 2:
                candidates.add(member_id)
                break
    return candidates


def _member_accounts(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, set[str]]:
    by_member: dict[str, set[str]] = defaultdict(set)
    for account in rows_by_file.get("accounts.csv", []):
        member_id = account.get("member_id")
        if member_id and account["account_owner_type"] == "MEMBER":
            by_member[str(member_id)].add(str(account["account_id"]))
    return by_member
