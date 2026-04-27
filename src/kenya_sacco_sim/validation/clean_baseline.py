from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

from kenya_sacco_sim.core.rules import RAPID_PASS_THROUGH_RULE_CONFIG, STRUCTURING_RULE_CONFIG


def clean_baseline_metrics(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    members = rows_by_file.get("members.csv", [])
    transactions = rows_by_file.get("transactions.csv", [])
    guarantors = rows_by_file.get("guarantors.csv", [])
    if not transactions:
        return {"status": "not_applicable_milestone_1"}

    member_count = len(members)
    structuring_members = _structuring_candidates(transactions, rows_by_file)
    rapid_members = _rapid_pass_through_candidates(transactions, rows_by_file)
    guarantor_ring_members = _directed_guarantor_cycle_candidates(guarantors)

    return {
        "clean_structuring_rule_definition": "Member has >=5 inbound deposits under KES 100,000 into member-owned accounts within 7 days with total >= KES 300,000. Counted txn types: FOSA_CASH_DEPOSIT, BUSINESS_SETTLEMENT_IN, MPESA_PAYBILL_IN, PESALINK_IN.",
        "clean_structuring_rule_config": STRUCTURING_RULE_CONFIG,
        "clean_structuring_candidate_count": len(structuring_members),
        "clean_structuring_candidate_share": round(len(structuring_members) / member_count, 4) if member_count else 0,
        "clean_structuring_candidate_member_ids": sorted(structuring_members),
        "clean_rapid_pass_through_rule_definition": "Same-account inbound credit >= KES 100,000 into a member-owned account followed within 48 hours by configured outbound transaction types totaling >=75% of that inbound amount to >=2 distinct counterparties.",
        "clean_rapid_pass_through_rule_config": RAPID_PASS_THROUGH_RULE_CONFIG,
        "clean_rapid_pass_through_candidate_count": len(rapid_members),
        "clean_rapid_pass_through_candidate_share": round(len(rapid_members) / member_count, 4) if member_count else 0,
        "clean_rapid_pass_through_candidate_member_ids": sorted(rapid_members),
        "clean_guarantor_ring_rule_definition": "Directed guarantee cycle exists where member A guarantees member B directly or indirectly and the path returns to member A.",
        "clean_guarantor_ring_candidate_count": len(guarantor_ring_members),
        "clean_guarantor_ring_candidate_share": round(len(guarantor_ring_members) / member_count, 4) if member_count else 0,
        "clean_guarantor_ring_candidate_member_ids": sorted(guarantor_ring_members),
    }


def _structuring_candidates(transactions: list[dict[str, object]], rows_by_file: dict[str, list[dict[str, object]]]) -> set[str]:
    member_accounts = _member_accounts(rows_by_file)
    deposits_by_member: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    allowed_types = set(STRUCTURING_RULE_CONFIG["inbound_txn_types"])
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if not member_id or str(txn["account_id_cr"]) not in member_accounts[member_id]:
            continue
        amount = float(txn["amount_kes"])
        if amount >= float(STRUCTURING_RULE_CONFIG["max_counted_deposit_amount_kes"]):
            continue
        if str(txn["txn_type"]) not in allowed_types:
            continue
        deposits_by_member[member_id].append((datetime.fromisoformat(str(txn["timestamp"])), amount))

    candidates: set[str] = set()
    window_days = int(STRUCTURING_RULE_CONFIG["window_days"])
    min_count = int(STRUCTURING_RULE_CONFIG["min_deposit_count"])
    min_total = float(STRUCTURING_RULE_CONFIG["min_total_deposit_kes"])
    for member_id, deposits in deposits_by_member.items():
        deposits.sort(key=lambda item: item[0])
        total = 0.0
        right = 0
        for left, (start_ts, _) in enumerate(deposits):
            while right < len(deposits) and deposits[right][0] <= start_ts + timedelta(days=window_days):
                total += deposits[right][1]
                right += 1
            if right - left >= min_count and total >= min_total:
                candidates.add(member_id)
                break
            total -= deposits[left][1]
    return candidates


def _rapid_pass_through_candidates(transactions: list[dict[str, object]], rows_by_file: dict[str, list[dict[str, object]]]) -> set[str]:
    member_accounts = _member_accounts(rows_by_file)
    inbound_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["outbound_txn_types"])
    excluded_types = set(RAPID_PASS_THROUGH_RULE_CONFIG["excluded_txn_types"])
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
                candidates.add(member_id)
                break
    return candidates


def _directed_guarantor_cycle_candidates(guarantors: list[dict[str, object]]) -> set[str]:
    graph: dict[str, set[str]] = defaultdict(set)
    for row in guarantors:
        graph[str(row["guarantor_member_id"])].add(str(row["borrower_member_id"]))

    candidates: set[str] = set()
    for start in graph:
        visited: set[str] = set()
        stack = list(graph[start])
        while stack:
            current = stack.pop()
            if current == start:
                candidates.add(start)
                break
            if current in visited:
                continue
            visited.add(current)
            stack.extend(graph.get(current, set()))
    return candidates


def _member_accounts(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, set[str]]:
    by_member: dict[str, set[str]] = defaultdict(set)
    for account in rows_by_file.get("accounts.csv", []):
        member_id = account.get("member_id")
        if member_id and account["account_owner_type"] == "MEMBER":
            by_member[str(member_id)].add(str(account["account_id"]))
    return by_member
