from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta

from kenya_sacco_sim.core.rules import (
    CHURCH_CHARITY_MISUSE_RULE_CONFIG,
    DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG,
    DORMANT_REACTIVATION_ABUSE_RULE_CONFIG,
    FAKE_AFFORDABILITY_RULE_CONFIG,
    GUARANTOR_FRAUD_RING_RULE_CONFIG,
    RAPID_PASS_THROUGH_RULE_CONFIG,
    REMITTANCE_LAYERING_RULE_CONFIG,
    STRUCTURING_RULE_CONFIG,
    WALLET_FUNNELING_RULE_CONFIG,
)


def build_rule_results(
    transactions: list[dict[str, object]],
    accounts: list[dict[str, object]],
    alerts: list[dict[str, object]],
    loans: list[dict[str, object]] | None = None,
    guarantors: list[dict[str, object]] | None = None,
    members: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    member_accounts = member_account_ids(accounts)
    member_personas = {str(member["member_id"]): str(member.get("persona_type") or "") for member in members or []}
    structuring = structuring_candidates(transactions, member_accounts)
    rapid = rapid_pass_through_candidates(transactions, member_accounts)
    fake_affordability = fake_affordability_candidates(transactions, member_accounts, loans or [])
    device_sharing = device_sharing_mule_candidates(transactions, member_accounts)
    guarantor_ring = guarantor_fraud_ring_candidates(guarantors or [], loans or [])
    wallet_funneling = wallet_funneling_candidates(transactions, member_accounts)
    dormant_abuse = dormant_reactivation_abuse_candidates(transactions, member_accounts)
    remittance_layering = remittance_layering_candidates(transactions, member_accounts)
    church_misuse = church_charity_misuse_candidates(transactions, member_accounts, member_personas)
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
        "WALLET_FUNNELING": rule_section(
            "Many wallet/paybill credits fan into one member account within 7 days and disperse quickly to multiple counterparties",
            WALLET_FUNNELING_RULE_CONFIG,
            wallet_funneling,
            truth_by_typology["WALLET_FUNNELING"],
        ),
        "DORMANT_REACTIVATION_ABUSE": rule_section(
            "Dormant account reactivation followed by large first credit and rapid 7-day fanout",
            DORMANT_REACTIVATION_ABUSE_RULE_CONFIG,
            dormant_abuse,
            truth_by_typology["DORMANT_REACTIVATION_ABUSE"],
        ),
        "REMITTANCE_LAYERING": rule_section(
            "Remittance inbound followed within 72 hours by rapid redistribution across multiple counterparties",
            REMITTANCE_LAYERING_RULE_CONFIG,
            remittance_layering,
            truth_by_typology["REMITTANCE_LAYERING"],
        ),
        "CHURCH_CHARITY_MISUSE": rule_section(
            "Church or group account receives abnormal donor inflow and disperses funds outside normal collection rhythm",
            CHURCH_CHARITY_MISUSE_RULE_CONFIG,
            church_misuse,
            truth_by_typology["CHURCH_CHARITY_MISUSE"],
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


def wallet_funneling_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        if has_wallet_funneling(rows, member_accounts.get(member_id, set())):
            candidates[member_id] = True
    return candidates


def dormant_reactivation_abuse_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        if has_dormant_reactivation_abuse(rows, member_accounts.get(member_id, set())):
            candidates[member_id] = True
    return candidates


def remittance_layering_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        if has_remittance_layering(rows, member_accounts.get(member_id, set())):
            candidates[member_id] = True
    return candidates


def church_charity_misuse_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]], member_personas: dict[str, str] | None = None) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    eligible_personas = set(CHURCH_CHARITY_MISUSE_RULE_CONFIG["candidate_personas"])
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        if member_personas and member_personas.get(member_id) not in eligible_personas:
            continue
        rows.sort(key=lambda row: str(row["timestamp"]))
        if has_church_charity_misuse(rows, member_accounts.get(member_id, set())):
            candidates[member_id] = True
    return candidates


def has_structuring_window(deposits: list[tuple[datetime, float]]) -> bool:
    deposits.sort(key=lambda item: item[0])
    window_days = int(STRUCTURING_RULE_CONFIG["window_days"])
    min_count = int(STRUCTURING_RULE_CONFIG["min_deposit_count"])
    min_total = float(STRUCTURING_RULE_CONFIG["min_total_deposit_kes"])
    total = 0.0
    right = 0
    for left, (start_ts, _) in enumerate(deposits):
        while right < len(deposits) and deposits[right][0] <= start_ts + timedelta(days=window_days):
            total += deposits[right][1]
            right += 1
        if right - left >= min_count and total >= min_total:
            return True
        total -= deposits[left][1]
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


def has_wallet_funneling(rows: list[dict[str, object]], accounts: set[str]) -> bool:
    inbound_types = set(WALLET_FUNNELING_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(WALLET_FUNNELING_RULE_CONFIG["outbound_txn_types"])
    fan_in_window = timedelta(days=int(WALLET_FUNNELING_RULE_CONFIG["window_days"]))
    dispersion_window = timedelta(hours=int(WALLET_FUNNELING_RULE_CONFIG["dispersion_window_hours"]))
    min_inbound_count = int(WALLET_FUNNELING_RULE_CONFIG["min_inbound_count"])
    min_inbound_value = float(WALLET_FUNNELING_RULE_CONFIG["min_inbound_value_kes"])
    min_inbound_counterparties = int(WALLET_FUNNELING_RULE_CONFIG["min_inbound_counterparties"])
    min_outbound_share = float(WALLET_FUNNELING_RULE_CONFIG["min_outbound_share"])
    min_outbound_counterparties = int(WALLET_FUNNELING_RULE_CONFIG["min_outbound_counterparties"])
    by_account: dict[str, list[tuple[datetime, str, float, str, str, str]]] = defaultdict(list)
    for row in rows:
        txn_type = str(row.get("txn_type") or "")
        if txn_type not in inbound_types and txn_type not in outbound_types:
            continue
        credit_account = str(row.get("account_id_cr") or "")
        debit_account = str(row.get("account_id_dr") or "")
        account_id = credit_account if credit_account in accounts else debit_account if debit_account in accounts else ""
        if not account_id:
            continue
        by_account[account_id].append(
            (
                datetime.fromisoformat(str(row["timestamp"])),
                txn_type,
                float(row["amount_kes"]),
                str(row.get("counterparty_id_hash") or ""),
                credit_account,
                debit_account,
            )
        )

    for account_id, account_rows in by_account.items():
        account_rows.sort(key=lambda item: item[0])
        outbound_windows = _wallet_outbound_windows(account_rows, outbound_types, dispersion_window)
        right = 0
        inbound_count = 0
        inbound_value = 0.0
        inbound_counterparties: Counter[str] = Counter()
        last_inbound_ts: datetime | None = None
        for left, start in enumerate(account_rows):
            start_ts = start[0]
            while right < len(account_rows) and account_rows[right][0] <= start_ts + fan_in_window:
                timestamp, txn_type, amount, counterparty, credit_account, _ = account_rows[right]
                if credit_account == account_id and txn_type in inbound_types:
                    inbound_count += 1
                    inbound_value += amount
                    last_inbound_ts = timestamp
                    if counterparty:
                        inbound_counterparties[counterparty] += 1
                right += 1
            if (
                inbound_count >= min_inbound_count
                and inbound_value >= min_inbound_value
                and len(inbound_counterparties) >= min_inbound_counterparties
                and last_inbound_ts is not None
            ):
                outbound_value, outbound_counterparties = outbound_windows.get(last_inbound_ts, (0.0, 0))
                if inbound_value > 0 and outbound_value / inbound_value >= min_outbound_share and outbound_counterparties >= min_outbound_counterparties:
                    return True
            timestamp, txn_type, amount, counterparty, credit_account, _ = start
            if credit_account == account_id and txn_type in inbound_types:
                inbound_count -= 1
                inbound_value -= amount
                if counterparty:
                    inbound_counterparties[counterparty] -= 1
                    if inbound_counterparties[counterparty] <= 0:
                        del inbound_counterparties[counterparty]
                if timestamp == last_inbound_ts:
                    last_inbound_ts = _latest_inbound_timestamp(account_rows, left + 1, right, account_id, inbound_types)
    return False


def has_dormant_reactivation_abuse(rows: list[dict[str, object]], accounts: set[str]) -> bool:
    reactivation_types = set(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["reactivation_txn_types"])
    inbound_types = set(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["outbound_txn_types"])
    min_credit = float(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["min_first_credit_kes"])
    min_exit_ratio = float(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["min_exit_ratio"])
    min_outflows = int(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["min_outflow_count"])
    min_counterparties = int(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["min_outbound_counterparties"])
    window = timedelta(days=int(DORMANT_REACTIVATION_ABUSE_RULE_CONFIG["window_days_after_reactivation"]))
    reactivation_times = [
        datetime.fromisoformat(str(row["timestamp"]))
        for row in rows
        if str(row.get("txn_type") or "") in reactivation_types and str(row.get("account_id_cr") or "") in accounts
    ]
    for reactivation_ts in reactivation_times:
        window_end = reactivation_ts + window
        for inbound in rows:
            inbound_ts = datetime.fromisoformat(str(inbound["timestamp"]))
            inbound_amount = float(inbound.get("amount_kes") or 0)
            if inbound_ts <= reactivation_ts or inbound_ts > window_end:
                continue
            if inbound_amount < min_credit or str(inbound.get("txn_type") or "") not in inbound_types or str(inbound.get("account_id_cr") or "") not in accounts:
                continue
            outbound_value = 0.0
            outbound_count = 0
            counterparties: set[str] = set()
            for outbound in rows:
                outbound_ts = datetime.fromisoformat(str(outbound["timestamp"]))
                if outbound_ts <= inbound_ts or outbound_ts > window_end:
                    continue
                if str(outbound.get("txn_type") or "") not in outbound_types or str(outbound.get("account_id_dr") or "") not in accounts:
                    continue
                outbound_value += float(outbound.get("amount_kes") or 0)
                outbound_count += 1
                if outbound.get("counterparty_id_hash"):
                    counterparties.add(str(outbound["counterparty_id_hash"]))
            if outbound_count >= min_outflows and outbound_value / inbound_amount >= min_exit_ratio and len(counterparties) >= min_counterparties:
                return True
    return False


def has_remittance_layering(rows: list[dict[str, object]], accounts: set[str]) -> bool:
    inbound_types = set(REMITTANCE_LAYERING_RULE_CONFIG["inbound_txn_types"])
    inbound_rails = set(REMITTANCE_LAYERING_RULE_CONFIG["inbound_rails"])
    outbound_types = set(REMITTANCE_LAYERING_RULE_CONFIG["outbound_txn_types"])
    window = timedelta(hours=int(REMITTANCE_LAYERING_RULE_CONFIG["window_hours"]))
    min_inbound = float(REMITTANCE_LAYERING_RULE_CONFIG["min_inbound_kes"])
    min_exit_ratio = float(REMITTANCE_LAYERING_RULE_CONFIG["min_exit_ratio"])
    min_outflows = int(REMITTANCE_LAYERING_RULE_CONFIG["min_outflow_count"])
    min_counterparties = int(REMITTANCE_LAYERING_RULE_CONFIG["min_outbound_counterparties"])
    for inbound in rows:
        inbound_amount = float(inbound.get("amount_kes") or 0)
        if inbound_amount < min_inbound:
            continue
        if str(inbound.get("txn_type") or "") not in inbound_types or str(inbound.get("rail") or "") not in inbound_rails:
            continue
        if str(inbound.get("account_id_cr") or "") not in accounts:
            continue
        inbound_ts = datetime.fromisoformat(str(inbound["timestamp"]))
        cutoff = inbound_ts + window
        outbound_value = 0.0
        outbound_count = 0
        counterparties: set[str] = set()
        for outbound in rows:
            outbound_ts = datetime.fromisoformat(str(outbound["timestamp"]))
            if outbound_ts <= inbound_ts or outbound_ts > cutoff:
                continue
            if str(outbound.get("txn_type") or "") not in outbound_types or str(outbound.get("account_id_dr") or "") not in accounts:
                continue
            outbound_value += float(outbound.get("amount_kes") or 0)
            outbound_count += 1
            if outbound.get("counterparty_id_hash"):
                counterparties.add(str(outbound["counterparty_id_hash"]))
        if outbound_count >= min_outflows and outbound_value / inbound_amount >= min_exit_ratio and len(counterparties) >= min_counterparties:
            return True
    return False


def has_church_charity_misuse(rows: list[dict[str, object]], accounts: set[str]) -> bool:
    inbound_types = set(CHURCH_CHARITY_MISUSE_RULE_CONFIG["inbound_txn_types"])
    outbound_types = set(CHURCH_CHARITY_MISUSE_RULE_CONFIG["outbound_txn_types"])
    window = timedelta(hours=int(CHURCH_CHARITY_MISUSE_RULE_CONFIG["window_hours"]))
    min_inbound = float(CHURCH_CHARITY_MISUSE_RULE_CONFIG["min_inbound_kes"])
    min_exit_ratio = float(CHURCH_CHARITY_MISUSE_RULE_CONFIG["min_exit_ratio"])
    min_outflows = int(CHURCH_CHARITY_MISUSE_RULE_CONFIG["min_outflow_count"])
    min_counterparties = int(CHURCH_CHARITY_MISUSE_RULE_CONFIG["min_outbound_counterparties"])
    for inbound in rows:
        inbound_amount = float(inbound.get("amount_kes") or 0)
        if inbound_amount < min_inbound:
            continue
        if str(inbound.get("txn_type") or "") not in inbound_types or str(inbound.get("account_id_cr") or "") not in accounts:
            continue
        inbound_ts = datetime.fromisoformat(str(inbound["timestamp"]))
        cutoff = inbound_ts + window
        outbound_value = 0.0
        outbound_count = 0
        counterparties: set[str] = set()
        for outbound in rows:
            outbound_ts = datetime.fromisoformat(str(outbound["timestamp"]))
            if outbound_ts <= inbound_ts or outbound_ts > cutoff:
                continue
            if str(outbound.get("txn_type") or "") not in outbound_types or str(outbound.get("account_id_dr") or "") not in accounts:
                continue
            outbound_value += float(outbound.get("amount_kes") or 0)
            outbound_count += 1
            if outbound.get("counterparty_id_hash"):
                counterparties.add(str(outbound["counterparty_id_hash"]))
        if outbound_count >= min_outflows and outbound_value / inbound_amount >= min_exit_ratio and len(counterparties) >= min_counterparties:
            return True
    return False


def _wallet_outbound_windows(
    rows: list[tuple[datetime, str, float, str, str, str]],
    outbound_types: set[str],
    dispersion_window: timedelta,
) -> dict[datetime, tuple[float, int]]:
    windows: dict[datetime, tuple[float, int]] = {}
    right = 0
    outbound_value = 0.0
    counterparties: Counter[str] = Counter()
    for left, row in enumerate(rows):
        timestamp = row[0]
        while right < len(rows) and rows[right][0] <= timestamp + dispersion_window:
            _, txn_type, amount, counterparty, _, debit_account = rows[right]
            if debit_account and txn_type in outbound_types:
                outbound_value += amount
                if counterparty:
                    counterparties[counterparty] += 1
            right += 1
        windows[timestamp] = (outbound_value, len(counterparties))
        _, txn_type, amount, counterparty, _, debit_account = row
        if debit_account and txn_type in outbound_types:
            outbound_value -= amount
            if counterparty:
                counterparties[counterparty] -= 1
                if counterparties[counterparty] <= 0:
                    del counterparties[counterparty]
    return windows


def _latest_inbound_timestamp(
    rows: list[tuple[datetime, str, float, str, str, str]],
    start: int,
    end: int,
    account_id: str,
    inbound_types: set[str],
) -> datetime | None:
    for timestamp, txn_type, _, _, credit_account, _ in reversed(rows[start:end]):
        if credit_account == account_id and txn_type in inbound_types:
            return timestamp
    return None


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
    parsed_rows = [(_device_event(row, member_accounts, inbound_types, outbound_types), row) for row in rows]
    parsed_rows.sort(key=lambda item: item[0][0])
    member_counts: Counter[str] = Counter()
    inbound = 0.0
    outbound = 0.0
    right = 0
    for left, (start_event, _) in enumerate(parsed_rows):
        start_ts = start_event[0]
        while right < len(parsed_rows) and parsed_rows[right][0][0] <= start_ts + timedelta(days=window_days):
            _, member_id, inbound_amount, outbound_amount = parsed_rows[right][0]
            if member_id:
                member_counts[member_id] += 1
            inbound += inbound_amount
            outbound += outbound_amount
            right += 1
        members = set(member_counts)
        if len(members) < min_members or right - left < min_txns:
            pass
        else:
            total_value = inbound + outbound
            if total_value >= min_total and inbound > 0 and outbound / inbound >= min_outbound_share:
                windows.append(members)
        _, member_id, inbound_amount, outbound_amount = start_event
        if member_id:
            member_counts[member_id] -= 1
            if member_counts[member_id] <= 0:
                del member_counts[member_id]
        inbound -= inbound_amount
        outbound -= outbound_amount
    return windows


def _device_event(
    row: dict[str, object],
    member_accounts: dict[str, set[str]],
    inbound_types: set[str],
    outbound_types: set[str],
) -> tuple[datetime, str, float, float]:
    member_id = str(row.get("member_id_primary") or "")
    accounts = member_accounts.get(member_id, set())
    amount = float(row.get("amount_kes") or 0)
    txn_type = str(row.get("txn_type") or "")
    inbound = amount if txn_type in inbound_types and str(row.get("account_id_cr") or "") in accounts else 0.0
    outbound = amount if txn_type in outbound_types and str(row.get("account_id_dr") or "") in accounts else 0.0
    return datetime.fromisoformat(str(row["timestamp"])), member_id, inbound, outbound


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
    "dormant_reactivation_abuse_candidates",
    "has_wallet_funneling",
    "has_church_charity_misuse",
    "has_dormant_reactivation_abuse",
    "has_remittance_layering",
    "rapid_pass_through_candidates",
    "remittance_layering_candidates",
    "rule_section",
    "church_charity_misuse_candidates",
    "structuring_candidates",
    "wallet_funneling_candidates",
]
