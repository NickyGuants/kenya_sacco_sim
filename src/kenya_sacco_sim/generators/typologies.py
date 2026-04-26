from __future__ import annotations

import random
from collections import defaultdict
from datetime import datetime, timedelta

from kenya_sacco_sim.core.config import EAT, WorldConfig
from kenya_sacco_sim.core.id_factory import IdFactory


def inject_typologies(
    config: WorldConfig,
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
) -> tuple[list[dict[str, object]], dict[str, object]]:
    rng = random.Random(config.seed + 505)
    account_by_member = _accounts_by_member(accounts)
    source_accounts = [account for account in accounts if account["account_type"] == "SOURCE_ACCOUNT"]
    sink_accounts = [account for account in accounts if account["account_type"] == "SINK_ACCOUNT"]
    alerts: list[dict[str, object]] = []
    next_txn = _next_txn_index(transactions)
    next_pattern = 1
    used_members: set[str] = set()
    targets = _target_counts(config)

    next_txn, next_pattern = _inject_structuring(rng, members, account_by_member, source_accounts, transactions, alerts, used_members, next_txn, next_pattern, targets["STRUCTURING"])
    _inject_rapid_pass_through(rng, members, account_by_member, source_accounts, sink_accounts, transactions, alerts, used_members, next_txn, next_pattern, targets["RAPID_PASS_THROUGH"])

    transactions.sort(key=lambda row: (str(row["timestamp"]), str(row["txn_id"])))
    _recompute_balances(transactions, accounts)
    rule_results = build_rule_results(transactions, accounts, alerts)
    return alerts, rule_results


def build_rule_results(transactions: list[dict[str, object]], accounts: list[dict[str, object]], alerts: list[dict[str, object]]) -> dict[str, object]:
    member_accounts = _member_account_ids(accounts)
    structuring = _structuring_candidates(transactions, member_accounts)
    rapid = _rapid_pass_through_candidates(transactions, member_accounts)
    truth_by_typology: dict[str, list[dict[str, object]]] = defaultdict(list)
    for alert in alerts:
        if alert["entity_type"] == "PATTERN":
            truth_by_typology[str(alert["typology"])].append(alert)
    return {
        "STRUCTURING": _rule_section(
            "deposit_count_7d >= 5 and max_single_deposit_7d < 100000 and total_deposit_7d >= 300000",
            structuring,
            truth_by_typology["STRUCTURING"],
        ),
        "RAPID_PASS_THROUGH": _rule_section(
            "inbound_value_48h >= 100000 and outbound_value_48h / inbound_value_48h >= 0.75 and retained_balance_ratio <= 0.25 and outbound_counterparty_count >= 2",
            rapid,
            truth_by_typology["RAPID_PASS_THROUGH"],
        ),
    }


def _target_counts(config: WorldConfig) -> dict[str, int]:
    total = max(2, round(config.member_count * config.suspicious_ratio))
    structuring = total // 2
    rapid = total - structuring
    return {"STRUCTURING": structuring, "RAPID_PASS_THROUGH": rapid}


def _inject_structuring(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    candidates = [
        member
        for member in members
        if str(member["persona_type"]) in {"SME_OWNER", "BODA_BODA_OPERATOR", "DIASPORA_SUPPORTED"}
        and str(member["member_id"]) not in used_members
        and _first(account_by_member[str(member["member_id"])], {"FOSA_SAVINGS", "FOSA_CURRENT"})
    ]
    rng.shuffle(candidates)
    for index, member in enumerate(candidates[:target_count], start=1):
        member_id = str(member["member_id"])
        used_members.add(member_id)
        fosa = _first(account_by_member[member_id], {"FOSA_SAVINGS", "FOSA_CURRENT"})
        assert fosa is not None
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        txn_ids: list[str] = []
        start = datetime(2024, 6, 3, 9, 0, tzinfo=EAT) + timedelta(days=index - 1)
        for offset in range(6):
            amount = float(rng.choice([72_000, 78_500, 83_000, 88_500, 94_000, 98_000]))
            txn_id = _txn_id(next_txn)
            next_txn += 1
            txn_ids.append(txn_id)
            use_cash = rng.random() < 0.40
            tx = _txn_row(
                txn_id,
                start + timedelta(days=offset, hours=rng.randint(0, 5)),
                rng.choice(source_accounts),
                fosa,
                member,
                "FOSA_CASH_DEPOSIT" if use_cash else "MPESA_PAYBILL_IN",
                "CASH_BRANCH" if use_cash else "MPESA",
                "BRANCH" if use_cash else "PAYBILL",
                amount,
                "SACCO_CORE",
                "CUSTOMER",
                fosa.get("branch_id"),
                f"STRUCTURING:{member_id}:{offset}",
            )
            transactions.append(tx)
            alerts.append(
                _alert_row(
                    len(alerts) + 1,
                    pattern_id,
                    "STRUCTURING",
                    "TRANSACTION",
                    txn_id,
                    member,
                    fosa,
                    txn_id,
                    None,
                    tx["timestamp"],
                    tx["timestamp"],
                    "HIGH",
                    "PLACEMENT",
                    "STRUCTURED_SUB_THRESHOLD_DEPOSITS",
                )
            )
        alerts.append(
            _alert_row(
                len(alerts) + 1,
                pattern_id,
                "STRUCTURING",
                "PATTERN",
                pattern_id,
                member,
                fosa,
                None,
                None,
                start.isoformat(timespec="seconds"),
                (start + timedelta(days=5)).isoformat(timespec="seconds"),
                "HIGH",
                "PATTERN_SUMMARY",
                "SUSPICIOUS_PATTERN_SUMMARY",
            )
        )
    return next_txn, next_pattern


def _inject_rapid_pass_through(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    candidates = [
        member
        for member in members
        if str(member["persona_type"]) in {"SME_OWNER", "DIASPORA_SUPPORTED", "CHURCH_ORG"}
        and str(member["member_id"]) not in used_members
        and _first(account_by_member[str(member["member_id"])], {"FOSA_CURRENT", "FOSA_SAVINGS"})
    ]
    rng.shuffle(candidates)
    for index, member in enumerate(candidates[:target_count], start=1):
        member_id = str(member["member_id"])
        used_members.add(member_id)
        fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        assert fosa is not None
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        txn_ids: list[str] = []
        start = datetime(2024, 7, 8, 10, 0, tzinfo=EAT) + timedelta(days=index - 1)
        inbound_amount = float(rng.choice([180_000, 240_000, 360_000, 520_000]))
        inbound_id = _txn_id(next_txn)
        next_txn += 1
        txn_ids.append(inbound_id)
        use_pesalink = rng.random() < 0.45
        inbound = _txn_row(
            inbound_id,
            start,
            rng.choice(source_accounts),
            fosa,
            member,
            "PESALINK_IN" if use_pesalink else "MPESA_PAYBILL_IN",
            "PESALINK" if use_pesalink else "MPESA",
            "BANK_TRANSFER" if use_pesalink else "PAYBILL",
            inbound_amount,
            "BANK_PARTNER",
            "BANK",
            fosa.get("branch_id"),
            f"RAPID_IN:{member_id}",
        )
        transactions.append(inbound)
        alerts.append(
            _alert_row(len(alerts) + 1, pattern_id, "RAPID_PASS_THROUGH", "TRANSACTION", inbound_id, member, fosa, inbound_id, None, inbound["timestamp"], inbound["timestamp"], "HIGH", "PLACEMENT", "RAPID_IN_OUT_MOVEMENT")
        )
        for offset, share in enumerate([0.28, 0.26, 0.24], start=1):
            out_id = _txn_id(next_txn)
            next_txn += 1
            txn_ids.append(out_id)
            outbound = _txn_row(
                out_id,
                start + timedelta(hours=6 + offset * 4),
                fosa,
                sink_accounts[offset % len(sink_accounts)],
                member,
                "PESALINK_OUT",
                "PESALINK",
                "BANK_TRANSFER",
                round(inbound_amount * share, 2),
                "BANK_PARTNER",
                "MERCHANT",
                fosa.get("branch_id"),
                f"RAPID_OUT:{member_id}:{offset}",
            )
            transactions.append(outbound)
            alerts.append(
                _alert_row(len(alerts) + 1, pattern_id, "RAPID_PASS_THROUGH", "TRANSACTION", out_id, member, fosa, out_id, None, outbound["timestamp"], outbound["timestamp"], "HIGH", "LAYERING", "HIGH_EXIT_RATIO")
            )
        alerts.append(
            _alert_row(
                len(alerts) + 1,
                pattern_id,
                "RAPID_PASS_THROUGH",
                "PATTERN",
                pattern_id,
                member,
                fosa,
                None,
                None,
                start.isoformat(timespec="seconds"),
                (start + timedelta(hours=18)).isoformat(timespec="seconds"),
                "HIGH",
                "PATTERN_SUMMARY",
                "SUSPICIOUS_PATTERN_SUMMARY",
            )
        )
    return next_txn, next_pattern


def _txn_row(
    txn_id: str,
    timestamp: datetime,
    debit_account: dict[str, object],
    credit_account: dict[str, object],
    member: dict[str, object],
    txn_type: str,
    rail: str,
    channel: str,
    amount: float,
    provider: str,
    counterparty_type: str,
    branch_id: object | None,
    counterparty_seed: str,
) -> dict[str, object]:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=EAT)
    if txn_type == "MPESA_PAYBILL_IN":
        channel = "PAYBILL"
        provider = "MPESA"
    return {
        "txn_id": txn_id,
        "timestamp": timestamp.isoformat(timespec="seconds"),
        "institution_id": member["institution_id"],
        "account_id_dr": debit_account["account_id"],
        "account_id_cr": credit_account["account_id"],
        "member_id_primary": member["member_id"],
        "txn_type": txn_type,
        "rail": rail,
        "channel": channel,
        "provider": provider,
        "counterparty_type": counterparty_type,
        "counterparty_id_hash": IdFactory.hash_id("CP", counterparty_seed),
        "amount_kes": round(amount, 2),
        "fee_kes": 0.0,
        "currency": "KES",
        "narrative": txn_type.replace("_", " ").title(),
        "reference": txn_id.replace("TXN", "REF", 1),
        "branch_id": branch_id,
        "agent_id": None,
        "device_id": None,
        "geo_bucket": member["county"],
        "batch_id": None,
        "balance_after_dr_kes": 0.0,
        "balance_after_cr_kes": 0.0,
        "is_reversal": False,
    }


def _alert_row(
    index: int,
    pattern_id: str,
    typology: str,
    entity_type: str,
    entity_id: str,
    member: dict[str, object],
    account: dict[str, object],
    txn_id: str | None,
    edge_id: str | None,
    start_timestamp: str,
    end_timestamp: str,
    severity: str,
    stage: str,
    explanation_code: str,
) -> dict[str, object]:
    return {
        "alert_id": f"ALT{index:08d}",
        "pattern_id": pattern_id,
        "typology": typology,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "member_id": member["member_id"],
        "account_id": account["account_id"],
        "txn_id": txn_id,
        "edge_id": edge_id,
        "start_timestamp": start_timestamp,
        "end_timestamp": end_timestamp,
        "severity": severity,
        "truth_label": True,
        "stage": stage,
        "explanation_code": explanation_code,
    }


def _rule_section(rule_definition: str, candidates: dict[str, object], truth_alerts: list[dict[str, object]]) -> dict[str, object]:
    truth_member_ids = sorted({str(alert["member_id"]) for alert in truth_alerts})
    return {
        "rule_definition": rule_definition,
        "candidate_count": len(candidates),
        "candidate_member_ids": sorted(candidates),
        "truth_pattern_alert_ids": sorted(str(alert["alert_id"]) for alert in truth_alerts),
        "truth_member_ids": truth_member_ids,
        "truth_members_detected": sorted(member_id for member_id in truth_member_ids if member_id in candidates),
    }


def _structuring_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    deposits_by_member: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if not member_id or str(txn["account_id_cr"]) not in member_accounts[member_id]:
            continue
        amount = float(txn["amount_kes"])
        if amount >= 100_000:
            continue
        if str(txn["txn_type"]) not in {"FOSA_CASH_DEPOSIT", "MPESA_PAYBILL_IN", "PESALINK_IN"}:
            continue
        deposits_by_member[member_id].append((datetime.fromisoformat(str(txn["timestamp"])), amount))
    return {member_id: True for member_id, deposits in deposits_by_member.items() if _has_structuring_window(deposits)}


def _rapid_pass_through_candidates(transactions: list[dict[str, object]], member_accounts: dict[str, set[str]]) -> dict[str, object]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        member_id = str(txn.get("member_id_primary") or "")
        if member_id:
            by_member[member_id].append(txn)
    candidates: dict[str, object] = {}
    for member_id, rows in by_member.items():
        rows.sort(key=lambda row: str(row["timestamp"]))
        if _has_rapid_pass_through(rows, member_accounts[member_id]):
            candidates[member_id] = True
    return candidates


def _has_structuring_window(deposits: list[tuple[datetime, float]]) -> bool:
    deposits.sort(key=lambda item: item[0])
    for start_index, (start_ts, _) in enumerate(deposits):
        window = [(ts, amount) for ts, amount in deposits[start_index:] if ts <= start_ts + timedelta(days=7)]
        if len(window) >= 5 and max(amount for _, amount in window) < 100_000 and sum(amount for _, amount in window) >= 300_000:
            return True
    return False


def _has_rapid_pass_through(rows: list[dict[str, object]], accounts: set[str]) -> bool:
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
        retained_balance_ratio = max(0.0, (inbound_amount - outbound_amount) / inbound_amount)
        if outbound_amount / inbound_amount >= 0.75 and retained_balance_ratio <= 0.25 and len(counterparties) >= 2:
            return True
    return False


def _recompute_balances(transactions: list[dict[str, object]], accounts: list[dict[str, object]]) -> None:
    account_by_id = {str(account["account_id"]): account for account in accounts}
    balances = {str(account["account_id"]): float(account["opening_balance_kes"]) for account in accounts}
    for txn in transactions:
        amount = float(txn["amount_kes"])
        debit_id = str(txn["account_id_dr"])
        credit_id = str(txn["account_id_cr"])
        _apply_movement(balances, account_by_id, debit_id, "dr", amount)
        _apply_movement(balances, account_by_id, credit_id, "cr", amount)
        txn["balance_after_dr_kes"] = round(balances[debit_id], 2)
        txn["balance_after_cr_kes"] = round(balances[credit_id], 2)
    for account in accounts:
        account["current_balance_kes"] = round(balances[str(account["account_id"])], 2)


def _apply_movement(balances: dict[str, float], account_by_id: dict[str, dict[str, object]], account_id: str, side: str, amount: float) -> None:
    if account_by_id[account_id]["account_type"] == "LOAN_ACCOUNT":
        balances[account_id] += amount if side == "dr" else -amount
    else:
        balances[account_id] += -amount if side == "dr" else amount


def _accounts_by_member(accounts: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for account in accounts:
        if account.get("member_id"):
            by_member[str(account["member_id"])].append(account)
    return by_member


def _member_account_ids(accounts: list[dict[str, object]]) -> dict[str, set[str]]:
    by_member: dict[str, set[str]] = defaultdict(set)
    for account in accounts:
        member_id = account.get("member_id")
        if member_id and account["account_owner_type"] == "MEMBER":
            by_member[str(member_id)].add(str(account["account_id"]))
    return by_member


def _first(accounts: list[dict[str, object]], account_types: set[str]) -> dict[str, object] | None:
    for account in accounts:
        if str(account["account_type"]) in account_types:
            return account
    return None


def _next_txn_index(transactions: list[dict[str, object]]) -> int:
    return max(int(str(txn["txn_id"])[3:]) for txn in transactions if str(txn["txn_id"]).startswith("TXN")) + 1


def _txn_id(index: int) -> str:
    return f"TXN{index:012d}"


def _pattern_id(index: int) -> str:
    return f"PAT{index:08d}"
