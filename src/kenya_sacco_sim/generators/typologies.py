from __future__ import annotations

import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

from kenya_sacco_sim.benchmark.baseline_rules import build_rule_results
from kenya_sacco_sim.core.config import EAT, WorldConfig
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


DIGITAL_DEVICE_REQUIRED_CHANNELS = {"MOBILE_APP", "USSD", "PAYBILL", "TILL", "BANK_TRANSFER"}


def inject_typologies(
    config: WorldConfig,
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    world: InstitutionWorld | None = None,
    loans: list[dict[str, object]] | None = None,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    rng = random.Random(config.seed + 505)
    account_by_member = _accounts_by_member(accounts)
    source_accounts = [account for account in accounts if account["account_type"] == "SOURCE_ACCOUNT"]
    sink_accounts = [account for account in accounts if account["account_type"] == "SINK_ACCOUNT"]
    agents_by_branch = _agents_by_branch(world.agents if world else [])
    normal_txn_counts = Counter(str(txn["member_id_primary"]) for txn in transactions if txn.get("member_id_primary"))
    alerts: list[dict[str, object]] = []
    next_txn = _next_txn_index(transactions)
    next_pattern = 1
    used_members: set[str] = set()
    targets = _target_counts(config, include_fake_affordability=bool(loans))

    next_txn, next_pattern = _inject_structuring(
        rng,
        members,
        account_by_member,
        source_accounts,
        agents_by_branch,
        transactions,
        alerts,
        used_members,
        normal_txn_counts,
        next_txn,
        next_pattern,
        targets["STRUCTURING"],
    )
    next_txn, next_pattern = _inject_rapid_pass_through(
        rng,
        members,
        account_by_member,
        source_accounts,
        sink_accounts,
        transactions,
        alerts,
        used_members,
        normal_txn_counts,
        next_txn,
        next_pattern,
        targets["RAPID_PASS_THROUGH"],
    )
    next_txn, next_pattern = _inject_fake_affordability(
        rng,
        config,
        members,
        account_by_member,
        source_accounts,
        transactions,
        alerts,
        used_members,
        normal_txn_counts,
        loans or [],
        next_txn,
        next_pattern,
        targets["FAKE_AFFORDABILITY_BEFORE_LOAN"],
    )
    decoy_target = max(2, targets["STRUCTURING"] // 5) if sum(targets.values()) else 0
    next_txn, near_miss_stats = _inject_decoys(rng, members, account_by_member, source_accounts, sink_accounts, agents_by_branch, transactions, used_members, next_txn, decoy_target)

    _backfill_digital_device_ids(transactions, world, rng)
    transactions.sort(key=lambda row: (str(row["timestamp"]), str(row["txn_id"])))
    _reassign_transaction_ids(transactions, alerts)
    _recompute_balances(transactions, accounts)
    rule_results = build_rule_results(transactions, accounts, alerts, loans or [])
    rule_results["near_miss_disclosure"] = near_miss_stats
    return alerts, rule_results


def _target_counts(config: WorldConfig, include_fake_affordability: bool = True) -> dict[str, int]:
    target = Decimal(str(config.member_count)) * Decimal(str(config.suspicious_ratio))
    total = max(0, int(target.to_integral_value(rounding=ROUND_HALF_UP)))
    if not include_fake_affordability:
        structuring = total // 2
        rapid = total - structuring
        return {"STRUCTURING": structuring, "RAPID_PASS_THROUGH": rapid, "FAKE_AFFORDABILITY_BEFORE_LOAN": 0}
    structuring = total // 3
    rapid = total // 3
    fake = total - structuring - rapid
    return {"STRUCTURING": structuring, "RAPID_PASS_THROUGH": rapid, "FAKE_AFFORDABILITY_BEFORE_LOAN": fake}


def _inject_structuring(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    agents_by_branch: dict[str, list[str]],
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    normal_txn_counts: Counter[str],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    candidates = _candidate_members(members, account_by_member, used_members, {"SME_OWNER", "BODA_BODA_OPERATOR", "DIASPORA_SUPPORTED"}, {"FOSA_SAVINGS", "FOSA_CURRENT"})
    rng.shuffle(candidates)
    inserted = 0
    for member in candidates:
        if inserted >= target_count:
            break
        member_id = str(member["member_id"])
        suspicious_txn_count = rng.randint(5, 12)
        extra_above_threshold = inserted % 4 == 0
        if extra_above_threshold and suspicious_txn_count == 5:
            suspicious_txn_count = 6
        if normal_txn_counts[member_id] < suspicious_txn_count:
            continue
        fosa = _first(account_by_member[member_id], {"FOSA_SAVINGS", "FOSA_CURRENT"})
        assert fosa is not None
        used_members.add(member_id)
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        inserted += 1
        start = datetime(2024, 5, 20, 9, 0, tzinfo=EAT) + timedelta(days=inserted * 3)
        window_hours = rng.randint(48, 168)
        rail_mix = _structuring_rail_mix(inserted)
        timestamps = _spread_timestamps(start, window_hours, suspicious_txn_count, rng, include_endpoints=True)
        txn_times: list[str] = []
        for offset in range(suspicious_txn_count):
            amount = _structuring_amount(rng, above_threshold=extra_above_threshold and offset == suspicious_txn_count - 1)
            rail = rail_mix[offset % len(rail_mix)]
            txn_type = "MPESA_PAYBILL_IN" if rail == "MPESA" else "FOSA_CASH_DEPOSIT"
            channel = "PAYBILL" if rail == "MPESA" else "AGENT" if rail == "CASH_AGENT" else "BRANCH"
            provider = "MPESA" if rail == "MPESA" else "SACCO_CORE"
            txn_id = _txn_id(next_txn)
            next_txn += 1
            tx = _txn_row(
                txn_id,
                timestamps[offset],
                rng.choice(source_accounts),
                fosa,
                member,
                txn_type,
                rail,
                channel,
                amount,
                provider,
                "CUSTOMER",
                fosa.get("branch_id"),
                f"STRUCTURING:{member_id}:{offset}",
                _select_agent_id(agents_by_branch, fosa.get("branch_id"), rng) if rail == "CASH_AGENT" else None,
            )
            transactions.append(tx)
            txn_times.append(str(tx["timestamp"]))
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "STRUCTURING", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "PLACEMENT", "STRUCTURED_SUB_THRESHOLD_DEPOSITS"))
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
                min(txn_times),
                max(txn_times),
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
    normal_txn_counts: Counter[str],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    candidates = _candidate_members(members, account_by_member, used_members, {"SME_OWNER", "DIASPORA_SUPPORTED", "CHURCH_ORG"}, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    rng.shuffle(candidates)
    inserted = 0
    for member in candidates:
        if inserted >= target_count:
            break
        member_id = str(member["member_id"])
        outflow_count = rng.randint(2, 8)
        suspicious_txn_count = outflow_count + 1
        if normal_txn_counts[member_id] < suspicious_txn_count:
            continue
        fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        assert fosa is not None
        used_members.add(member_id)
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        inserted += 1
        start = datetime(2024, 7, 1, 10, 0, tzinfo=EAT) + timedelta(days=inserted * 2)
        inbound_amount = float(rng.randrange(120_000, 740_000, 10_000))
        is_partial_truth = inserted % 5 == 0
        exit_ratio = rng.uniform(0.64, 0.74) if is_partial_truth else rng.uniform(0.76, 0.96)
        window_hours = rng.randint(4, 48)
        outflow_times = _spread_timestamps(start + timedelta(hours=1), window_hours - 1, outflow_count, rng)
        rail_variant = inserted % 3
        use_pesalink = rail_variant != 1
        inbound_id = _txn_id(next_txn)
        next_txn += 1
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
            "BANK_PARTNER" if use_pesalink else "MPESA",
            "BANK" if use_pesalink else "CUSTOMER",
            fosa.get("branch_id"),
            f"RAPID_IN:{member_id}",
        )
        transactions.append(inbound)
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "RAPID_PASS_THROUGH", "TRANSACTION", inbound_id, member, fosa, inbound_id, None, inbound["timestamp"], inbound["timestamp"], "HIGH", "PLACEMENT", "RAPID_IN_OUT_MOVEMENT"))
        allocations = _allocate_amounts(round(inbound_amount * exit_ratio, 2), outflow_count, rng)
        txn_times = [str(inbound["timestamp"])]
        for offset, amount in enumerate(allocations):
            out_id = _txn_id(next_txn)
            next_txn += 1
            if rail_variant == 1:
                txn_type = "SUPPLIER_PAYMENT_OUT"
                rail = "MPESA"
            elif rail_variant == 2:
                txn_type = "PESALINK_OUT"
                rail = "PESALINK"
            else:
                txn_type = "SUPPLIER_PAYMENT_OUT" if offset % 2 == 0 else "PESALINK_OUT"
                rail = "MPESA" if offset % 2 == 0 else "PESALINK"
            outbound = _txn_row(
                out_id,
                outflow_times[offset],
                fosa,
                sink_accounts[(inserted + offset) % len(sink_accounts)],
                member,
                txn_type,
                rail,
                "BANK_TRANSFER" if rail == "PESALINK" else "PAYBILL",
                amount,
                "BANK_PARTNER",
                "MERCHANT",
                fosa.get("branch_id"),
                f"RAPID_OUT:{member_id}:{offset}",
            )
            transactions.append(outbound)
            txn_times.append(str(outbound["timestamp"]))
            explanation = "HIGH_EXIT_RATIO" if not is_partial_truth else "RAPID_IN_OUT_MOVEMENT"
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "RAPID_PASS_THROUGH", "TRANSACTION", out_id, member, fosa, out_id, None, outbound["timestamp"], outbound["timestamp"], "HIGH", "LAYERING", explanation))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "RAPID_PASS_THROUGH", "PATTERN", pattern_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "PATTERN_SUMMARY", "SUSPICIOUS_PATTERN_SUMMARY"))
    return next_txn, next_pattern


def _inject_fake_affordability(
    rng: random.Random,
    config: WorldConfig,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    normal_txn_counts: Counter[str],
    loans: list[dict[str, object]],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    if not loans or target_count <= 0:
        return next_txn, next_pattern
    member_by_id = {str(member["member_id"]): member for member in members}
    eligible_products = {"DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "BIASHARA_LOAN"}
    candidates = [
        loan
        for loan in loans
        if str(loan["product_code"]) in eligible_products
        and str(loan["member_id"]) in member_by_id
        and str(member_by_id[str(loan["member_id"])]["persona_type"]) in {"SME_OWNER", "DIASPORA_SUPPORTED", "COUNTY_WORKER", "SALARIED_TEACHER"}
        and str(loan["member_id"]) not in used_members
    ]
    rng.shuffle(candidates)
    inserted = 0
    for loan in candidates:
        if inserted >= target_count:
            break
        member_id = str(loan["member_id"])
        member = member_by_id[member_id]
        credit_count = rng.randint(2, 5)
        if normal_txn_counts[member_id] < credit_count:
            continue
        fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        if not fosa:
            continue
        application_date = datetime.fromisoformat(f"{loan['application_date']}T09:00:00+03:00")
        simulation_start = _simulation_datetime(config.start_date, 9, 0)
        simulation_end = _simulation_datetime(config.end_date, 23, 59)
        start = max(simulation_start, application_date - timedelta(days=rng.randint(24, 30)))
        end = min(simulation_end, application_date - timedelta(hours=2))
        if start >= end:
            continue
        timestamps = _spread_timestamps(start, max(credit_count + 1, int((end - start).total_seconds() // 3600)), credit_count, rng)
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        inserted += 1
        used_members.add(member_id)
        txn_times: list[str] = []
        for offset, timestamp in enumerate(timestamps):
            txn_id = _txn_id(next_txn)
            next_txn += 1
            rail = rng.choice(["REMITTANCE", "MPESA", "PESALINK", "CASH_BRANCH"])
            txn_type = "PESALINK_IN" if rail in {"REMITTANCE", "PESALINK"} else "MPESA_PAYBILL_IN" if rail == "MPESA" else "FOSA_CASH_DEPOSIT"
            channel = "BANK_TRANSFER" if rail in {"REMITTANCE", "PESALINK"} else "PAYBILL" if rail == "MPESA" else "BRANCH"
            provider = "BANK_PARTNER" if rail in {"REMITTANCE", "PESALINK"} else "MPESA" if rail == "MPESA" else "SACCO_CORE"
            counterparty_type = "BANK" if rail in {"REMITTANCE", "PESALINK"} else "CUSTOMER"
            amount = float(rng.randrange(45_000, 150_000, 5_000))
            tx = _txn_row(
                txn_id,
                timestamp,
                rng.choice(source_accounts),
                fosa,
                member,
                txn_type,
                rail,
                channel,
                amount,
                provider,
                counterparty_type,
                fosa.get("branch_id"),
                f"FAKE_AFFORDABILITY:{member_id}:{loan['loan_id']}:{offset}",
            )
            transactions.append(tx)
            txn_times.append(str(tx["timestamp"]))
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "FAKE_AFFORDABILITY_BEFORE_LOAN", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "PLACEMENT", "PRE_LOAN_AFFORDABILITY_BOOST"))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "FAKE_AFFORDABILITY_BEFORE_LOAN", "MEMBER", member_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "INTEGRATION", "PRE_LOAN_AFFORDABILITY_BOOST"))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "FAKE_AFFORDABILITY_BEFORE_LOAN", "ACCOUNT", str(fosa["account_id"]), member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "INTEGRATION", "PRE_LOAN_AFFORDABILITY_BOOST"))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "FAKE_AFFORDABILITY_BEFORE_LOAN", "PATTERN", pattern_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "PATTERN_SUMMARY", "SUSPICIOUS_PATTERN_SUMMARY"))
    return next_txn, next_pattern


def _inject_decoys(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    agents_by_branch: dict[str, list[str]],
    transactions: list[dict[str, object]],
    used_members: set[str],
    next_txn: int,
    target_count: int,
) -> tuple[int, dict[str, object]]:
    decoy_members = _candidate_members(members, account_by_member, used_members, {"SME_OWNER", "BODA_BODA_OPERATOR", "DIASPORA_SUPPORTED", "CHURCH_ORG"}, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    rng.shuffle(decoy_members)
    near_miss_member_ids: set[str] = set()
    near_miss_txn_count = 0
    for index, member in enumerate(decoy_members[: target_count * 4], start=1):
        fosa = _first(account_by_member[str(member["member_id"])], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        if not fosa:
            continue
        before = next_txn
        if index % 4 == 1:
            next_txn = _inject_legitimate_structuring_like(rng, member, fosa, source_accounts, agents_by_branch, transactions, next_txn, index)
        elif index % 4 == 2:
            next_txn = _inject_incomplete_structuring(rng, member, fosa, source_accounts, transactions, next_txn, index)
        elif index % 4 == 3:
            next_txn = _inject_legitimate_rapid_like(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        else:
            next_txn = _inject_near_rapid(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        if next_txn > before:
            near_miss_member_ids.add(str(member["member_id"]))
            near_miss_txn_count += next_txn - before
    return next_txn, {
        "near_miss_transaction_count": near_miss_txn_count,
        "near_miss_member_count": len(near_miss_member_ids),
    }


def _inject_legitimate_structuring_like(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], agents_by_branch: dict[str, list[str]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 9, 1, 9, 0, tzinfo=EAT) + timedelta(days=index)
    for offset, amount in enumerate([58_000.0, 62_000.0, 65_000.0, 71_000.0, 78_000.0]):
        rail = ["CASH_BRANCH", "MPESA", "CASH_AGENT"][offset % 3]
        txn_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(
            _txn_row(
                txn_id,
                start + timedelta(days=offset),
                rng.choice(source_accounts),
                fosa,
                member,
                "MPESA_PAYBILL_IN" if rail == "MPESA" else "FOSA_CASH_DEPOSIT",
                rail,
                "PAYBILL" if rail == "MPESA" else "AGENT" if rail == "CASH_AGENT" else "BRANCH",
                amount,
                "MPESA" if rail == "MPESA" else "SACCO_CORE",
                "CUSTOMER",
                fosa.get("branch_id"),
                f"LEGIT_STRUCTURING_LIKE:{member['member_id']}:{offset}",
                _select_agent_id(agents_by_branch, fosa.get("branch_id"), rng) if rail == "CASH_AGENT" else None,
            )
        )
    return next_txn


def _inject_incomplete_structuring(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 9, 20, 10, 0, tzinfo=EAT) + timedelta(days=index)
    for offset in range(4):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(_txn_row(txn_id, start + timedelta(days=offset), rng.choice(source_accounts), fosa, member, "MPESA_PAYBILL_IN", "MPESA", "PAYBILL", float(rng.randrange(55_000, 90_000, 5_000)), "MPESA", "CUSTOMER", fosa.get("branch_id"), f"INCOMPLETE_STRUCTURING:{member['member_id']}:{offset}"))
    return next_txn


def _inject_legitimate_rapid_like(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 10, 2, 11, 0, tzinfo=EAT) + timedelta(days=index)
    amount = float(rng.randrange(160_000, 420_000, 20_000))
    inbound_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(_txn_row(inbound_id, start, rng.choice(source_accounts), fosa, member, "BUSINESS_SETTLEMENT_IN", "MPESA", "PAYBILL", amount, "MPESA", "CUSTOMER", fosa.get("branch_id"), f"LEGIT_SWEEP_IN:{member['member_id']}"))
    for offset, share in enumerate([0.31, 0.27, 0.22]):
        out_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(_txn_row(out_id, start + timedelta(hours=4 + offset * 5), fosa, sink_accounts[(index + offset) % len(sink_accounts)], member, "SUPPLIER_PAYMENT_OUT", rng.choice(["PESALINK", "MPESA"]), rng.choice(["BANK_TRANSFER", "PAYBILL"]), round(amount * share, 2), "BANK_PARTNER", "MERCHANT", fosa.get("branch_id"), f"LEGIT_SWEEP_OUT:{member['member_id']}:{offset}"))
    return next_txn


def _inject_near_rapid(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 10, 18, 12, 0, tzinfo=EAT) + timedelta(days=index)
    amount = float(rng.randrange(130_000, 300_000, 10_000))
    inbound_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(_txn_row(inbound_id, start, rng.choice(source_accounts), fosa, member, "PESALINK_IN", "PESALINK", "BANK_TRANSFER", amount, "BANK_PARTNER", "BANK", fosa.get("branch_id"), f"NEAR_RAPID_IN:{member['member_id']}"))
    out_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(_txn_row(out_id, start + timedelta(hours=12), fosa, rng.choice(sink_accounts), member, "PESALINK_OUT", "PESALINK", "BANK_TRANSFER", round(amount * rng.uniform(0.55, 0.70), 2), "BANK_PARTNER", "MERCHANT", fosa.get("branch_id"), f"NEAR_RAPID_OUT:{member['member_id']}"))
    return next_txn


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
    agent_id: str | None = None,
) -> dict[str, object]:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=EAT)
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
        "agent_id": agent_id,
        "device_id": None,  # Digital typology rows are assigned devices before ID reordering.
        "geo_bucket": member["county"],
        "batch_id": None,
        "balance_after_dr_kes": 0.0,
        "balance_after_cr_kes": 0.0,
        "is_reversal": False,
    }


def _reassign_transaction_ids(transactions: list[dict[str, object]], alerts: list[dict[str, object]]) -> None:
    id_map: dict[str, str] = {}
    transactions.sort(key=lambda row: (str(row["timestamp"]), str(row["txn_id"])))
    for index, txn in enumerate(transactions, start=1):
        old_txn_id = str(txn["txn_id"])
        new_txn_id = _txn_id(index)
        id_map[old_txn_id] = new_txn_id
        txn["txn_id"] = new_txn_id

    for txn in transactions:
        txn["reference"] = _reference_for_transaction(txn)

    for alert in alerts:
        old_alert_txn_id = str(alert.get("txn_id") or "")
        if old_alert_txn_id in id_map:
            alert["txn_id"] = id_map[old_alert_txn_id]
        old_entity_id = str(alert.get("entity_id") or "")
        if alert.get("entity_type") == "TRANSACTION" and old_entity_id in id_map:
            alert["entity_id"] = id_map[old_entity_id]


def _reference_for_transaction(txn: dict[str, object]) -> str:
    digest = IdFactory.hash_id(
        "REF",
        f"{txn['txn_id']}|{txn['timestamp']}|{txn['account_id_dr']}|{txn['account_id_cr']}|{txn['amount_kes']}|{txn['counterparty_id_hash']}",
    ).split("_", 1)[1].upper()[:12]
    rail = str(txn["rail"])
    if rail == "MPESA":
        return f"MPESA_{digest}"
    if rail == "PESALINK" or str(txn["channel"]) == "BANK_TRANSFER":
        return f"PSL_{digest}"
    if rail in {"CASH_AGENT", "CASH_BRANCH"}:
        branch = str(txn.get("branch_id") or "NA").replace("_", "")
        return f"CASH_{branch}_{digest}"
    if rail == "PAYROLL_CHECKOFF":
        return f"PAY_{digest}"
    return f"SACCO_{digest}"


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


def _structuring_amount(rng: random.Random, above_threshold: bool = False) -> float:
    if above_threshold:
        return float(rng.choice([105_000, 112_000, 125_000]))
    return float(rng.randrange(70_000, 99_000, 500))


def _structuring_rail_mix(index: int) -> list[str]:
    variants = [
        ["CASH_BRANCH", "MPESA"],
        ["CASH_AGENT", "MPESA"],
        ["CASH_BRANCH", "CASH_AGENT"],
        ["MPESA"],
        ["CASH_BRANCH", "CASH_AGENT", "MPESA"],
    ]
    return variants[index % len(variants)]


def _spread_timestamps(start: datetime, window_hours: int, count: int, rng: random.Random, include_endpoints: bool = False) -> list[datetime]:
    if count <= 1:
        return [start]
    if include_endpoints:
        middle_count = max(0, count - 2)
        interior = rng.sample(range(1, window_hours), min(middle_count, max(0, window_hours - 1)))
        offsets = sorted([0, window_hours, *interior])
        while len(offsets) < count:
            offsets.insert(-1, rng.randint(1, window_hours - 1))
        return [start + timedelta(hours=offset) for offset in offsets]
    offsets = sorted(rng.sample(range(0, max(window_hours, count) + 1), count))
    return [start + timedelta(hours=offset) for offset in offsets]


def _simulation_datetime(date_value: str, hour: int, minute: int) -> datetime:
    return datetime.fromisoformat(f"{date_value}T{hour:02d}:{minute:02d}:00+03:00").astimezone(EAT)


def _allocate_amounts(total: float, count: int, rng: random.Random) -> list[float]:
    weights = [rng.uniform(0.6, 1.4) for _ in range(count)]
    weight_total = sum(weights)
    amounts = [round(total * weight / weight_total, 2) for weight in weights]
    drift = round(total - sum(amounts), 2)
    amounts[-1] = round(amounts[-1] + drift, 2)
    return amounts


def _candidate_members(
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    used_members: set[str],
    personas: set[str],
    account_types: set[str],
) -> list[dict[str, object]]:
    return [
        member
        for member in members
        if str(member["persona_type"]) in personas
        and str(member["member_id"]) not in used_members
        and _first(account_by_member[str(member["member_id"])], account_types)
    ]


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


def _agents_by_branch(agents: list[dict[str, object]]) -> dict[str, list[str]]:
    by_branch: dict[str, list[str]] = defaultdict(list)
    for agent in agents:
        by_branch[str(agent["branch_id"])].append(str(agent["agent_id"]))
    return by_branch


def _backfill_digital_device_ids(transactions: list[dict[str, object]], world: InstitutionWorld | None, rng: random.Random) -> None:
    if not world or not world.devices:
        return
    devices_by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    devices_by_group: dict[str, list[dict[str, object]]] = defaultdict(list)
    for device in world.devices:
        devices_by_member[str(device["member_id"])].append(device)
        group = device.get("shared_device_group")
        if group:
            devices_by_group[str(group)].append(device)

    for txn in transactions:
        if txn.get("device_id") or txn.get("channel") not in DIGITAL_DEVICE_REQUIRED_CHANNELS:
            continue
        member_id = str(txn.get("member_id_primary") or "")
        if not member_id:
            continue
        devices = devices_by_member.get(member_id, [])
        if not devices:
            continue
        device = devices[0]
        group = device.get("shared_device_group")
        if group and rng.random() < 0.25:
            txn["device_id"] = str(rng.choice(devices_by_group.get(str(group), devices))["device_id"])
        else:
            txn["device_id"] = str(device["device_id"])


def _select_agent_id(agents_by_branch: dict[str, list[str]], branch_id: object | None, rng: random.Random) -> str | None:
    if branch_id is None:
        return None
    candidates = agents_by_branch.get(str(branch_id), [])
    if not candidates:
        return None
    return rng.choice(candidates)


def _first(accounts: list[dict[str, object]], account_types: set[str]) -> dict[str, object] | None:
    for account in accounts:
        if str(account["account_type"]) in account_types:
            return account
    return None


def _next_txn_index(transactions: list[dict[str, object]]) -> int:
    indices = [int(str(txn["txn_id"])[3:]) for txn in transactions if str(txn["txn_id"]).startswith("TXN")]
    return max(indices, default=0) + 1


def _txn_id(index: int) -> str:
    return f"TXN{index:012d}"


def _pattern_id(index: int) -> str:
    return f"PAT{index:08d}"
