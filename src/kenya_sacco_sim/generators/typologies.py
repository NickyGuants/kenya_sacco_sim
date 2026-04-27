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
TYPOLOGY_CANDIDATE_PERSONAS = {
    "SALARIED_TEACHER",
    "COUNTY_WORKER",
    "SME_OWNER",
    "FARMER_SEASONAL",
    "DIASPORA_SUPPORTED",
    "BODA_BODA_OPERATOR",
    "CHURCH_ORG",
}


def inject_typologies(
    config: WorldConfig,
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    world: InstitutionWorld | None = None,
    loans: list[dict[str, object]] | None = None,
    guarantors: list[dict[str, object]] | None = None,
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
    guarantors = guarantors if guarantors is not None else []
    used_members: set[str] = set()
    targets = _target_counts(config, include_fake_affordability=bool(loans))

    next_txn, next_pattern = _inject_structuring(
        rng,
        config,
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
        config,
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
    next_txn, next_pattern = _inject_wallet_funneling(
        rng,
        config,
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
        targets["WALLET_FUNNELING"],
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
    next_txn, next_pattern = _inject_device_sharing_mule_network(
        rng,
        config,
        members,
        account_by_member,
        source_accounts,
        sink_accounts,
        world,
        transactions,
        alerts,
        used_members,
        normal_txn_counts,
        next_txn,
        next_pattern,
        targets["DEVICE_SHARING_MULE_NETWORK"],
    )
    next_pattern = _inject_guarantor_fraud_ring(
        rng,
        config,
        members,
        account_by_member,
        transactions,
        alerts,
        used_members,
        normal_txn_counts,
        loans or [],
        guarantors,
        next_pattern,
        targets["GUARANTOR_FRAUD_RING"],
    )
    guarantor_near_miss_stats = _inject_guarantor_ring_decoys(
        rng,
        members,
        account_by_member,
        loans or [],
        guarantors,
        used_members,
        max(1, targets["GUARANTOR_FRAUD_RING"] // 6) if targets["GUARANTOR_FRAUD_RING"] else 0,
    )
    next_txn, device_near_miss_stats = _inject_device_sharing_decoys(
        rng,
        members,
        account_by_member,
        source_accounts,
        sink_accounts,
        world,
        transactions,
        used_members,
        next_txn,
        max(1, targets["DEVICE_SHARING_MULE_NETWORK"] // 6) if targets["DEVICE_SHARING_MULE_NETWORK"] else 0,
    )
    decoy_target = max(2, sum(targets.values()) // 9) if sum(targets.values()) else 0
    next_txn, near_miss_stats = _inject_decoys(
        rng,
        members,
        account_by_member,
        source_accounts,
        sink_accounts,
        agents_by_branch,
        transactions,
        used_members,
        loans or [],
        next_txn,
        decoy_target,
    )
    near_miss_stats = _merge_near_miss_stats(near_miss_stats, device_near_miss_stats)
    near_miss_stats = _merge_near_miss_stats(near_miss_stats, guarantor_near_miss_stats)

    _backfill_digital_device_ids(transactions, world, rng)
    transactions.sort(key=lambda row: (str(row["timestamp"]), str(row["txn_id"])))
    _reassign_transaction_ids(transactions, alerts)
    _recompute_balances(transactions, accounts)
    rule_results = build_rule_results(transactions, accounts, alerts, loans or [], guarantors)
    rule_results["near_miss_disclosure"] = near_miss_stats
    return alerts, rule_results


def _target_counts(config: WorldConfig, include_fake_affordability: bool = True) -> dict[str, int]:
    target = Decimal(str(config.member_count)) * Decimal(str(config.suspicious_ratio))
    total = max(0, int(target.to_integral_value(rounding=ROUND_HALF_UP)))
    if total <= 0:
        return {
            "STRUCTURING": 0,
            "RAPID_PASS_THROUGH": 0,
            "FAKE_AFFORDABILITY_BEFORE_LOAN": 0,
            "DEVICE_SHARING_MULE_NETWORK": 0,
            "GUARANTOR_FRAUD_RING": 0,
            "WALLET_FUNNELING": 0,
        }
    if total > 0 and config.member_count >= 100:
        total = max(total, 5 if include_fake_affordability else 3)
    if total > 0 and config.member_count >= 10_000:
        total = max(total, 30 * (6 if include_fake_affordability else 4))

    base_typologies = ["STRUCTURING", "RAPID_PASS_THROUGH", "WALLET_FUNNELING"]
    if include_fake_affordability:
        base_typologies.append("FAKE_AFFORDABILITY_BEFORE_LOAN")
        base_typologies.append("GUARANTOR_FRAUD_RING")
    device_minimum_total = len(base_typologies) + 3
    include_device_sharing = config.member_count >= 1_000 and total >= device_minimum_total

    if not include_fake_affordability:
        typologies = ["STRUCTURING", "RAPID_PASS_THROUGH", "WALLET_FUNNELING"]
        if include_device_sharing:
            typologies.append("DEVICE_SHARING_MULE_NETWORK")
        counts = _balanced_target_counts(total, typologies)
        counts["FAKE_AFFORDABILITY_BEFORE_LOAN"] = 0
        counts["GUARANTOR_FRAUD_RING"] = 0
    else:
        typologies = list(base_typologies)
        if include_device_sharing:
            typologies.append("DEVICE_SHARING_MULE_NETWORK")
        counts = _balanced_target_counts(total, typologies)
    counts.setdefault("DEVICE_SHARING_MULE_NETWORK", 0)
    counts.setdefault("GUARANTOR_FRAUD_RING", 0)
    counts.setdefault("WALLET_FUNNELING", 0)
    if include_device_sharing and 0 < counts["DEVICE_SHARING_MULE_NETWORK"] < 3:
        _raise_count_floor(counts, "DEVICE_SHARING_MULE_NETWORK", 3, base_typologies)
    return counts


def _balanced_target_counts(total: int, typologies: list[str]) -> dict[str, int]:
    if total <= 0:
        return {typology: 0 for typology in typologies}
    base = total // len(typologies)
    remainder = total % len(typologies)
    return {typology: base + (1 if index < remainder else 0) for index, typology in enumerate(typologies)}


def _raise_count_floor(counts: dict[str, int], target_name: str, floor: int, donor_names: list[str]) -> None:
    needed = max(0, floor - counts.get(target_name, 0))
    counts[target_name] = max(counts.get(target_name, 0), floor)
    while needed > 0:
        donor = max(donor_names, key=lambda name: counts.get(name, 0))
        if counts.get(donor, 0) <= 0:
            break
        counts[donor] -= 1
        needed -= 1


def _inject_structuring(
    rng: random.Random,
    config: WorldConfig,
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
    candidates = _candidate_members(members, account_by_member, used_members, TYPOLOGY_CANDIDATE_PERSONAS, {"FOSA_SAVINGS", "FOSA_CURRENT"})
    candidates = _stratified_items_by_persona(candidates, rng, lambda member: str(member["persona_type"]))
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
        start = _distributed_pattern_start(config, rng, inserted, target_count, max_duration_days=8)
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
    config: WorldConfig,
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
    candidates = _candidate_members(members, account_by_member, used_members, TYPOLOGY_CANDIDATE_PERSONAS, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    candidates = _stratified_items_by_persona(candidates, rng, lambda member: str(member["persona_type"]))
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
        start = _distributed_pattern_start(config, rng, inserted, target_count, max_duration_days=3)
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


def _inject_wallet_funneling(
    rng: random.Random,
    config: WorldConfig,
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
    candidates = _candidate_members(members, account_by_member, used_members, TYPOLOGY_CANDIDATE_PERSONAS, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    candidates = _stratified_items_by_persona(candidates, rng, lambda member: str(member["persona_type"]))
    inserted = 0
    for member in candidates:
        if inserted >= target_count:
            break
        member_id = str(member["member_id"])
        inbound_count = rng.randint(6, 10)
        outbound_count = rng.randint(2, 5)
        suspicious_txn_count = inbound_count + outbound_count
        if normal_txn_counts[member_id] < suspicious_txn_count:
            continue
        fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        if not fosa:
            continue
        used_members.add(member_id)
        pattern_id = _pattern_id(next_pattern)
        next_pattern += 1
        inserted += 1
        start = _distributed_pattern_start(config, rng, inserted, target_count, max_duration_days=12)
        fan_in_hours = rng.randint(36, 168)
        inbound_times = _spread_timestamps(start, fan_in_hours, inbound_count, rng, include_endpoints=True)
        txn_times: list[str] = []
        inbound_total = 0.0
        for offset, timestamp in enumerate(inbound_times):
            txn_id = _txn_id(next_txn)
            next_txn += 1
            txn_type, rail, channel, provider = _wallet_funnel_inbound_shape(offset)
            amount = float(rng.randrange(55_000, 115_000, 5_000))
            inbound_total += amount
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
                "WALLET_USER" if rail in {"MPESA", "AIRTEL_MONEY"} else "MERCHANT",
                fosa.get("branch_id"),
                f"WALLET_FUNNEL_IN:{pattern_id}:{member_id}:{offset}",
            )
            transactions.append(tx)
            txn_times.append(str(tx["timestamp"]))
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "WALLET_FUNNELING", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "PLACEMENT", "WALLET_FUNNEL_ACTIVITY"))
        outbound_total = round(inbound_total * rng.uniform(0.58, 0.82), 2)
        allocations = _allocate_amounts(outbound_total, outbound_count, rng)
        last_inbound = max(inbound_times)
        outbound_times = _spread_timestamps(last_inbound + timedelta(hours=2), rng.randint(18, 72), outbound_count, rng)
        for offset, amount in enumerate(allocations):
            txn_id = _txn_id(next_txn)
            next_txn += 1
            txn_type, rail, channel, provider = _wallet_funnel_outbound_shape(offset)
            tx = _txn_row(
                txn_id,
                outbound_times[offset],
                fosa,
                sink_accounts[(inserted + offset) % len(sink_accounts)],
                member,
                txn_type,
                rail,
                channel,
                amount,
                provider,
                "WALLET_USER" if txn_type in {"MPESA_WALLET_TOPUP", "WALLET_P2P_OUT"} else "MERCHANT",
                fosa.get("branch_id"),
                f"WALLET_FUNNEL_OUT:{pattern_id}:{member_id}:{offset}",
            )
            transactions.append(tx)
            txn_times.append(str(tx["timestamp"]))
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "WALLET_FUNNELING", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "LAYERING", "WALLET_FUNNEL_ACTIVITY"))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "WALLET_FUNNELING", "MEMBER", member_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "LAYERING", "WALLET_FUNNEL_ACTIVITY"))
        alerts.append(_alert_row(len(alerts) + 1, pattern_id, "WALLET_FUNNELING", "PATTERN", pattern_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "PATTERN_SUMMARY", "SUSPICIOUS_PATTERN_SUMMARY"))
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
        and str(member_by_id[str(loan["member_id"])]["persona_type"]) in TYPOLOGY_CANDIDATE_PERSONAS
        and str(loan["member_id"]) not in used_members
    ]
    candidates = _stratified_items_by_persona(candidates, rng, lambda loan: str(member_by_id[str(loan["member_id"])]["persona_type"]))
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


def _inject_device_sharing_mule_network(
    rng: random.Random,
    config: WorldConfig,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    world: InstitutionWorld | None,
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    normal_txn_counts: Counter[str],
    next_txn: int,
    next_pattern: int,
    target_count: int,
) -> tuple[int, int]:
    if not world or not world.devices or target_count <= 0:
        return next_txn, next_pattern
    available_device_members = _members_without_shared_devices(world)
    candidates = _candidate_members(members, account_by_member, used_members, TYPOLOGY_CANDIDATE_PERSONAS, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    candidates = [member for member in candidates if str(member["member_id"]) in available_device_members and normal_txn_counts[str(member["member_id"])] >= 4]
    candidates = _stratified_items_by_persona(candidates, rng, lambda member: str(member["persona_type"]))
    inserted = 0
    group_index = 1
    cursor = 0
    while inserted < target_count and cursor < len(candidates):
        remaining = target_count - inserted
        if remaining in {3, 4, 5}:
            group_size = remaining
        else:
            group_size = 3
        if group_size < 3:
            break
        group = candidates[cursor : cursor + group_size]
        cursor += group_size
        if len(group) < group_size:
            break
        group_member_ids = [str(member["member_id"]) for member in group]
        if any(member_id in used_members for member_id in group_member_ids):
            continue
        current_group_index = group_index
        device_id = _assign_shared_device_group(world, group_member_ids, f"SHARED_DEVICE_GROUP_V1_{current_group_index:05d}")
        if not device_id:
            continue
        group_index += 1
        expected_group_count = max(1, (target_count + 2) // 3)
        start = _distributed_pattern_start(config, rng, current_group_index, expected_group_count, max_duration_days=3)
        for member_offset, member in enumerate(group):
            if inserted >= target_count:
                break
            member_id = str(member["member_id"])
            fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
            if not fosa:
                continue
            used_members.add(member_id)
            pattern_id = _pattern_id(next_pattern)
            next_pattern += 1
            inserted += 1
            inbound_count = rng.randint(1, 2)
            outbound_count = rng.randint(1, 2)
            member_start = start + timedelta(hours=member_offset * 3)
            inbound_total = 0.0
            txn_times: list[str] = []
            member_txn_ids: list[str] = []
            for offset in range(inbound_count):
                amount = float(rng.randrange(100_000, 165_000, 5_000))
                inbound_total += amount
                txn_id = _txn_id(next_txn)
                next_txn += 1
                txn_type, rail, channel, provider, counterparty_type = _device_mule_inbound_shape(offset)
                tx = _txn_row(
                    txn_id,
                    member_start + timedelta(hours=offset * rng.randint(4, 9)),
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
                    f"DEVICE_MULE_IN:{device_id}:{member_id}:{offset}",
                )
                tx["device_id"] = device_id
                transactions.append(tx)
                txn_times.append(str(tx["timestamp"]))
                member_txn_ids.append(txn_id)
                alerts.append(_alert_row(len(alerts) + 1, pattern_id, "DEVICE_SHARING_MULE_NETWORK", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "LAYERING", "SHARED_DEVICE_MULE_ACTIVITY"))
            outbound_total = round(inbound_total * rng.uniform(0.48, 0.68), 2)
            allocations = _allocate_amounts(outbound_total, outbound_count, rng)
            for offset, amount in enumerate(allocations):
                txn_id = _txn_id(next_txn)
                next_txn += 1
                txn_type = "PESALINK_OUT" if offset % 2 else "SUPPLIER_PAYMENT_OUT"
                rail = "PESALINK" if txn_type == "PESALINK_OUT" else "MPESA"
                tx = _txn_row(
                    txn_id,
                    member_start + timedelta(hours=18 + offset * rng.randint(8, 18)),
                    fosa,
                    sink_accounts[(inserted + offset) % len(sink_accounts)],
                    member,
                    txn_type,
                    rail,
                    "BANK_TRANSFER" if rail == "PESALINK" else "PAYBILL",
                    amount,
                    "BANK_PARTNER" if rail == "PESALINK" else "MPESA",
                    "MERCHANT",
                    fosa.get("branch_id"),
                    f"DEVICE_MULE_OUT:{device_id}:{member_id}:{offset}",
                )
                tx["device_id"] = device_id
                transactions.append(tx)
                txn_times.append(str(tx["timestamp"]))
                member_txn_ids.append(txn_id)
                alerts.append(_alert_row(len(alerts) + 1, pattern_id, "DEVICE_SHARING_MULE_NETWORK", "TRANSACTION", txn_id, member, fosa, txn_id, None, tx["timestamp"], tx["timestamp"], "HIGH", "LAYERING", "SHARED_DEVICE_MULE_ACTIVITY"))
            alerts.append(_alert_row(len(alerts) + 1, pattern_id, "DEVICE_SHARING_MULE_NETWORK", "PATTERN", pattern_id, member, fosa, None, None, min(txn_times), max(txn_times), "HIGH", "PATTERN_SUMMARY", "SUSPICIOUS_PATTERN_SUMMARY"))
    return next_txn, next_pattern


def _inject_guarantor_fraud_ring(
    rng: random.Random,
    config: WorldConfig,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    transactions: list[dict[str, object]],
    alerts: list[dict[str, object]],
    used_members: set[str],
    normal_txn_counts: Counter[str],
    loans: list[dict[str, object]],
    guarantors: list[dict[str, object]],
    next_pattern: int,
    target_count: int,
) -> int:
    if not loans or target_count <= 0:
        return next_pattern
    member_by_id = {str(member["member_id"]): member for member in members}
    account_by_id = {str(account["account_id"]): account for accounts in account_by_member.values() for account in accounts}
    loan_txns = _loan_context_transactions(transactions)
    active_counts = _active_guarantee_counts(guarantors, loans)
    guarantee_index = _next_guarantee_index(guarantors)
    eligible_products = {"DEVELOPMENT_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE"}
    active_statuses = {"CURRENT", "IN_ARREARS", "DEFAULTED", "RESTRUCTURED"}
    candidates = [
        loan
        for loan in loans
        if str(loan.get("product_code")) in eligible_products
        and str(loan.get("performing_status")) in active_statuses
        and str(loan.get("member_id")) in member_by_id
        and str(loan.get("member_id")) not in used_members
        and normal_txn_counts[str(loan["member_id"])] >= 6
        and len(loan_txns.get(str(loan["loan_account_id"]), [])) >= 2
        and _first(account_by_member[str(loan["member_id"])], {"BOSA_DEPOSIT"})
        and active_counts[str(loan["member_id"])] <= 4
    ]
    candidates = _stratified_items_by_persona(candidates, rng, lambda loan: str(member_by_id[str(loan["member_id"])]["persona_type"]))
    inserted = 0
    cursor = 0
    group_index = 1
    while inserted < target_count and cursor < len(candidates):
        remaining = target_count - inserted
        group_size = remaining if remaining <= 5 else 3
        if group_size < 3:
            break
        group = candidates[cursor : cursor + group_size]
        cursor += group_size
        if len(group) < group_size:
            break
        member_ids = [str(loan["member_id"]) for loan in group]
        if len(set(member_ids)) != len(member_ids) or any(member_id in used_members for member_id in member_ids):
            continue
        if any(active_counts[member_id] >= 5 for member_id in member_ids):
            continue
        label_context: list[tuple[dict[str, object], dict[str, object], dict[str, object], list[dict[str, object]]]] = []
        for loan in group:
            member_id = str(loan["member_id"])
            member = member_by_id[member_id]
            account = account_by_id.get(str(loan["loan_account_id"])) or _first(account_by_member[member_id], {"LOAN_ACCOUNT"})
            context_txns = loan_txns.get(str(loan["loan_account_id"]), [])[:2]
            if not account or len(context_txns) < 2:
                label_context = []
                break
            label_context.append((loan, member, account, context_txns))
        if len(label_context) != group_size:
            continue
        appended_guarantees = _append_guarantor_cycle(rng, group, member_by_id, account_by_member, guarantors, active_counts, guarantee_index)
        if appended_guarantees < group_size:
            continue
        guarantee_index += appended_guarantees
        for loan, member, account, context_txns in label_context:
            if inserted >= target_count:
                break
            member_id = str(loan["member_id"])
            pattern_id = _pattern_id(next_pattern)
            next_pattern += 1
            inserted += 1
            used_members.add(member_id)
            txn_times = [str(txn["timestamp"]) for txn in context_txns]
            for txn in context_txns:
                alerts.append(
                    _alert_row(
                        len(alerts) + 1,
                        pattern_id,
                        "GUARANTOR_FRAUD_RING",
                        "TRANSACTION",
                        str(txn["txn_id"]),
                        member,
                        account,
                        str(txn["txn_id"]),
                        None,
                        str(txn["timestamp"]),
                        str(txn["timestamp"]),
                        "HIGH",
                        "INTEGRATION",
                        "RECIPROCAL_GUARANTEE_RING",
                    )
                )
            alerts.append(
                _alert_row(
                    len(alerts) + 1,
                    pattern_id,
                    "GUARANTOR_FRAUD_RING",
                    "MEMBER",
                    member_id,
                    member,
                    account,
                    None,
                    None,
                    min(txn_times),
                    max(txn_times),
                    "HIGH",
                    "INTEGRATION",
                    "RECIPROCAL_GUARANTEE_RING",
                )
            )
            alerts.append(
                _alert_row(
                    len(alerts) + 1,
                    pattern_id,
                    "GUARANTOR_FRAUD_RING",
                    "PATTERN",
                    pattern_id,
                    member,
                    account,
                    None,
                    None,
                    min(txn_times),
                    max(txn_times),
                    "HIGH",
                    "PATTERN_SUMMARY",
                    "SUSPICIOUS_PATTERN_SUMMARY",
                )
            )
        group_index += 1
    return next_pattern


def _inject_guarantor_ring_decoys(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    loans: list[dict[str, object]],
    guarantors: list[dict[str, object]],
    used_members: set[str],
    target_group_count: int,
) -> dict[str, object]:
    if not loans or target_group_count <= 0:
        return _near_miss_result({})
    member_by_id = {str(member["member_id"]): member for member in members}
    active_counts = _active_guarantee_counts(guarantors, loans)
    guarantee_index = _next_guarantee_index(guarantors)
    eligible = [
        loan
        for loan in loans
        if str(loan.get("product_code")) in {"DEVELOPMENT_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE"}
        and str(loan.get("performing_status")) != "CLOSED"
        and str(loan.get("member_id")) in member_by_id
        and str(loan.get("member_id")) not in used_members
        and _first(account_by_member[str(loan["member_id"])], {"BOSA_DEPOSIT"})
        and active_counts[str(loan["member_id"])] <= 3
    ]
    rng.shuffle(eligible)
    families = {
        "legitimate_two_member_reciprocal_guarantee": {
            "target_typology": "GUARANTOR_FRAUD_RING",
            "description": "Two trusted members reciprocally guarantee one loan each, below the minimum ring-size threshold.",
            "expected_rule_effect": "negative_control",
        },
        "trusted_guarantor_star": {
            "target_typology": "GUARANTOR_FRAUD_RING",
            "description": "A high-capacity community guarantor backs several members without forming a directed cycle.",
            "expected_rule_effect": "negative_control",
        },
    }
    group_count = 0
    cursor = 0
    while group_count < target_group_count and cursor + 5 <= len(eligible):
        pair = eligible[cursor : cursor + 2]
        cursor += 2
        pair_members = [str(loan["member_id"]) for loan in pair]
        if len(set(pair_members)) == 2 and all(active_counts[member_id] < 5 for member_id in pair_members):
            appended = _append_guarantor_cycle(rng, pair, member_by_id, account_by_member, guarantors, active_counts, guarantee_index)
            if appended == 2:
                guarantee_index += appended
                for member_id in pair_members:
                    _record_guarantee_near_miss(families, "legitimate_two_member_reciprocal_guarantee", member_id, 1)
                    used_members.add(member_id)

        star = eligible[cursor : cursor + 3]
        cursor += 3
        if len(star) < 3:
            break
        star_guarantor_member_id = str(star[0]["member_id"])
        if active_counts[star_guarantor_member_id] > 2:
            continue
        for borrower_loan in star[1:]:
            borrower_id = str(borrower_loan["member_id"])
            if borrower_id == star_guarantor_member_id or active_counts[star_guarantor_member_id] >= 5:
                continue
            guarantee = _guarantee_row(
                rng,
                f"GUA{guarantee_index:06d}",
                borrower_loan,
                member_by_id[star_guarantor_member_id],
                member_by_id[borrower_id],
                account_by_member,
            )
            guarantee_index += 1
            if guarantee:
                guarantors.append(guarantee)
                active_counts[star_guarantor_member_id] += 1
                _record_guarantee_near_miss(families, "trusted_guarantor_star", star_guarantor_member_id, 1)
                _record_guarantee_near_miss(families, "trusted_guarantor_star", borrower_id, 0)
                used_members.update({star_guarantor_member_id, borrower_id})
        group_count += 1
    return _near_miss_result(families)


def _inject_device_sharing_decoys(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    world: InstitutionWorld | None,
    transactions: list[dict[str, object]],
    used_members: set[str],
    next_txn: int,
    target_group_count: int,
) -> tuple[int, dict[str, object]]:
    if not world or not world.devices or target_group_count <= 0:
        return next_txn, _near_miss_result({})
    available_device_members = _members_without_shared_devices(world)
    candidates = _candidate_members(members, account_by_member, used_members, {"SME_OWNER", "BODA_BODA_OPERATOR", "DIASPORA_SUPPORTED", "CHURCH_ORG"}, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    candidates = [member for member in candidates if str(member["member_id"]) in available_device_members]
    rng.shuffle(candidates)
    group_count = 0
    member_ids: set[str] = set()
    family_members: set[str] = set()
    txn_count = 0
    cursor = 0
    while group_count < target_group_count and cursor + 3 <= len(candidates):
        group = candidates[cursor : cursor + 3]
        cursor += 3
        group_member_ids = [str(member["member_id"]) for member in group]
        device_id = _assign_shared_device_group(world, group_member_ids, f"SHARED_DEVICE_GROUP_V1_DECOY_{group_count + 1:05d}")
        if not device_id:
            continue
        used_members.update(group_member_ids)
        group_count += 1
        start = datetime(2024, 11, 5, 9, 0, tzinfo=EAT) + timedelta(days=group_count * 4)
        for offset, member in enumerate(group):
            member_id = str(member["member_id"])
            fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
            if not fosa:
                continue
            member_ids.add(member_id)
            inbound_amount = float(rng.randrange(18_000, 36_000, 1_000))
            inbound_id = _txn_id(next_txn)
            next_txn += 1
            inbound = _txn_row(
                inbound_id,
                start + timedelta(hours=offset * 2),
                rng.choice(source_accounts),
                fosa,
                member,
                "MPESA_PAYBILL_IN",
                "MPESA",
                "PAYBILL",
                inbound_amount,
                "MPESA",
                "CUSTOMER",
                fosa.get("branch_id"),
                f"LEGIT_SHARED_DEVICE_IN:{device_id}:{member_id}",
            )
            inbound["device_id"] = device_id
            transactions.append(inbound)
            outbound_id = _txn_id(next_txn)
            next_txn += 1
            outbound = _txn_row(
                outbound_id,
                start + timedelta(hours=18 + offset * 3),
                fosa,
                rng.choice(sink_accounts),
                member,
                "SUPPLIER_PAYMENT_OUT",
                "MPESA",
                "PAYBILL",
                round(inbound_amount * rng.uniform(0.15, 0.32), 2),
                "MPESA",
                "MERCHANT",
                fosa.get("branch_id"),
                f"LEGIT_SHARED_DEVICE_OUT:{device_id}:{member_id}",
            )
            outbound["device_id"] = device_id
            transactions.append(outbound)
            txn_count += 2
            family_members.add(member_id)
    families = {
        "normal_shared_device_low_value": {
            "target_typology": "DEVICE_SHARING_MULE_NETWORK",
            "description": "Family or co-owner shared devices with low value and low outbound share.",
            "expected_rule_effect": "negative_control",
            "member_ids": family_members,
            "transaction_count": txn_count,
            "group_count": group_count,
        }
    }
    return next_txn, {
        **_near_miss_result(families),
        "device_sharing_near_miss_group_count": group_count,
        "device_sharing_near_miss_member_count": len(member_ids),
        "device_sharing_near_miss_transaction_count": txn_count,
    }


def _inject_decoys(
    rng: random.Random,
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    agents_by_branch: dict[str, list[str]],
    transactions: list[dict[str, object]],
    used_members: set[str],
    loans: list[dict[str, object]],
    next_txn: int,
    target_count: int,
) -> tuple[int, dict[str, object]]:
    decoy_members = _candidate_members(members, account_by_member, used_members, TYPOLOGY_CANDIDATE_PERSONAS, {"FOSA_CURRENT", "FOSA_SAVINGS"})
    rng.shuffle(decoy_members)
    families = _empty_near_miss_families()
    family_order = [
        "legitimate_structuring_like",
        "incomplete_structuring",
        "legitimate_sme_liquidity_sweep",
        "near_rapid_low_exit",
        "legitimate_chama_wallet_collection",
        "near_wallet_funnel_low_fanout",
        "church_family_bulk_payments",
    ]
    family_counts = Counter()
    cursor = 0
    index = 1
    while cursor < len(decoy_members) and any(family_counts[family] < target_count for family in family_order):
        member = decoy_members[cursor]
        cursor += 1
        fosa = _first(account_by_member[str(member["member_id"])], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        if not fosa:
            continue
        before = next_txn
        family = next((name for name in family_order if family_counts[name] < target_count), family_order[0])
        if family == "legitimate_structuring_like":
            next_txn = _inject_legitimate_structuring_like(rng, member, fosa, source_accounts, agents_by_branch, transactions, next_txn, index)
        elif family == "incomplete_structuring":
            next_txn = _inject_incomplete_structuring(rng, member, fosa, source_accounts, transactions, next_txn, index)
        elif family == "legitimate_sme_liquidity_sweep":
            next_txn = _inject_legitimate_rapid_like(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        elif family == "near_rapid_low_exit":
            next_txn = _inject_near_rapid(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        elif family == "legitimate_chama_wallet_collection":
            next_txn = _inject_legitimate_chama_wallet_collection(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        elif family == "near_wallet_funnel_low_fanout":
            next_txn = _inject_near_wallet_funnel_low_fanout(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        else:
            next_txn = _inject_church_family_bulk(rng, member, fosa, source_accounts, sink_accounts, transactions, next_txn, index)
        if next_txn > before:
            _record_near_miss(families, family, str(member["member_id"]), next_txn - before)
            used_members.add(str(member["member_id"]))
            family_counts[family] += 1
            index += 1

    next_txn = _inject_fake_affordability_near_misses(
        rng,
        loans,
        members,
        account_by_member,
        source_accounts,
        sink_accounts,
        transactions,
        used_members,
        families,
        next_txn,
        target_count,
    )
    return next_txn, _near_miss_result(families)


def _inject_legitimate_structuring_like(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], agents_by_branch: dict[str, list[str]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 9, 1, 9, 0, tzinfo=EAT) + timedelta(days=index % 90)
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
    start = datetime(2024, 9, 20, 10, 0, tzinfo=EAT) + timedelta(days=index % 80)
    for offset in range(4):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(_txn_row(txn_id, start + timedelta(days=offset), rng.choice(source_accounts), fosa, member, "MPESA_PAYBILL_IN", "MPESA", "PAYBILL", float(rng.randrange(55_000, 90_000, 5_000)), "MPESA", "CUSTOMER", fosa.get("branch_id"), f"INCOMPLETE_STRUCTURING:{member['member_id']}:{offset}"))
    return next_txn


def _inject_legitimate_rapid_like(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 10, 2, 11, 0, tzinfo=EAT) + timedelta(days=index % 75)
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
    start = datetime(2024, 10, 18, 12, 0, tzinfo=EAT) + timedelta(days=index % 60)
    amount = float(rng.randrange(130_000, 300_000, 10_000))
    inbound_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(_txn_row(inbound_id, start, rng.choice(source_accounts), fosa, member, "PESALINK_IN", "PESALINK", "BANK_TRANSFER", amount, "BANK_PARTNER", "BANK", fosa.get("branch_id"), f"NEAR_RAPID_IN:{member['member_id']}"))
    out_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(_txn_row(out_id, start + timedelta(hours=12), fosa, rng.choice(sink_accounts), member, "PESALINK_OUT", "PESALINK", "BANK_TRANSFER", round(amount * rng.uniform(0.55, 0.70), 2), "BANK_PARTNER", "MERCHANT", fosa.get("branch_id"), f"NEAR_RAPID_OUT:{member['member_id']}"))
    return next_txn


def _inject_legitimate_chama_wallet_collection(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 6, 3, 9, 0, tzinfo=EAT) + timedelta(days=(index * 2) % 90)
    inbound_total = 0.0
    inbound_count = rng.randint(6, 8)
    for offset in range(inbound_count):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        txn_type, rail, channel, provider = _wallet_funnel_inbound_shape(offset)
        amount = float(rng.randrange(60_000, 95_000, 5_000))
        inbound_total += amount
        transactions.append(
            _txn_row(
                txn_id,
                start + timedelta(hours=offset * 8),
                rng.choice(source_accounts),
                fosa,
                member,
                txn_type,
                rail,
                channel,
                amount,
                provider,
                "WALLET_USER",
                fosa.get("branch_id"),
                f"LEGIT_CHAMA_WALLET_IN:{member['member_id']}:{index}:{offset}",
            )
        )
    payout_total = round(inbound_total * rng.uniform(0.58, 0.70), 2)
    payout_amounts = _allocate_amounts(payout_total, 3, rng)
    payout_shapes = [
        ("SUPPLIER_PAYMENT_OUT", "MPESA", "PAYBILL", "MPESA", "MERCHANT"),
        ("PESALINK_OUT", "PESALINK", "BANK_TRANSFER", "BANK_PARTNER", "BANK"),
        ("WALLET_P2P_OUT", "MPESA", "MOBILE_APP", "MPESA", "WALLET_USER"),
    ]
    last_inbound = start + timedelta(hours=(inbound_count - 1) * 8)
    for offset, amount in enumerate(payout_amounts):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        txn_type, rail, channel, provider, counterparty_type = payout_shapes[offset]
        transactions.append(
            _txn_row(
                txn_id,
                last_inbound + timedelta(hours=8 + offset * 9),
                fosa,
                sink_accounts[(index + offset) % len(sink_accounts)],
                member,
                txn_type,
                rail,
                channel,
                amount,
                provider,
                counterparty_type,
                fosa.get("branch_id"),
                f"LEGIT_CHAMA_WALLET_OUT:{member['member_id']}:{index}:{offset}",
            )
        )
    return next_txn


def _inject_near_wallet_funnel_low_fanout(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 7, 8, 9, 0, tzinfo=EAT) + timedelta(days=(index * 2) % 90)
    inbound_total = 0.0
    shared_counterparty = f"NEAR_WALLET_LOW_FANOUT:{member['member_id']}:{index}:SHARED"
    for offset in range(6):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        amount = float(rng.randrange(55_000, 95_000, 5_000))
        inbound_total += amount
        tx = _txn_row(
            txn_id,
            start + timedelta(hours=offset * 9),
            rng.choice(source_accounts),
            fosa,
            member,
            "MPESA_PAYBILL_IN",
            "MPESA",
            "PAYBILL",
            amount,
            "MPESA",
            "WALLET_USER",
            fosa.get("branch_id"),
            shared_counterparty if offset < 4 else f"NEAR_WALLET_LOW_FANOUT:{member['member_id']}:{index}:{offset}",
        )
        transactions.append(tx)
    txn_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(
        _txn_row(
            txn_id,
            start + timedelta(days=6, hours=6),
            fosa,
            rng.choice(sink_accounts),
            member,
            "MPESA_WALLET_TOPUP",
            "MPESA",
            "MOBILE_APP",
            round(inbound_total * rng.uniform(0.62, 0.74), 2),
            "MPESA",
            "WALLET_USER",
            fosa.get("branch_id"),
            f"NEAR_WALLET_LOW_FANOUT_OUT:{member['member_id']}:{index}",
        )
    )
    return next_txn


def _inject_church_family_bulk(rng: random.Random, member: dict[str, object], fosa: dict[str, object], source_accounts: list[dict[str, object]], sink_accounts: list[dict[str, object]], transactions: list[dict[str, object]], next_txn: int, index: int) -> int:
    start = datetime(2024, 8, 4, 10, 0, tzinfo=EAT) + timedelta(days=index % 110)
    persona = str(member.get("persona_type") or "")
    if persona == "CHURCH_ORG":
        for offset in range(4):
            txn_id = _txn_id(next_txn)
            next_txn += 1
            transactions.append(
                _txn_row(
                    txn_id,
                    start + timedelta(days=offset * 7),
                    rng.choice(source_accounts),
                    fosa,
                    member,
                    "CHURCH_COLLECTION_IN",
                    "MPESA" if offset % 2 else "CASH_BRANCH",
                    "PAYBILL" if offset % 2 else "BRANCH",
                    float(rng.randrange(70_000, 135_000, 5_000)),
                    "MPESA" if offset % 2 else "SACCO_CORE",
                    "CHURCH",
                    fosa.get("branch_id"),
                    f"LEGIT_CHURCH_COLLECTION:{member['member_id']}:{offset}",
                )
            )
        for offset, txn_type in enumerate(["SUPPLIER_PAYMENT_OUT", "PESALINK_OUT"]):
            txn_id = _txn_id(next_txn)
            next_txn += 1
            rail = "PESALINK" if txn_type == "PESALINK_OUT" else "MPESA"
            transactions.append(
                _txn_row(
                    txn_id,
                    start + timedelta(days=29, hours=offset * 3),
                    fosa,
                    sink_accounts[(index + offset) % len(sink_accounts)],
                    member,
                    txn_type,
                    rail,
                    "BANK_TRANSFER" if rail == "PESALINK" else "PAYBILL",
                    float(rng.randrange(45_000, 95_000, 5_000)),
                    "BANK_PARTNER" if rail == "PESALINK" else "MPESA",
                    "MERCHANT",
                    fosa.get("branch_id"),
                    f"LEGIT_CHURCH_OUT:{member['member_id']}:{offset}",
                )
            )
        return next_txn

    inbound_amount = float(rng.randrange(160_000, 340_000, 10_000))
    inbound_id = _txn_id(next_txn)
    next_txn += 1
    transactions.append(
        _txn_row(
            inbound_id,
            start,
            rng.choice(source_accounts),
            fosa,
            member,
            "PESALINK_IN",
            "REMITTANCE",
            "BANK_TRANSFER",
            inbound_amount,
            "BANK_PARTNER",
            "BANK",
            fosa.get("branch_id"),
            f"LEGIT_FAMILY_BULK_IN:{member['member_id']}",
        )
    )
    outflows = [
        ("SCHOOL_FEES_PAYMENT_OUT", 0.35, "MPESA", "PAYBILL"),
        ("HOUSEHOLD_SPEND_OUT", 0.22, "MPESA", "PAYBILL"),
        ("HOUSEHOLD_SPEND_OUT", 0.18, "MPESA", "PAYBILL"),
    ]
    for offset, (txn_type, share, rail, channel) in enumerate(outflows):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(
            _txn_row(
                txn_id,
                start + timedelta(hours=8 + offset * 7),
                fosa,
                sink_accounts[(index + offset) % len(sink_accounts)],
                member,
                txn_type,
                rail,
                channel,
                round(inbound_amount * share, 2),
                "MPESA" if rail == "MPESA" else "SACCO_CORE",
                "MERCHANT" if rail == "MPESA" else "SACCO",
                fosa.get("branch_id"),
                f"LEGIT_FAMILY_BULK_OUT:{member['member_id']}:{offset}",
            )
        )
    return next_txn


def _inject_fake_affordability_near_misses(
    rng: random.Random,
    loans: list[dict[str, object]],
    members: list[dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    used_members: set[str],
    families: dict[str, dict[str, object]],
    next_txn: int,
    target_count: int,
) -> int:
    if not loans or target_count <= 0:
        return next_txn
    member_by_id = {str(member["member_id"]): member for member in members}
    eligible_products = {"DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "BIASHARA_LOAN"}
    candidates = [
        loan
        for loan in loans
        if str(loan.get("product_code")) in eligible_products
        and str(loan.get("member_id")) in member_by_id
        and str(loan.get("member_id")) not in used_members
        and _first(account_by_member[str(loan["member_id"])], {"FOSA_CURRENT", "FOSA_SAVINGS"})
    ]
    candidates = _stratified_items_by_persona(candidates, rng, lambda loan: str(member_by_id[str(loan["member_id"])]["persona_type"]))
    target_by_family = {
        "legitimate_preloan_affordability_candidate": target_count,
        "near_affordability_low_growth": target_count,
    }
    family_counts = Counter()
    for index, loan in enumerate(candidates, start=1):
        family = next((name for name, target in target_by_family.items() if family_counts[name] < target), None)
        if family is None:
            break
        member_id = str(loan["member_id"])
        member = member_by_id[member_id]
        fosa = _first(account_by_member[member_id], {"FOSA_CURRENT", "FOSA_SAVINGS"})
        if not fosa:
            continue
        before = next_txn
        next_txn = _inject_preloan_inflow_sequence(
            rng,
            member,
            fosa,
            loan,
            source_accounts,
            sink_accounts,
            transactions,
            next_txn,
            index,
            trigger_rule=family == "legitimate_preloan_affordability_candidate",
        )
        if next_txn > before:
            _record_near_miss(families, family, member_id, next_txn - before)
            used_members.add(member_id)
            family_counts[family] += 1
    return next_txn


def _inject_preloan_inflow_sequence(
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    loan: dict[str, object],
    source_accounts: list[dict[str, object]],
    sink_accounts: list[dict[str, object]],
    transactions: list[dict[str, object]],
    next_txn: int,
    index: int,
    trigger_rule: bool,
) -> int:
    application_ts = datetime.fromisoformat(f"{loan['application_date']}T09:00:00+03:00")
    start = application_ts - timedelta(days=18 + index % 9)
    if start.year != application_ts.year:
        return next_txn
    amounts = [float(rng.randrange(55_000, 95_000, 5_000)), float(rng.randrange(45_000, 85_000, 5_000))]
    if trigger_rule:
        amounts.append(float(rng.randrange(35_000, 75_000, 5_000)))
    else:
        amounts = [float(rng.randrange(25_000, 45_000, 5_000)), float(rng.randrange(20_000, 40_000, 5_000))]
    for offset, amount in enumerate(amounts):
        txn_id = _txn_id(next_txn)
        next_txn += 1
        rail = ["REMITTANCE", "PESALINK", "MPESA"][offset % 3]
        transactions.append(
            _txn_row(
                txn_id,
                start + timedelta(days=offset * 4, hours=offset),
                rng.choice(source_accounts),
                fosa,
                member,
                "PESALINK_IN" if rail in {"REMITTANCE", "PESALINK"} else "MPESA_PAYBILL_IN",
                rail,
                "BANK_TRANSFER" if rail in {"REMITTANCE", "PESALINK"} else "PAYBILL",
                amount,
                "BANK_PARTNER" if rail in {"REMITTANCE", "PESALINK"} else "MPESA",
                "BANK" if rail in {"REMITTANCE", "PESALINK"} else "CUSTOMER",
                fosa.get("branch_id"),
                f"LEGIT_PRELOAN_INFLOW:{member['member_id']}:{loan['loan_id']}:{offset}",
            )
        )
    if not trigger_rule:
        txn_id = _txn_id(next_txn)
        next_txn += 1
        transactions.append(
            _txn_row(
                txn_id,
                start + timedelta(days=10),
                fosa,
                rng.choice(sink_accounts),
                member,
                "SCHOOL_FEES_PAYMENT_OUT",
                "MPESA",
                "PAYBILL",
                round(sum(amounts) * rng.uniform(0.55, 0.80), 2),
                "MPESA",
                "MERCHANT",
                fosa.get("branch_id"),
                f"LEGIT_PRELOAN_OUTFLOW:{member['member_id']}:{loan['loan_id']}",
            )
        )
    return next_txn


def _append_guarantor_cycle(
    rng: random.Random,
    loans: list[dict[str, object]],
    member_by_id: dict[str, dict[str, object]],
    account_by_member: dict[str, list[dict[str, object]]],
    guarantors: list[dict[str, object]],
    active_counts: Counter[str],
    guarantee_index_start: int,
) -> int:
    group_size = len(loans)
    pending: list[tuple[dict[str, object], str]] = []
    for index, borrower_loan in enumerate(loans):
        guarantor_loan = loans[(index - 1) % group_size]
        borrower_id = str(borrower_loan["member_id"])
        guarantor_id = str(guarantor_loan["member_id"])
        if borrower_id == guarantor_id or active_counts[guarantor_id] >= 5:
            return 0
        guarantee = _guarantee_row(
            rng,
            f"GUA{guarantee_index_start + index:06d}",
            borrower_loan,
            member_by_id[guarantor_id],
            member_by_id[borrower_id],
            account_by_member,
        )
        if not guarantee:
            return 0
        pending.append((guarantee, guarantor_id))
    for guarantee, guarantor_id in pending:
        guarantors.append(guarantee)
        active_counts[guarantor_id] += 1
    return len(pending)


def _guarantee_row(
    rng: random.Random,
    guarantee_id: str,
    borrower_loan: dict[str, object],
    guarantor: dict[str, object],
    borrower: dict[str, object],
    account_by_member: dict[str, list[dict[str, object]]],
) -> dict[str, object] | None:
    bosa = _first(account_by_member[str(guarantor["member_id"])], {"BOSA_DEPOSIT"})
    if not bosa:
        return None
    principal = float(borrower_loan["principal_kes"])
    capacity = float(bosa["current_balance_kes"]) * 1.5
    amount = min(capacity * rng.uniform(0.18, 0.32), principal * rng.uniform(0.18, 0.30))
    if amount <= 5_000:
        return None
    return {
        "guarantee_id": guarantee_id,
        "loan_id": borrower_loan["loan_id"],
        "borrower_member_id": borrower["member_id"],
        "guarantor_member_id": guarantor["member_id"],
        "guarantee_amount_kes": round(amount, 2),
        "guarantee_pct": round(amount / principal, 4),
        "pledge_date": borrower_loan["approval_date"],
        "release_date": None,
        "guarantor_deposit_balance_at_pledge_kes": round(float(bosa["current_balance_kes"]), 2),
        "relationship_type": rng.choice(["SACCO_MEMBER", "FRIEND", "BUSINESS_ASSOCIATE"]),
        "guarantor_capacity_remaining_kes": round(capacity - amount, 2),
    }


def _loan_context_transactions(transactions: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    by_loan_account: dict[str, list[dict[str, object]]] = defaultdict(list)
    for txn in transactions:
        txn_type = str(txn.get("txn_type") or "")
        debit = str(txn.get("account_id_dr") or "")
        credit = str(txn.get("account_id_cr") or "")
        if txn_type == "LOAN_DISBURSEMENT":
            by_loan_account[debit].append(txn)
        elif txn_type in {"LOAN_REPAYMENT", "CHECKOFF_LOAN_RECOVERY", "PENALTY_POST"}:
            by_loan_account[credit if txn_type != "PENALTY_POST" else debit].append(txn)
    for rows in by_loan_account.values():
        rows.sort(key=lambda row: str(row["timestamp"]))
    return by_loan_account


def _active_guarantee_counts(guarantors: list[dict[str, object]], loans: list[dict[str, object]]) -> Counter[str]:
    loan_by_id = {str(loan["loan_id"]): loan for loan in loans}
    counts: Counter[str] = Counter()
    for guarantee in guarantors:
        loan = loan_by_id.get(str(guarantee.get("loan_id") or ""))
        if loan and loan.get("performing_status") != "CLOSED":
            counts[str(guarantee["guarantor_member_id"])] += 1
    return counts


def _next_guarantee_index(guarantors: list[dict[str, object]]) -> int:
    indices = [int(str(row["guarantee_id"])[3:]) for row in guarantors if str(row.get("guarantee_id") or "").startswith("GUA")]
    return max(indices, default=0) + 1


def _empty_near_miss_families() -> dict[str, dict[str, object]]:
    return {
        "legitimate_structuring_like": {
            "target_typology": "STRUCTURING",
            "description": "Legitimate high-cash operating deposits that can satisfy the structuring rule.",
            "expected_rule_effect": "false_positive_pressure",
        },
        "incomplete_structuring": {
            "target_typology": "STRUCTURING",
            "description": "Sub-threshold deposits that miss the minimum-count rule.",
            "expected_rule_effect": "negative_control",
        },
        "legitimate_sme_liquidity_sweep": {
            "target_typology": "RAPID_PASS_THROUGH",
            "description": "Legitimate SME settlement followed by supplier settlement.",
            "expected_rule_effect": "false_positive_pressure",
        },
        "near_rapid_low_exit": {
            "target_typology": "RAPID_PASS_THROUGH",
            "description": "Large inbound movement with a below-threshold exit ratio.",
            "expected_rule_effect": "negative_control",
        },
        "church_family_bulk_payments": {
            "target_typology": "STRUCTURING,RAPID_PASS_THROUGH",
            "description": "Legitimate church collections, donor support, school fees, and family bulk payments.",
            "expected_rule_effect": "negative_control",
        },
        "legitimate_chama_wallet_collection": {
            "target_typology": "WALLET_FUNNELING",
            "description": "Normal chama, welfare, or project collection credits from many wallets followed by legitimate vendor or member payouts.",
            "expected_rule_effect": "false_positive_pressure",
        },
        "near_wallet_funnel_low_fanout": {
            "target_typology": "WALLET_FUNNELING",
            "description": "Many wallet credits followed by high outbound value but too few source or destination counterparties.",
            "expected_rule_effect": "negative_control",
        },
        "legitimate_preloan_affordability_candidate": {
            "target_typology": "FAKE_AFFORDABILITY_BEFORE_LOAN",
            "description": "Legitimate pre-loan remittance, donor, harvest, or business inflow that can satisfy the fake-affordability rule.",
            "expected_rule_effect": "false_positive_pressure",
        },
        "near_affordability_low_growth": {
            "target_typology": "FAKE_AFFORDABILITY_BEFORE_LOAN",
            "description": "Pre-loan inflows offset by legitimate spending so balance growth stays below rule threshold.",
            "expected_rule_effect": "negative_control",
        },
    }


def _record_near_miss(families: dict[str, dict[str, object]], family: str, member_id: str, txn_count: int) -> None:
    families.setdefault(family, {"target_typology": "UNKNOWN", "description": "", "expected_rule_effect": "negative_control"})
    families[family].setdefault("member_ids", set())
    families[family].setdefault("transaction_count", 0)
    families[family]["member_ids"].add(member_id)
    families[family]["transaction_count"] = int(families[family]["transaction_count"]) + txn_count


def _record_guarantee_near_miss(families: dict[str, dict[str, object]], family: str, member_id: str, guarantee_count: int) -> None:
    families.setdefault(family, {"target_typology": "UNKNOWN", "description": "", "expected_rule_effect": "negative_control"})
    families[family].setdefault("member_ids", set())
    families[family].setdefault("guarantee_count", 0)
    families[family]["member_ids"].add(member_id)
    families[family]["guarantee_count"] = int(families[family]["guarantee_count"]) + guarantee_count


def _near_miss_result(families: dict[str, dict[str, object]]) -> dict[str, object]:
    serialized: dict[str, dict[str, object]] = {}
    all_members: set[str] = set()
    transaction_count = 0
    guarantee_count = 0
    for family, section in sorted(families.items()):
        member_ids = set(section.get("member_ids", set()))
        count = int(section.get("transaction_count", 0) or 0)
        guarantees = int(section.get("guarantee_count", 0) or 0)
        if not member_ids and count == 0 and guarantees == 0:
            continue
        all_members.update(str(member_id) for member_id in member_ids)
        transaction_count += count
        guarantee_count += guarantees
        serialized[family] = {
            "target_typology": section.get("target_typology", "UNKNOWN"),
            "description": section.get("description", ""),
            "expected_rule_effect": section.get("expected_rule_effect", "negative_control"),
            "member_count": len(member_ids),
            "transaction_count": count,
        }
        if guarantees:
            serialized[family]["guarantee_count"] = guarantees
        if "group_count" in section:
            serialized[family]["group_count"] = int(section.get("group_count") or 0)
    return {
        "status": "available" if serialized else "not_applicable",
        "family_count": len(serialized),
        "families": serialized,
        "near_miss_member_count": len(all_members),
        "near_miss_transaction_count": transaction_count,
        "near_miss_guarantee_count": guarantee_count,
    }


def _merge_near_miss_stats(primary: dict[str, object], secondary: dict[str, object]) -> dict[str, object]:
    families: dict[str, dict[str, object]] = {}
    for source in (primary, secondary):
        for family, section in (source.get("families") or {}).items():
            families[family] = dict(section)
    return {
        "status": "available" if families else "not_applicable",
        "family_count": len(families),
        "families": families,
        "near_miss_member_count": sum(int(section.get("member_count") or 0) for section in families.values()),
        "near_miss_transaction_count": sum(int(section.get("transaction_count") or 0) for section in families.values()),
        "near_miss_guarantee_count": sum(int(section.get("guarantee_count") or 0) for section in families.values()),
        "device_sharing_near_miss_group_count": secondary.get("device_sharing_near_miss_group_count", 0),
        "device_sharing_near_miss_member_count": secondary.get("device_sharing_near_miss_member_count", 0),
        "device_sharing_near_miss_transaction_count": secondary.get("device_sharing_near_miss_transaction_count", 0),
    }


def _device_mule_inbound_shape(offset: int) -> tuple[str, str, str, str, str]:
    variants = [
        ("MPESA_PAYBILL_IN", "MPESA", "PAYBILL", "MPESA", "CUSTOMER"),
        ("PESALINK_IN", "PESALINK", "BANK_TRANSFER", "BANK_PARTNER", "BANK"),
        ("BUSINESS_SETTLEMENT_IN", "MPESA", "PAYBILL", "MPESA", "MERCHANT"),
    ]
    return variants[offset % len(variants)]


def _wallet_funnel_inbound_shape(offset: int) -> tuple[str, str, str, str]:
    variants = [
        ("MPESA_PAYBILL_IN", "MPESA", "PAYBILL", "MPESA"),
        ("WALLET_P2P_IN", "MPESA", "MOBILE_APP", "MPESA"),
        ("BUSINESS_SETTLEMENT_IN", "MPESA", "TILL", "MPESA"),
        ("WALLET_P2P_IN", "AIRTEL_MONEY", "MOBILE_APP", "AIRTEL_MONEY"),
    ]
    return variants[offset % len(variants)]


def _wallet_funnel_outbound_shape(offset: int) -> tuple[str, str, str, str]:
    variants = [
        ("MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", "MPESA"),
        ("WALLET_P2P_OUT", "MPESA", "MOBILE_APP", "MPESA"),
        ("PESALINK_OUT", "PESALINK", "BANK_TRANSFER", "BANK_PARTNER"),
        ("SUPPLIER_PAYMENT_OUT", "MPESA", "PAYBILL", "MPESA"),
    ]
    return variants[offset % len(variants)]


def _members_without_shared_devices(world: InstitutionWorld) -> set[str]:
    members: set[str] = set()
    blocked: set[str] = set()
    for device in world.devices:
        member_id = str(device["member_id"])
        if device.get("shared_device_group"):
            blocked.add(member_id)
        else:
            members.add(member_id)
    return members - blocked


def _assign_shared_device_group(world: InstitutionWorld, member_ids: list[str], group_name: str) -> str | None:
    devices_by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for device in world.devices:
        devices_by_member[str(device["member_id"])].append(device)
    if any(not devices_by_member.get(member_id) for member_id in member_ids):
        return None
    for member_id in member_ids:
        for device in devices_by_member[member_id]:
            device["shared_device_group"] = group_name
    return str(devices_by_member[member_ids[0]][0]["device_id"])


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


def _random_pattern_start(config: WorldConfig, rng: random.Random, max_duration_days: int) -> datetime:
    simulation_start = _simulation_datetime(config.start_date, 7, 0)
    latest_start = _simulation_datetime(config.end_date, 18, 0) - timedelta(days=max_duration_days)
    if latest_start <= simulation_start:
        return simulation_start
    day_span = max(0, (latest_start.date() - simulation_start.date()).days)
    return simulation_start + timedelta(
        days=rng.randint(0, day_span),
        hours=rng.randint(1, 10),
        minutes=rng.choice([0, 10, 20, 30, 40, 50]),
    )


def _distributed_pattern_start(config: WorldConfig, rng: random.Random, sequence_index: int, sequence_total: int, max_duration_days: int) -> datetime:
    simulation_start = _simulation_datetime(config.start_date, 7, 0)
    simulation_end = _simulation_datetime(config.end_date, 18, 0)
    latest_start = simulation_end - timedelta(days=max_duration_days)
    if latest_start <= simulation_start:
        return simulation_start

    month_count = _simulation_month_count(simulation_start, simulation_end)
    if month_count > 1 and sequence_total > 1:
        if sequence_total >= month_count:
            month_offset = (sequence_index - 1) % month_count
            cycle_index = (sequence_index - 1) // month_count
            cycle_count = max(1, (sequence_total + month_count - 1) // month_count)
        else:
            month_offset = round((sequence_index - 1) * (month_count - 1) / max(1, sequence_total - 1))
            cycle_index = 0
            cycle_count = 1
        month_start = _add_months(simulation_start, month_offset).replace(day=1, hour=7, minute=0, second=0, microsecond=0)
        if month_start < simulation_start:
            month_start = simulation_start
        month_end = min(_month_end(month_start).replace(hour=18, minute=0, second=0, microsecond=0), latest_start)
        if month_end >= month_start:
            available_days = max(0, (month_end.date() - month_start.date()).days)
            slot_width = max(1, (available_days + 1) // cycle_count)
            slot_start = min(available_days, cycle_index * slot_width)
            slot_end = min(available_days, slot_start + slot_width - 1)
            return month_start + timedelta(
                days=rng.randint(slot_start, max(slot_start, slot_end)),
                hours=rng.randint(1, 10),
                minutes=rng.choice([0, 10, 20, 30, 40, 50]),
            )

    total_days = max(1, (latest_start.date() - simulation_start.date()).days)
    slots = max(1, sequence_total)
    slot_width = max(1, total_days // slots)
    slot_start = min(total_days - 1, max(0, (sequence_index - 1) * slot_width))
    slot_end = min(total_days - 1, slot_start + slot_width - 1)
    return simulation_start + timedelta(
        days=rng.randint(slot_start, max(slot_start, slot_end)),
        hours=rng.randint(1, 10),
        minutes=rng.choice([0, 10, 20, 30, 40, 50]),
    )


def _simulation_month_count(start: datetime, end: datetime) -> int:
    return max(1, (end.year - start.year) * 12 + end.month - start.month + 1)


def _add_months(value: datetime, offset: int) -> datetime:
    month_index = value.month - 1 + offset
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    month_start = datetime(year, month, 1, value.hour, value.minute, value.second, value.microsecond, tzinfo=value.tzinfo)
    day = min(value.day, _month_end(month_start).day)
    return month_start.replace(day=day)


def _month_end(value: datetime) -> datetime:
    if value.month == 12:
        return value.replace(month=12, day=31)
    return value.replace(month=value.month + 1, day=1) - timedelta(days=1)


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


def _stratified_items_by_persona(items: list, rng: random.Random, persona_for_item) -> list:
    buckets: dict[str, list] = defaultdict(list)
    for item in items:
        buckets[str(persona_for_item(item))].append(item)
    personas = sorted(buckets)
    rng.shuffle(personas)
    for bucket in buckets.values():
        rng.shuffle(bucket)
    ordered: list = []
    while personas:
        next_personas: list[str] = []
        for persona in personas:
            bucket = buckets[persona]
            if bucket:
                ordered.append(bucket.pop())
            if bucket:
                next_personas.append(persona)
        personas = next_personas
    return ordered


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
