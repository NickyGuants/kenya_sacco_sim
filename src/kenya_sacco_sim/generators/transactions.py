from __future__ import annotations

import random
from collections import defaultdict
from datetime import date, datetime, timedelta

from kenya_sacco_sim.core.config import WorldConfig


def generate_transactions(config: WorldConfig, members: list[dict[str, object]], accounts: list[dict[str, object]]) -> list[dict[str, object]]:
    rng = random.Random(config.seed + 303)
    balances = {str(account["account_id"]): float(account["opening_balance_kes"]) for account in accounts}
    by_member = _accounts_by_member(accounts)
    source_account = next(account for account in accounts if account["account_type"] == "SOURCE_ACCOUNT")
    sink_account = next(account for account in accounts if account["account_type"] == "SINK_ACCOUNT")
    transactions: list[dict[str, object]] = []
    txn_seq = 0

    def emit(
        timestamp: datetime,
        debit_account: dict[str, object],
        credit_account: dict[str, object],
        member: dict[str, object] | None,
        txn_type: str,
        rail: str,
        channel: str,
        amount: float,
        provider: str | None = None,
        counterparty_type: str = "EXTERNAL_UNKNOWN",
        branch_id: object | None = None,
    ) -> None:
        nonlocal txn_seq
        amount = round(float(amount), 2)
        if amount <= 0:
            return
        timestamp = _bounded_timestamp(timestamp, config)
        debit_id = str(debit_account["account_id"])
        credit_id = str(credit_account["account_id"])
        balances[debit_id] -= amount
        balances[credit_id] += amount
        txn_seq += 1
        transactions.append(
            {
                "txn_id": f"TXN_{txn_seq:09d}",
                "timestamp": timestamp.isoformat(timespec="seconds"),
                "institution_id": member["institution_id"] if member else None,
                "account_id_dr": debit_id,
                "account_id_cr": credit_id,
                "member_id_primary": member["member_id"] if member else None,
                "txn_type": txn_type,
                "rail": rail,
                "channel": channel,
                "provider": provider,
                "counterparty_type": counterparty_type,
                "counterparty_id_hash": None,
                "amount_kes": amount,
                "fee_kes": 0.0,
                "currency": config.currency,
                "narrative": _narrative(txn_type),
                "reference": f"REF{txn_seq:09d}",
                "branch_id": branch_id,
                "agent_id": None,
                "device_id": None,
                "geo_bucket": member["county"] if member else None,
                "batch_id": None,
                "balance_after_dr_kes": round(balances[debit_id], 2),
                "balance_after_cr_kes": round(balances[credit_id], 2),
                "is_reversal": False,
            }
        )

    for member in members:
        member_accounts = by_member[str(member["member_id"])]
        fosa = _first(member_accounts, {"FOSA_SAVINGS", "FOSA_CURRENT"})
        bosa = _first(member_accounts, {"BOSA_DEPOSIT"})
        wallet = _first(member_accounts, {"MPESA_WALLET"})
        if not fosa:
            continue

        persona = str(member["persona_type"])
        monthly_income = float(member["declared_monthly_income_kes"])
        if persona in {"SALARIED_TEACHER", "COUNTY_WORKER"} and bosa:
            _salary_checkoff_wallet_spend(config, rng, member, fosa, bosa, wallet, source_account, sink_account, monthly_income, emit)
        elif persona == "SME_OWNER":
            _sme_receipts_monday_deposit(config, rng, member, fosa, bosa, wallet, source_account, sink_account, monthly_income, emit)
        elif persona == "DIASPORA_SUPPORTED":
            _diaspora_support_household(config, rng, member, fosa, wallet, source_account, sink_account, monthly_income, emit)
        elif persona == "BODA_BODA_OPERATOR":
            _boda_cash_cycle(config, rng, member, fosa, bosa, wallet, source_account, sink_account, monthly_income, emit)
        elif persona == "FARMER_SEASONAL":
            _farmer_cash_cycle(config, rng, member, fosa, bosa, wallet, source_account, sink_account, monthly_income, emit)

    transactions.sort(key=lambda row: (str(row["timestamp"]), str(row["txn_id"])))
    _recompute_balances(transactions, accounts)
    return transactions


def _salary_checkoff_wallet_spend(
    config: WorldConfig,
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    bosa: dict[str, object],
    wallet: dict[str, object] | None,
    source: dict[str, object],
    sink: dict[str, object],
    monthly_income: float,
    emit,
) -> None:
    for month in range(1, config.months + 1):
        day = min(rng.choice([24, 25, 26, 27, 28]), _last_day(2024, month))
        payday = datetime(2024, month, day, rng.randint(8, 17), rng.choice([0, 15, 30, 45]))
        bosa_amount = round(monthly_income * rng.uniform(0.08, 0.16), 2)
        net_salary = round(monthly_income - bosa_amount, 2)
        emit(payday, source, bosa, member, "CHECKOFF_DEPOSIT", "PAYROLL_CHECKOFF", "PAYROLL_FILE", bosa_amount, "SACCO_CORE", "EMPLOYER", bosa.get("branch_id"))
        emit(payday + timedelta(minutes=3), source, fosa, member, "SALARY_IN", "PAYROLL_CHECKOFF", "PAYROLL_FILE", net_salary, "SACCO_CORE", "EMPLOYER", fosa.get("branch_id"))
        spend_base = net_salary
        if wallet and rng.random() < 0.82:
            topup = min(round(net_salary * rng.uniform(0.18, 0.38), 2), float(fosa["current_balance_kes"]) + net_salary - 500)
            emit(payday + timedelta(days=rng.randint(1, 3), hours=2), fosa, wallet, member, "MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", topup, "MPESA", "WALLET_USER")
            spend_base = topup
            spend_account = wallet
            rail = "MPESA"
            channel = "PAYBILL"
            provider = "MPESA"
        else:
            spend_account = fosa
            rail = "SACCO_INTERNAL"
            channel = "MOBILE_APP"
            provider = "SACCO_CORE"
        spend = round(spend_base * rng.uniform(0.25, 0.55), 2)
        emit(payday + timedelta(days=rng.randint(4, 10)), spend_account, sink, member, "HOUSEHOLD_SPEND_OUT", rail, channel, spend, provider, "MERCHANT")


def _sme_receipts_monday_deposit(
    config: WorldConfig,
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    bosa: dict[str, object] | None,
    wallet: dict[str, object] | None,
    source: dict[str, object],
    sink: dict[str, object],
    monthly_income: float,
    emit,
) -> None:
    for month in range(1, config.months + 1):
        receipt_count = rng.randint(4, 7)
        inflow_total = 0.0
        for _ in range(receipt_count):
            day = rng.randint(1, min(18, _last_day(2024, month)))
            amount = round(monthly_income * rng.uniform(0.08, 0.20), 2)
            inflow_total += amount
            emit(datetime(2024, month, day, rng.randint(9, 20), rng.choice([0, 10, 20, 30, 40, 50])), source, fosa, member, "BUSINESS_SETTLEMENT_IN", "MPESA", "PAYBILL", amount, "MPESA", "CUSTOMER", fosa.get("branch_id"))
        supplier = round(inflow_total * rng.uniform(0.35, 0.60), 2)
        emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(23, 28)), 11, 0), fosa, sink, member, "SUPPLIER_PAYMENT_OUT", rng.choice(["PESALINK", "MPESA"]), rng.choice(["BANK_TRANSFER", "PAYBILL"]), supplier, "BANK_PARTNER", "MERCHANT")
        if bosa and rng.random() < 0.35:
            topup = round(inflow_total * rng.uniform(0.04, 0.10), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(24, 28)), 10, 15), fosa, bosa, member, "BOSA_DEP_TOPUP", "SACCO_INTERNAL", "MOBILE_APP", topup, "SACCO_CORE", "SACCO")
        if wallet and rng.random() < 0.55:
            cashout = round(inflow_total * rng.uniform(0.08, 0.18), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(24, 28)), 16, 0), fosa, wallet, member, "MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", cashout, "MPESA", "WALLET_USER")


def _diaspora_support_household(
    config: WorldConfig,
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    wallet: dict[str, object] | None,
    source: dict[str, object],
    sink: dict[str, object],
    monthly_income: float,
    emit,
) -> None:
    for month in range(1, config.months + 1):
        if rng.random() > 0.65:
            continue
        day = rng.randint(3, 18)
        inflow = round(monthly_income * rng.uniform(0.55, 1.25), 2)
        ts = datetime(2024, month, day, rng.randint(8, 18), rng.choice([0, 20, 40]))
        emit(ts, source, fosa, member, "PESALINK_IN", "REMITTANCE", "BANK_TRANSFER", inflow, "BANK_PARTNER", "BANK", fosa.get("branch_id"))
        spend_account = fosa
        rail = "SACCO_INTERNAL"
        channel = "MOBILE_APP"
        provider = "SACCO_CORE"
        spend_base = inflow
        if wallet and rng.random() < 0.72:
            topup = round(inflow * rng.uniform(0.25, 0.55), 2)
            emit(ts + timedelta(days=1, hours=1), fosa, wallet, member, "MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", topup, "MPESA", "WALLET_USER")
            spend_account = wallet
            rail = "MPESA"
            channel = "PAYBILL"
            provider = "MPESA"
            spend_base = topup
        spend = round(spend_base * rng.uniform(0.45, 0.80), 2)
        txn_type = "SCHOOL_FEES_PAYMENT_OUT" if month in {1, 5, 8} and rng.random() < 0.45 else "HOUSEHOLD_SPEND_OUT"
        emit(ts + timedelta(days=rng.randint(2, 8)), spend_account, sink, member, txn_type, rail, channel, spend, provider, "MERCHANT")


def _boda_cash_cycle(
    config: WorldConfig,
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    bosa: dict[str, object] | None,
    wallet: dict[str, object] | None,
    source: dict[str, object],
    sink: dict[str, object],
    monthly_income: float,
    emit,
) -> None:
    for month in range(1, config.months + 1):
        monthly_cash = monthly_income * rng.uniform(0.75, 1.15)
        for cycle in range(2):
            deposit_day = min(_last_day(2024, month), 5 + cycle * 14 + rng.randint(0, 3))
            deposit = round(monthly_cash * rng.uniform(0.14, 0.22), 2)
            rail = rng.choices(["CASH_AGENT", "CASH_BRANCH"], weights=[0.72, 0.28], k=1)[0]
            channel = "AGENT" if rail == "CASH_AGENT" else "BRANCH"
            emit(datetime(2024, month, deposit_day, rng.randint(8, 18), rng.choice([0, 15, 30, 45])), source, fosa, member, "FOSA_CASH_DEPOSIT", rail, channel, deposit, "SACCO_CORE", "CUSTOMER", fosa.get("branch_id"))
            cashout = round(deposit * rng.uniform(0.30, 0.55), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), deposit_day + rng.randint(1, 3)), rng.randint(10, 19), 0), fosa, sink, member, "FOSA_CASH_WITHDRAWAL", rail, channel, cashout, "SACCO_CORE", "AGENT", fosa.get("branch_id"))

        if wallet and rng.random() < 0.70:
            topup = round(monthly_cash * rng.uniform(0.08, 0.16), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(20, 27)), 17, 30), fosa, wallet, member, "MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", topup, "MPESA", "WALLET_USER")
        if bosa and rng.random() < 0.25:
            bosa_topup = round(monthly_cash * rng.uniform(0.04, 0.08), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(24, 28)), 9, 30), fosa, bosa, member, "BOSA_DEP_TOPUP", "SACCO_INTERNAL", "BRANCH", bosa_topup, "SACCO_CORE", "SACCO", fosa.get("branch_id"))


def _farmer_cash_cycle(
    config: WorldConfig,
    rng: random.Random,
    member: dict[str, object],
    fosa: dict[str, object],
    bosa: dict[str, object] | None,
    wallet: dict[str, object] | None,
    source: dict[str, object],
    sink: dict[str, object],
    monthly_income: float,
    emit,
) -> None:
    harvest_months = {3, 4, 8, 9, 12}
    for month in range(1, config.months + 1):
        if month in harvest_months:
            inflow_count = rng.randint(1, 3)
            month_income = monthly_income * rng.uniform(1.7, 3.2)
        else:
            inflow_count = 1 if rng.random() < 0.33 else 0
            month_income = monthly_income * rng.uniform(0.35, 0.80)
        deposited = 0.0
        for _ in range(inflow_count):
            day = rng.randint(4, min(18, _last_day(2024, month)))
            amount = round(month_income / max(inflow_count, 1) * rng.uniform(0.75, 1.20), 2)
            deposited += amount
            rail = rng.choices(["CASH_AGENT", "CASH_BRANCH", "MPESA"], weights=[0.55, 0.30, 0.15], k=1)[0]
            channel = "AGENT" if rail == "CASH_AGENT" else "BRANCH" if rail == "CASH_BRANCH" else "PAYBILL"
            txn_type = "FOSA_CASH_DEPOSIT" if rail in {"CASH_AGENT", "CASH_BRANCH"} else "MPESA_PAYBILL_IN"
            provider = "SACCO_CORE" if rail in {"CASH_AGENT", "CASH_BRANCH"} else "MPESA"
            emit(datetime(2024, month, day, rng.randint(8, 16), rng.choice([0, 20, 40])), source, fosa, member, txn_type, rail, channel, amount, provider, "CUSTOMER", fosa.get("branch_id"))

        if deposited <= 0:
            continue
        if bosa and month in harvest_months:
            topup = round(deposited * rng.uniform(0.12, 0.24), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(20, 27)), 10, 0), fosa, bosa, member, "BOSA_DEP_TOPUP", "SACCO_INTERNAL", "BRANCH", topup, "SACCO_CORE", "SACCO", fosa.get("branch_id"))
        withdrawal = round(deposited * rng.uniform(0.25, 0.45), 2)
        rail = rng.choice(["CASH_AGENT", "CASH_BRANCH"])
        emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(21, 28)), 14, 0), fosa, sink, member, "FOSA_CASH_WITHDRAWAL", rail, "AGENT" if rail == "CASH_AGENT" else "BRANCH", withdrawal, "SACCO_CORE", "AGENT", fosa.get("branch_id"))
        if wallet and rng.random() < 0.40:
            topup = round(deposited * rng.uniform(0.08, 0.16), 2)
            emit(datetime(2024, month, min(_last_day(2024, month), rng.randint(22, 28)), 16, 30), fosa, wallet, member, "MPESA_WALLET_TOPUP", "MPESA", "MOBILE_APP", topup, "MPESA", "WALLET_USER")


def _accounts_by_member(accounts: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for account in accounts:
        member_id = account.get("member_id")
        if member_id:
            by_member[str(member_id)].append(account)
    return by_member


def _first(accounts: list[dict[str, object]], account_types: set[str]) -> dict[str, object] | None:
    for account in accounts:
        if str(account["account_type"]) in account_types:
            return account
    return None


def _last_day(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - timedelta(days=1)).day


def _narrative(txn_type: str) -> str:
    return txn_type.replace("_", " ").title()


def _bounded_timestamp(timestamp: datetime, config: WorldConfig) -> datetime:
    start = datetime.fromisoformat(f"{config.start_date}T00:00:00")
    end = datetime.fromisoformat(f"{config.end_date}T23:59:59")
    if timestamp < start:
        return start
    if timestamp > end:
        return end
    return timestamp


def _recompute_balances(transactions: list[dict[str, object]], accounts: list[dict[str, object]]) -> None:
    balances = {str(account["account_id"]): float(account["opening_balance_kes"]) for account in accounts}
    for txn in transactions:
        amount = float(txn["amount_kes"])
        debit_id = str(txn["account_id_dr"])
        credit_id = str(txn["account_id_cr"])
        balances[debit_id] -= amount
        balances[credit_id] += amount
        txn["balance_after_dr_kes"] = round(balances[debit_id], 2)
        txn["balance_after_cr_kes"] = round(balances[credit_id], 2)
    for account in accounts:
        account["current_balance_kes"] = round(balances[str(account["account_id"])], 2)
