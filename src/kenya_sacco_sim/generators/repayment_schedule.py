from __future__ import annotations

import random


def scheduled_repayment_count(disbursement_month: int, tenor_months: int, simulation_months: int) -> int:
    months_available = max(0, min(simulation_months, 12) - disbursement_month)
    return min(tenor_months, months_available)


def missed_payment_count(status: str, scheduled_payments: int, rng: random.Random) -> int:
    if status == "CURRENT":
        return 0
    if status == "IN_ARREARS":
        return min(scheduled_payments, rng.randint(1, min(2, scheduled_payments)))
    if status == "DEFAULTED":
        return min(scheduled_payments, rng.randint(max(1, scheduled_payments // 2), scheduled_payments))
    return 0
