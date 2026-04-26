from __future__ import annotations

import random

from kenya_sacco_sim.core.id_factory import IdFactory


GUARANTEED_PRODUCTS = {"DEVELOPMENT_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE"}


def select_guarantors(
    rng: random.Random,
    guarantee_ids: IdFactory,
    loan: dict[str, object],
    borrower: dict[str, object],
    members_by_institution: dict[str, list[dict[str, object]]],
    member_accounts: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    principal = float(loan["principal_kes"])
    required = principal * 0.60
    candidates: list[tuple[dict[str, object], dict[str, object], float]] = []
    for candidate in members_by_institution[str(borrower["institution_id"])]:
        if candidate["member_id"] == borrower["member_id"] or candidate["member_type"] != "INDIVIDUAL":
            continue
        bosa = _first(member_accounts[str(candidate["member_id"])], {"BOSA_DEPOSIT"})
        if not bosa:
            continue
        capacity = float(bosa["current_balance_kes"]) * 1.5
        if capacity > 5_000:
            candidates.append((candidate, bosa, capacity))

    rng.shuffle(candidates)
    candidates.sort(key=lambda item: item[2], reverse=True)
    selected: list[dict[str, object]] = []
    covered = 0.0
    for candidate, bosa, capacity in candidates[:3]:
        slots_left = 3 - len(selected)
        remaining = max(required - covered, 0.0)
        target_amount = remaining / slots_left if slots_left else remaining
        amount = min(capacity, max(target_amount, required * 0.20))
        if amount <= 0:
            continue
        covered += amount
        selected.append(
            {
                "guarantee_id": guarantee_ids.next("GUA"),
                "loan_id": loan["loan_id"],
                "borrower_member_id": borrower["member_id"],
                "guarantor_member_id": candidate["member_id"],
                "guarantee_amount_kes": round(amount, 2),
                "guarantee_pct": round(amount / principal, 4),
                "pledge_date": loan["approval_date"],
                "release_date": None,
                "guarantor_deposit_balance_at_pledge_kes": round(float(bosa["current_balance_kes"]), 2),
                "relationship_type": _relationship_type(rng),
                "guarantor_capacity_remaining_kes": round(capacity - amount, 2),
            }
        )
        if covered >= required:
            break
    return selected


def _first(accounts: list[dict[str, object]], account_types: set[str]) -> dict[str, object] | None:
    for account in accounts:
        if str(account["account_type"]) in account_types:
            return account
    return None


def _relationship_type(rng: random.Random) -> str:
    return rng.choices(
        ["COWORKER", "FAMILY", "FRIEND", "SACCO_MEMBER", "CHURCH_MEMBER", "BUSINESS_ASSOCIATE"],
        weights=[0.22, 0.18, 0.20, 0.24, 0.06, 0.10],
        k=1,
    )[0]
