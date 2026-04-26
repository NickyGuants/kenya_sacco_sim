from __future__ import annotations

import random

from kenya_sacco_sim.core.config import PERSONA_CONFIG, WorldConfig
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_accounts(config: WorldConfig, members: list[dict[str, object]], world: InstitutionWorld) -> list[dict[str, object]]:
    rng = random.Random(config.seed + 202)
    ids = IdFactory()
    accounts: list[dict[str, object]] = []
    branches_by_institution: dict[str, list[dict[str, object]]] = {}
    for branch in world.branches:
        branches_by_institution.setdefault(str(branch["institution_id"]), []).append(branch)

    for member in members:
        institution_id = str(member["institution_id"])
        branch = rng.choice(branches_by_institution[institution_id])
        member_id = str(member["member_id"])
        join_date = str(member["join_date"])
        persona = str(member["persona_type"])
        is_org = member["member_type"] == "ORGANIZATION"

        if not is_org:
            accounts.append(_account(ids, member_id, institution_id, "BOSA_DEPOSIT", "BOSA_STANDARD", join_date, branch["branch_id"], rng.randint(5_000, 75_000), config.currency))
            fosa_type = rng.choice(["FOSA_SAVINGS", "FOSA_CURRENT"])
            fosa_product = "FOSA_SAVINGS_STANDARD" if fosa_type == "FOSA_SAVINGS" else "FOSA_CURRENT_STANDARD"
            accounts.append(_account(ids, member_id, institution_id, fosa_type, fosa_product, join_date, branch["branch_id"], rng.randint(1_000, 35_000), config.currency))
            accounts.append(_account(ids, member_id, institution_id, "SHARE_CAPITAL", "SHARE_CAPITAL_STANDARD", join_date, branch["branch_id"], rng.randint(1_000, 20_000), config.currency))
        else:
            accounts.append(_account(ids, member_id, institution_id, "FOSA_CURRENT", "FOSA_CURRENT_STANDARD", join_date, branch["branch_id"], rng.randint(10_000, 120_000), config.currency))
            if rng.random() < 0.65:
                accounts.append(_account(ids, member_id, institution_id, "BOSA_DEPOSIT", "BOSA_STANDARD", join_date, branch["branch_id"], rng.randint(5_000, 60_000), config.currency))

        if rng.random() < PERSONA_CONFIG[persona]["wallet"]:
            accounts.append(_account(ids, member_id, institution_id, "MPESA_WALLET", "MPESA_WALLET", join_date, None, rng.randint(0, 15_000), config.currency))

    accounts.extend(_external_accounts(ids, config))
    return accounts


def _account(
    ids: IdFactory,
    member_id: str | None,
    institution_id: str | None,
    account_type: str,
    product_code: str,
    open_date: str,
    branch_id: object | None,
    balance: int,
    currency: str,
) -> dict[str, object]:
    account_id = ids.next("ACCT")
    return {
        "account_id": account_id,
        "member_id": member_id,
        "institution_id": institution_id,
        "account_owner_type": "MEMBER",
        "account_type": account_type,
        "product_code": product_code,
        "open_date": open_date,
        "status": "ACTIVE",
        "linked_wallet_id": account_id if account_type.endswith("_WALLET") else None,
        "branch_id": branch_id,
        "currency": currency,
        "opening_balance_kes": balance,
        "current_balance_kes": balance,
        "external_account_label": None,
    }


def _external_accounts(ids: IdFactory, config: WorldConfig) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for owner_type, account_type, product_code, label in [
        ("SOURCE", "SOURCE_ACCOUNT", "EXTERNAL_SOURCE", "EXT_SRC_GENERIC"),
        ("SINK", "SINK_ACCOUNT", "EXTERNAL_SINK", "EXT_SINK_GENERIC"),
    ]:
        rows.append(
            {
                "account_id": ids.next("EXT"),
                "member_id": None,
                "institution_id": None,
                "account_owner_type": owner_type,
                "account_type": account_type,
                "product_code": product_code,
                "open_date": config.start_date,
                "status": "ACTIVE",
                "linked_wallet_id": None,
                "branch_id": None,
                "currency": config.currency,
                "opening_balance_kes": 0,
                "current_balance_kes": 0,
                "external_account_label": label,
            }
        )
    return rows
