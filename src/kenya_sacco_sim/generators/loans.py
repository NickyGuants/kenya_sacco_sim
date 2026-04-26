from __future__ import annotations

import random
from collections import defaultdict
from datetime import date, timedelta

from kenya_sacco_sim.core.config import PERSONA_CONFIG, WorldConfig
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld
from kenya_sacco_sim.generators.guarantors import GUARANTEED_PRODUCTS, select_guarantors


PRODUCT_TENORS = {
    "DEVELOPMENT_LOAN": (12, 36),
    "SCHOOL_FEES_LOAN": (3, 12),
    "EMERGENCY_LOAN": (1, 12),
    "BIASHARA_LOAN": (6, 24),
    "ASSET_FINANCE": (12, 48),
    "SALARY_ADVANCE": (1, 3),
}


def generate_loans_and_guarantors(
    config: WorldConfig,
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    world: InstitutionWorld,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    rng = random.Random(config.seed + 404)
    loan_ids = IdFactory()
    next_account_id = _next_account_index(accounts)
    branches_by_institution: dict[str, list[dict[str, object]]] = defaultdict(list)
    for branch in world.branches:
        branches_by_institution[str(branch["institution_id"])].append(branch)
    member_accounts = _accounts_by_member(accounts)
    members_by_institution: dict[str, list[dict[str, object]]] = defaultdict(list)
    for member in members:
        members_by_institution[str(member["institution_id"])].append(member)

    loans: list[dict[str, object]] = []
    guarantors: list[dict[str, object]] = []
    guarantee_ids = IdFactory()

    for member in members:
        persona = str(member["persona_type"])
        if member["member_type"] != "INDIVIDUAL":
            continue
        if rng.random() > PERSONA_CONFIG[persona]["loan"]:
            continue
        fosa = _first(member_accounts[str(member["member_id"])], {"FOSA_SAVINGS", "FOSA_CURRENT"})
        bosa = _first(member_accounts[str(member["member_id"])], {"BOSA_DEPOSIT"})
        if not fosa or not bosa:
            continue

        product_code = _product_for_persona(rng, persona)
        tenor_min, tenor_max = PRODUCT_TENORS[product_code]
        tenor_months = rng.randint(tenor_min, tenor_max)
        deposit_balance = float(bosa["current_balance_kes"])
        monthly_income = float(member["declared_monthly_income_kes"])
        principal = round(min(deposit_balance * _deposit_multiple(product_code), monthly_income * rng.uniform(2.0, 7.5)), 2)
        if principal < 10_000:
            continue

        application_date = _loan_application_date(rng, persona)
        approval_date = application_date + timedelta(days=rng.randint(1, 7))
        disbursement_date = approval_date + timedelta(days=rng.randint(1, 5))
        branch = rng.choice(branches_by_institution[str(member["institution_id"])])
        next_account_id += 1
        loan_account_id = f"ACC{next_account_id:08d}"
        accounts.append(
            {
                "account_id": loan_account_id,
                "member_id": member["member_id"],
                "institution_id": member["institution_id"],
                "account_owner_type": "MEMBER",
                "account_type": "LOAN_ACCOUNT",
                "product_code": product_code,
                "open_date": disbursement_date.isoformat(),
                "status": "ACTIVE",
                "linked_wallet_id": None,
                "branch_id": branch["branch_id"],
                "currency": config.currency,
                "opening_balance_kes": 0,
                "current_balance_kes": 0,
                "external_account_label": None,
            }
        )

        arrears_roll = rng.random()
        if arrears_roll < _default_probability(persona):
            performing_status = "DEFAULTED"
            arrears_days = rng.randint(91, 180)
            default_flag = True
        elif arrears_roll < _default_probability(persona) + _arrears_probability(persona):
            performing_status = "IN_ARREARS"
            arrears_days = rng.randint(15, 75)
            default_flag = False
        else:
            performing_status = "CURRENT"
            arrears_days = 0
            default_flag = False

        loan_id = loan_ids.next("LOAN")
        loan = {
            "loan_id": loan_id,
            "member_id": member["member_id"],
            "institution_id": member["institution_id"],
            "loan_account_id": loan_account_id,
            "product_code": product_code,
            "application_date": application_date.isoformat(),
            "approval_date": approval_date.isoformat(),
            "disbursement_date": disbursement_date.isoformat(),
            "principal_kes": principal,
            "tenor_months": tenor_months,
            "interest_rate_annual": _interest_rate(product_code),
            "repayment_mode": _repayment_mode(persona),
            "disbursement_channel": "FOSA_ACCOUNT",
            "purpose_code": _purpose_for_product(product_code, persona),
            "deposit_balance_at_application_kes": round(deposit_balance, 2),
            "loan_to_deposit_multiple": round(principal / deposit_balance, 3) if deposit_balance else 0,
            "performing_status": performing_status,
            "arrears_days": arrears_days,
            "restructure_flag": False,
            "default_flag": default_flag,
        }
        loans.append(loan)

        if product_code in GUARANTEED_PRODUCTS:
            guarantors.extend(select_guarantors(rng, guarantee_ids, loan, member, members_by_institution, member_accounts))

    return loans, guarantors


def _accounts_by_member(accounts: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    by_member: dict[str, list[dict[str, object]]] = defaultdict(list)
    for account in accounts:
        if account.get("member_id"):
            by_member[str(account["member_id"])].append(account)
    return by_member


def _first(accounts: list[dict[str, object]], account_types: set[str]) -> dict[str, object] | None:
    for account in accounts:
        if str(account["account_type"]) in account_types:
            return account
    return None


def _next_account_index(accounts: list[dict[str, object]]) -> int:
    return max(int(str(account["account_id"])[3:]) for account in accounts if str(account["account_id"]).startswith("ACC"))


def _product_for_persona(rng: random.Random, persona: str) -> str:
    choices = {
        "SALARIED_TEACHER": ["DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "SALARY_ADVANCE"],
        "COUNTY_WORKER": ["DEVELOPMENT_LOAN", "EMERGENCY_LOAN", "SALARY_ADVANCE"],
        "SME_OWNER": ["BIASHARA_LOAN", "ASSET_FINANCE", "EMERGENCY_LOAN"],
        "FARMER_SEASONAL": ["DEVELOPMENT_LOAN", "EMERGENCY_LOAN", "BIASHARA_LOAN"],
        "BODA_BODA_OPERATOR": ["ASSET_FINANCE", "BIASHARA_LOAN", "EMERGENCY_LOAN"],
        "DIASPORA_SUPPORTED": ["SCHOOL_FEES_LOAN", "EMERGENCY_LOAN"],
    }
    return rng.choice(choices.get(persona, ["EMERGENCY_LOAN"]))


def _loan_application_date(rng: random.Random, persona: str) -> date:
    if persona == "SME_OWNER" and rng.random() < 0.35:
        month = 11
    elif persona == "FARMER_SEASONAL":
        month = rng.choice([2, 3, 7, 8])
    elif rng.random() < 0.25:
        month = rng.choice([1, 5, 8])
    else:
        month = rng.randint(1, 10)
    return date(2024, month, rng.randint(1, 18))


def _deposit_multiple(product_code: str) -> float:
    return {
        "DEVELOPMENT_LOAN": 3.0,
        "SCHOOL_FEES_LOAN": 2.0,
        "EMERGENCY_LOAN": 1.5,
        "BIASHARA_LOAN": 2.5,
        "ASSET_FINANCE": 3.0,
        "SALARY_ADVANCE": 1.0,
    }[product_code]


def _interest_rate(product_code: str) -> float:
    return {
        "DEVELOPMENT_LOAN": 0.12,
        "SCHOOL_FEES_LOAN": 0.10,
        "EMERGENCY_LOAN": 0.13,
        "BIASHARA_LOAN": 0.14,
        "ASSET_FINANCE": 0.15,
        "SALARY_ADVANCE": 0.10,
    }[product_code]


def _repayment_mode(persona: str) -> str:
    if persona in {"SALARIED_TEACHER", "COUNTY_WORKER"}:
        return "PAYROLL_CHECKOFF"
    if persona in {"SME_OWNER", "DIASPORA_SUPPORTED"}:
        return "MPESA_PAYBILL"
    return "CASH_BRANCH"


def _purpose_for_product(product_code: str, persona: str) -> str:
    if product_code == "SCHOOL_FEES_LOAN":
        return "SCHOOL_FEES"
    if product_code == "ASSET_FINANCE":
        return "ASSET_PURCHASE"
    if product_code == "BIASHARA_LOAN":
        return "BUSINESS_WORKING_CAPITAL"
    if persona == "FARMER_SEASONAL":
        return "AGRICULTURE_INPUTS"
    if product_code == "EMERGENCY_LOAN":
        return "MEDICAL_EMERGENCY"
    return "DEVELOPMENT_PROJECT"


def _arrears_probability(persona: str) -> float:
    return {
        "SALARIED_TEACHER": 0.04,
        "COUNTY_WORKER": 0.05,
        "SME_OWNER": 0.08,
        "FARMER_SEASONAL": 0.12,
        "BODA_BODA_OPERATOR": 0.10,
        "DIASPORA_SUPPORTED": 0.05,
    }.get(persona, 0.05)


def _default_probability(persona: str) -> float:
    return {
        "SALARIED_TEACHER": 0.01,
        "COUNTY_WORKER": 0.015,
        "SME_OWNER": 0.025,
        "FARMER_SEASONAL": 0.04,
        "BODA_BODA_OPERATOR": 0.035,
        "DIASPORA_SUPPORTED": 0.015,
    }.get(persona, 0.02)

