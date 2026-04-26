from __future__ import annotations

import random

from kenya_sacco_sim.core.config import COUNTIES, WorldConfig, institution_archetypes, start_timestamp
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_institution_world(config: WorldConfig) -> InstitutionWorld:
    rng = random.Random(config.seed)
    ids = IdFactory()
    institutions: list[dict[str, object]] = []
    branches: list[dict[str, object]] = []
    employers: list[dict[str, object]] = []
    agents: list[dict[str, object]] = []

    archetype_names = list(institution_archetypes(config))
    for index in range(config.institution_count):
        institution_id = ids.next("SACCO")
        county = COUNTIES[index % len(COUNTIES)]
        archetype = archetype_names[index % len(archetype_names)]
        archetype_settings = institution_archetypes(config)[archetype]
        institutions.append(
            {
                "institution_id": institution_id,
                "name": f"{_title_archetype(archetype)} SACCO {index + 1}",
                "archetype": archetype,
                "county": county,
                "urban_rural": "URBAN" if county in {"Nairobi", "Mombasa", "Kisumu"} else "PERI_URBAN",
                "digital_maturity": float(archetype_settings["digital_maturity"]),
                "cash_intensity": float(archetype_settings["cash_intensity"]),
                "loan_guarantor_intensity": float(archetype_settings["loan_guarantor_intensity"]),
                "created_at": start_timestamp(config),
            }
        )
        institution_branches: list[dict[str, object]] = []
        for _ in range(2):
            branch = {
                "branch_id": ids.next("BRANCH"),
                "institution_id": institution_id,
                "county": county,
                "urban_rural": rng.choice(["URBAN", "PERI_URBAN", "RURAL"]),
                "branch_type": "HQ" if not institution_branches else "BRANCH",
                "opening_date": config.start_date,
                "created_at": start_timestamp(config),
            }
            branches.append(
                branch
            )
            institution_branches.append(branch)
        for branch in institution_branches:
            for _ in range(5):
                agents.append(
                    {
                        "agent_id": ids.next("AGENT"),
                        "institution_id": institution_id,
                        "branch_id": branch["branch_id"],
                        "provider": rng.choice(["MPESA", "SACCO_CORE", "AIRTEL_MONEY"]),
                        "county": branch["county"],
                        "urban_rural": branch["urban_rural"],
                        "location_type": "AGENT_SHOP" if branch["urban_rural"] != "RURAL" else "MARKET_CENTER",
                        "active_from": config.start_date,
                        "active_to": None,
                        "created_at": start_timestamp(config),
                    }
                )
        for _ in range(12):
            employers.append(
                {
                    "employer_id": ids.next("EMPLOYER"),
                    "institution_id": institution_id,
                    "employer_type": _employer_type(archetype),
                    "sector": _sector(archetype),
                    "public_private": "PUBLIC" if archetype in {"TEACHER_PUBLIC_SECTOR", "UNIFORMED_SERVICES"} else "PRIVATE",
                    "county": county,
                    "urban_rural": rng.choice(["URBAN", "PERI_URBAN", "RURAL"]),
                    "payroll_frequency": "MONTHLY",
                    "checkoff_supported": True,
                    "created_at": start_timestamp(config),
                }
            )

    return InstitutionWorld(institutions=institutions, branches=branches, employers=employers, agents=agents, devices=[])


def _title_archetype(archetype: str) -> str:
    return archetype.replace("_", " ").title()


def _employer_type(archetype: str) -> str:
    if archetype == "TEACHER_PUBLIC_SECTOR":
        return "TEACHERS_SERVICE"
    if archetype == "UNIFORMED_SERVICES":
        return "UNIFORMED_SERVICE"
    if archetype == "UTILITY_PRIVATE_SECTOR":
        return "UTILITY_COMPANY"
    if archetype == "FARMER_COOPERATIVE":
        return "COOPERATIVE_BUYER"
    return "PRIVATE_EMPLOYER"


def _sector(archetype: str) -> str:
    return {
        "TEACHER_PUBLIC_SECTOR": "EDUCATION",
        "UNIFORMED_SERVICES": "SECURITY",
        "UTILITY_PRIVATE_SECTOR": "UTILITY",
        "COMMUNITY_CHURCH": "FAITH_BASED",
        "FARMER_COOPERATIVE": "AGRICULTURE",
        "SME_BIASHARA": "TRADE",
        "DIASPORA_FACING": "FINANCIAL_SERVICES",
    }.get(archetype, "GENERAL")
