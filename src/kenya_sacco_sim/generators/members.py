from __future__ import annotations

import random
from datetime import date, timedelta

from kenya_sacco_sim.core.config import COUNTIES, WorldConfig, persona_config, start_timestamp
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_members(config: WorldConfig, world: InstitutionWorld) -> list[dict[str, object]]:
    rng = random.Random(config.seed + 101)
    ids = IdFactory()
    personas = list(persona_config(config))
    weights = [float(persona_config(config)[p]["share"]) for p in personas]
    members: list[dict[str, object]] = []

    for _ in range(config.member_count):
        persona = rng.choices(personas, weights=weights, k=1)[0]
        settings = persona_config(config)[persona]
        institution = _institution_for_persona(rng, persona, world.institutions)
        urban_rural = _urban_rural(rng, settings["rural"])
        member_id = ids.next("MEMBER")
        member_type = "ORGANIZATION" if persona in {"CHURCH_ORG", "CHAMA_GROUP"} else "INDIVIDUAL"
        employer_id = None
        if persona in {"SALARIED_TEACHER", "COUNTY_WORKER", "UNIFORMED_OFFICER", "PRIVATE_SECTOR_EMPLOYEE", "SACCO_STAFF"}:
            employer_id = rng.choice(world.employers)["employer_id"]
        min_income, mode_income, max_income = settings["income"]
        monthly_income = int(rng.triangular(min_income, max_income, mode_income))

        members.append(
            {
                "member_id": member_id,
                "institution_id": institution["institution_id"],
                "member_type": member_type,
                "persona_type": persona,
                "county": rng.choice(COUNTIES),
                "urban_rural": urban_rural,
                "gender": "UNKNOWN" if member_type == "ORGANIZATION" else rng.choice(["MALE", "FEMALE"]),
                "age": 0 if member_type == "ORGANIZATION" else rng.randint(21, 68),
                "occupation": _occupation(persona),
                "employer_id": employer_id,
                "join_date": _random_date(rng, date(2015, 1, 1), date.fromisoformat(config.start_date)).isoformat(),
                "kyc_level": rng.choices(["STANDARD", "ENHANCED", "SIMPLIFIED"], weights=[0.78, 0.12, 0.10], k=1)[0],
                "risk_segment": rng.choices(["LOW", "MEDIUM", "HIGH"], weights=[0.72, 0.23, 0.05], k=1)[0],
                "phone_hash": IdFactory.hash_id("PHONE", member_id),
                "id_hash": None if member_type == "ORGANIZATION" else IdFactory.hash_id("ID", member_id),
                "declared_monthly_income_kes": monthly_income,
                "income_stability_score": round(rng.uniform(0.35, 0.95), 3),
                "dormant_flag": rng.random() < 0.08,
                "created_at": start_timestamp(config),
            }
        )
    return members


def _institution_for_persona(rng: random.Random, persona: str, institutions: list[dict[str, object]]) -> dict[str, object]:
    affinity = {
        "SALARIED_TEACHER": {"TEACHER_PUBLIC_SECTOR": 5, "UNIFORMED_SERVICES": 2},
        "COUNTY_WORKER": {"TEACHER_PUBLIC_SECTOR": 2, "UNIFORMED_SERVICES": 3, "UTILITY_PRIVATE_SECTOR": 2},
        "UNIFORMED_OFFICER": {"UNIFORMED_SERVICES": 7, "TEACHER_PUBLIC_SECTOR": 1},
        "PRIVATE_SECTOR_EMPLOYEE": {"UTILITY_PRIVATE_SECTOR": 5, "SME_BIASHARA": 2, "DIASPORA_FACING": 1},
        "SME_OWNER": {"SME_BIASHARA": 5, "UTILITY_PRIVATE_SECTOR": 2, "DIASPORA_FACING": 2},
        "MICRO_TRADER": {"SME_BIASHARA": 5, "COMMUNITY_CHURCH": 2, "FARMER_COOPERATIVE": 2},
        "FARMER_SEASONAL": {"FARMER_COOPERATIVE": 6, "COMMUNITY_CHURCH": 2},
        "DIASPORA_SUPPORTED": {"DIASPORA_FACING": 6, "COMMUNITY_CHURCH": 2},
        "BODA_BODA_OPERATOR": {"SME_BIASHARA": 3, "COMMUNITY_CHURCH": 2, "FARMER_COOPERATIVE": 2},
        "CHAMA_GROUP": {"COMMUNITY_CHURCH": 4, "FARMER_COOPERATIVE": 3, "SME_BIASHARA": 2},
        "CHURCH_ORG": {"COMMUNITY_CHURCH": 7, "DIASPORA_FACING": 2},
        "SACCO_STAFF": {"TEACHER_PUBLIC_SECTOR": 2, "UNIFORMED_SERVICES": 2, "UTILITY_PRIVATE_SECTOR": 2, "SME_BIASHARA": 2},
    }
    weights = [affinity.get(persona, {}).get(str(institution.get("archetype")), 1) for institution in institutions]
    return rng.choices(institutions, weights=weights, k=1)[0]


def _urban_rural(rng: random.Random, rural_probability: float) -> str:
    if rng.random() < rural_probability:
        return "RURAL"
    return rng.choices(["URBAN", "PERI_URBAN"], weights=[0.65, 0.35], k=1)[0]


def _random_date(rng: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=rng.randint(0, (end - start).days))


def _occupation(persona: str) -> str:
    return {
        "SALARIED_TEACHER": "Teacher",
        "COUNTY_WORKER": "County worker",
        "UNIFORMED_OFFICER": "Uniformed officer",
        "PRIVATE_SECTOR_EMPLOYEE": "Private sector employee",
        "SME_OWNER": "SME owner",
        "MICRO_TRADER": "Micro trader",
        "FARMER_SEASONAL": "Farmer",
        "DIASPORA_SUPPORTED": "Household recipient",
        "BODA_BODA_OPERATOR": "Boda boda operator",
        "CHAMA_GROUP": "Chama group",
        "CHURCH_ORG": "Church organization",
        "SACCO_STAFF": "SACCO staff",
    }[persona]
