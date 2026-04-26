from __future__ import annotations

import random

from kenya_sacco_sim.core.config import COUNTIES, WorldConfig, start_timestamp
from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_institution_world(config: WorldConfig) -> InstitutionWorld:
    rng = random.Random(config.seed)
    ids = IdFactory()
    institutions: list[dict[str, object]] = []
    branches: list[dict[str, object]] = []
    employers: list[dict[str, object]] = []

    for index in range(config.institution_count):
        institution_id = ids.next("SACCO")
        county = COUNTIES[index % len(COUNTIES)]
        institutions.append(
            {
                "institution_id": institution_id,
                "county": county,
                "urban_rural": "URBAN" if county in {"Nairobi", "Mombasa", "Kisumu"} else "PERI_URBAN",
                "created_at": start_timestamp(config),
            }
        )
        for _ in range(2):
            branches.append(
                {
                    "branch_id": ids.next("BRANCH"),
                    "institution_id": institution_id,
                    "county": county,
                    "urban_rural": rng.choice(["URBAN", "PERI_URBAN", "RURAL"]),
                    "created_at": start_timestamp(config),
                }
            )
        for _ in range(12):
            employers.append(
                {
                    "employer_id": ids.next("EMPLOYER"),
                    "institution_id": institution_id,
                    "county": county,
                    "urban_rural": rng.choice(["URBAN", "PERI_URBAN", "RURAL"]),
                    "created_at": start_timestamp(config),
                }
            )

    return InstitutionWorld(institutions=institutions, branches=branches, employers=employers)
