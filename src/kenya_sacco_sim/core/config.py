from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WorldConfig:
    member_count: int = 10_000
    institution_count: int = 5
    months: int = 12
    seed: int = 42
    suspicious_ratio: float = 0.01
    difficulty: str = "medium"
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"
    currency: str = "KES"


PERSONA_CONFIG = {
    "SALARIED_TEACHER": {"share": 0.22, "income": (45_000, 78_000, 120_000), "wallet": 0.95, "rural": 0.35, "loan": 0.35},
    "COUNTY_WORKER": {"share": 0.13, "income": (35_000, 65_000, 95_000), "wallet": 0.95, "rural": 0.30, "loan": 0.30},
    "SME_OWNER": {"share": 0.18, "income": (30_000, 120_000, 300_000), "wallet": 0.98, "rural": 0.25, "loan": 0.25},
    "FARMER_SEASONAL": {"share": 0.17, "income": (10_000, 35_000, 150_000), "wallet": 0.80, "rural": 0.85, "loan": 0.20},
    "DIASPORA_SUPPORTED": {"share": 0.10, "income": (15_000, 50_000, 180_000), "wallet": 0.95, "rural": 0.45, "loan": 0.15},
    "BODA_BODA_OPERATOR": {"share": 0.15, "income": (20_000, 45_000, 80_000), "wallet": 0.98, "rural": 0.40, "loan": 0.22},
    "CHURCH_ORG": {"share": 0.05, "income": (30_000, 150_000, 600_000), "wallet": 0.90, "rural": 0.50, "loan": 0.10},
}

COUNTIES = ["Nairobi", "Kiambu", "Nakuru", "Mombasa", "Kisumu", "Meru", "Nyeri", "Uasin Gishu", "Kakamega", "Machakos"]
