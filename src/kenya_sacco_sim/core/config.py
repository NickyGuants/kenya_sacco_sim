from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import timedelta, timezone
from pathlib import Path
from typing import Any


EAT = timezone(timedelta(hours=3))


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
    config_dir: str | None = None
    loaded_config_files: tuple[str, ...] = ()
    personas: dict[str, dict[str, object]] = field(default_factory=dict)
    institutions: dict[str, object] = field(default_factory=dict)
    products: dict[str, object] = field(default_factory=dict)
    patterns: dict[str, object] = field(default_factory=dict)
    typologies: dict[str, object] = field(default_factory=dict)
    calendar: dict[str, object] = field(default_factory=dict)
    validation: dict[str, object] = field(default_factory=dict)


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

INSTITUTION_ARCHETYPES = {
    "TEACHER_PUBLIC_SECTOR": {"digital_maturity": 0.72, "cash_intensity": 0.16, "loan_guarantor_intensity": 0.75},
    "UNIFORMED_SERVICES": {"digital_maturity": 0.66, "cash_intensity": 0.22, "loan_guarantor_intensity": 0.82},
    "UTILITY_PRIVATE_SECTOR": {"digital_maturity": 0.86, "cash_intensity": 0.12, "loan_guarantor_intensity": 0.58},
    "COMMUNITY_CHURCH": {"digital_maturity": 0.55, "cash_intensity": 0.42, "loan_guarantor_intensity": 0.50},
    "FARMER_COOPERATIVE": {"digital_maturity": 0.48, "cash_intensity": 0.58, "loan_guarantor_intensity": 0.63},
    "SME_BIASHARA": {"digital_maturity": 0.80, "cash_intensity": 0.28, "loan_guarantor_intensity": 0.68},
    "DIASPORA_FACING": {"digital_maturity": 0.90, "cash_intensity": 0.14, "loan_guarantor_intensity": 0.42},
}

DEFAULT_PRODUCTS = {
    "accounts": {
        "BOSA_DEPOSIT": {"withdrawable": False, "loan_eligibility_base": True},
        "FOSA_SAVINGS": {"withdrawable": True, "transactional": True},
        "FOSA_CURRENT": {"withdrawable": True, "transactional": True},
        "SHARE_CAPITAL": {"withdrawable": False, "ownership_equity": True},
        "MPESA_WALLET": {"withdrawable": True, "external_wallet": True},
        "LOAN_ACCOUNT": {"balance_semantics": "positive_outstanding_principal"},
    }
}

DEFAULT_PATTERNS = {
    "normal": {
        "SALARY_CHECKOFF_WALLET_SPEND": {"enabled": True, "monthly_probability": 1.0},
        "SME_DAILY_RECEIPTS_MONDAY_DEPOSIT": {"enabled": True, "monthly_probability": 0.95},
        "FARMER_SEASONAL_INCOME": {"enabled": True, "harvest_months": [3, 4, 8, 9, 12]},
        "DIASPORA_SUPPORT_HOUSEHOLD": {"enabled": True, "monthly_probability": 0.65},
        "LOAN_LIFECYCLE_NORMAL": {"enabled": True},
    },
    "suspicious": {
        "STRUCTURING": {"enabled": True},
        "RAPID_PASS_THROUGH": {"enabled": True},
        "FAKE_AFFORDABILITY_BEFORE_LOAN": {"enabled": True},
        "DEVICE_SHARING_MULE_NETWORK": {"enabled": True},
    },
}

DEFAULT_TYPOLOGIES = {
    "STRUCTURING": {"candidate_personas": ["SME_OWNER", "BODA_BODA_OPERATOR", "DIASPORA_SUPPORTED"], "deposit_count_7d": [5, 12], "amount_each_kes": [70_000, 99_000], "window_days": [2, 7], "rails": ["CASH_BRANCH", "CASH_AGENT", "MPESA"]},
    "RAPID_PASS_THROUGH": {"candidate_personas": ["DIASPORA_SUPPORTED", "SME_OWNER", "CHURCH_ORG"], "inflow_amount_kes": [100_000, 750_000], "exit_ratio": [0.75, 0.98], "exit_delay_hours": [1, 48], "outflow_count": [2, 8]},
    "FAKE_AFFORDABILITY_BEFORE_LOAN": {"candidate_personas": ["SME_OWNER", "DIASPORA_SUPPORTED", "COUNTY_WORKER", "SALARIED_TEACHER"], "lookback_days": 30, "min_external_credit_share": 0.55, "min_balance_growth_kes": 50_000, "credit_count": [2, 5], "credit_amount_kes": [25_000, 150_000], "eligible_loan_products": ["DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "BIASHARA_LOAN"]},
}

DEFAULT_CALENDAR = {
    "payday_days": [24, 25, 26, 27, 28, 29, 30, 31],
    "school_fee_months": [1, 5, 8],
    "harvest_months": [3, 4, 8, 9, 12],
    "december_spend_multiplier": 1.5,
    "weekend_wallet_multiplier": 1.3,
    "monday_sme_deposit_multiplier": 1.4,
}

DEFAULT_VALIDATION = {
    "suspicious_ratio_tolerance": 0.002,
    "allow_negative_balances_for_customer_accounts": False,
    "allow_negative_balances_for_source_accounts": True,
    "max_missing_foreign_key_count": 0,
    "require_pattern_summary_for_suspicious_patterns": True,
    "forbid_label_leakage": True,
}


def start_timestamp(config: WorldConfig) -> str:
    return f"{config.start_date}T00:00:00+03:00"


def load_world_config(config_dir: Path | None = None) -> WorldConfig:
    config_dir = config_dir or Path("config")
    loaded: dict[str, dict[str, object]] = {}
    loaded_files: list[str] = []
    for name in ("world", "personas", "products", "institutions", "patterns", "typologies", "calendar", "validation"):
        path = config_dir / f"{name}.yaml"
        if path.exists():
            loaded[name] = _read_yaml(path)
            loaded_files.append(str(path))
        else:
            loaded[name] = {}

    world = loaded["world"].get("world", loaded["world"])
    personas = loaded["personas"].get("personas", loaded["personas"]) or PERSONA_CONFIG
    institutions = loaded["institutions"].get("institutions", loaded["institutions"]) or {"archetypes": INSTITUTION_ARCHETYPES}
    products = loaded["products"].get("products", loaded["products"]) or DEFAULT_PRODUCTS
    patterns = loaded["patterns"].get("patterns", loaded["patterns"]) or DEFAULT_PATTERNS
    typologies = loaded["typologies"].get("typologies", loaded["typologies"]) or DEFAULT_TYPOLOGIES
    calendar = loaded["calendar"].get("calendar", loaded["calendar"]) or DEFAULT_CALENDAR
    validation = loaded["validation"].get("validation", loaded["validation"]) or DEFAULT_VALIDATION

    return WorldConfig(
        member_count=int(world.get("members", {}).get("count", world.get("member_count", 10_000)) if isinstance(world.get("members"), dict) else world.get("members", 10_000)),
        institution_count=int(world.get("institutions", {}).get("count", world.get("institution_count", 5)) if isinstance(world.get("institutions"), dict) else world.get("institutions", 5)),
        months=int(world.get("months", 12)),
        seed=int(world.get("seed", 42)),
        suspicious_ratio=float(world.get("suspicious_member_ratio", world.get("suspicious_ratio", 0.01))),
        difficulty=str(world.get("difficulty", "medium")),
        start_date=str(world.get("start_date", "2024-01-01")),
        end_date=str(world.get("end_date", "2024-12-31")),
        currency=str(world.get("currency", "KES")),
        config_dir=str(config_dir),
        loaded_config_files=tuple(loaded_files),
        personas=_normalise_persona_config(personas),
        institutions=institutions,
        products=products,
        patterns=patterns,
        typologies=typologies,
        calendar=calendar,
        validation=validation,
    )


def with_cli_overrides(
    config: WorldConfig,
    *,
    member_count: int | None = None,
    institution_count: int | None = None,
    months: int | None = None,
    seed: int | None = None,
    suspicious_ratio: float | None = None,
    difficulty: str | None = None,
) -> WorldConfig:
    return replace(
        config,
        member_count=config.member_count if member_count is None else member_count,
        institution_count=config.institution_count if institution_count is None else institution_count,
        months=config.months if months is None else months,
        seed=config.seed if seed is None else seed,
        suspicious_ratio=config.suspicious_ratio if suspicious_ratio is None else suspicious_ratio,
        difficulty=config.difficulty if difficulty is None else difficulty,
    )


def persona_config(config: WorldConfig) -> dict[str, dict[str, object]]:
    return config.personas or PERSONA_CONFIG


def institution_archetypes(config: WorldConfig) -> dict[str, dict[str, float]]:
    raw = config.institutions.get("archetypes") if config.institutions else None
    return raw if isinstance(raw, dict) else INSTITUTION_ARCHETYPES


def _read_yaml(path: Path) -> dict[str, object]:
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data or {}
    except ModuleNotFoundError:
        return _parse_simple_yaml(path.read_text(encoding="utf-8"))


def _normalise_persona_config(raw: object) -> dict[str, dict[str, object]]:
    if not isinstance(raw, dict):
        return PERSONA_CONFIG
    normalised: dict[str, dict[str, object]] = {}
    for persona, settings in raw.items():
        if not isinstance(settings, dict):
            continue
        income = settings.get("income") or settings.get("monthly_income_kes") or PERSONA_CONFIG.get(str(persona), {}).get("income", (10_000, 50_000, 100_000))
        normalised[str(persona)] = {
            "share": float(settings.get("share", PERSONA_CONFIG.get(str(persona), {}).get("share", 0))),
            "income": tuple(income),
            "wallet": float(settings.get("wallet", settings.get("wallet_adoption_probability", PERSONA_CONFIG.get(str(persona), {}).get("wallet", 0.9)))),
            "rural": float(settings.get("rural", settings.get("rural_probability", PERSONA_CONFIG.get(str(persona), {}).get("rural", 0.5)))),
            "loan": float(settings.get("loan", settings.get("loan_probability_annual", PERSONA_CONFIG.get(str(persona), {}).get("loan", 0.2)))),
        }
    return normalised or PERSONA_CONFIG


def _parse_simple_yaml(text: str) -> dict[str, object]:
    # Small fallback parser for the repo's default YAML files. Real deployments should use PyYAML.
    root: dict[str, object] = {}
    stack: list[tuple[int, dict[str, object]]] = [(-1, root)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value.strip():
            child: dict[str, object] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value.strip())
    return root


def _parse_scalar(value: str) -> Any:
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [_parse_scalar(item.strip()) for item in inner.split(",")]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value.strip('"')
