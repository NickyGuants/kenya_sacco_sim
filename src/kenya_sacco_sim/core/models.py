from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class InstitutionWorld:
    institutions: list[dict[str, object]]
    branches: list[dict[str, object]]
    employers: list[dict[str, object]]


@dataclass(frozen=True)
class ValidationFinding:
    severity: str
    code: str
    message: str
    file: str | None = None
    row_id: str | None = None
