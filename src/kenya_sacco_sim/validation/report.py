from __future__ import annotations

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.core.models import ValidationFinding
from kenya_sacco_sim.validation.balances import validate_balances
from kenya_sacco_sim.validation.distribution import validate_distribution
from kenya_sacco_sim.validation.foreign_keys import validate_foreign_keys
from kenya_sacco_sim.validation.schema import validate_schema


def build_validation_report(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> dict[str, object]:
    findings: list[ValidationFinding] = []
    findings.extend(validate_schema(rows_by_file, config))
    findings.extend(validate_foreign_keys(rows_by_file))
    findings.extend(validate_balances(rows_by_file))
    distribution_findings, distribution_section = validate_distribution(rows_by_file)
    findings.extend(distribution_findings)
    has_transactions = "transactions.csv" in rows_by_file

    return {
        "schema_validation": _section(findings, "schema"),
        "row_counts": {filename: len(rows) for filename, rows in rows_by_file.items()},
        "balance_validation": _section(findings, "balance") if has_transactions else {"status": "not_applicable_milestone_1"},
        "graph_validation": _section(findings, "foreign_key"),
        "label_validation": {"status": "not_applicable_milestone_1"},
        "distribution_validation": distribution_section,
        "typology_validation": {"status": "not_applicable_milestone_1"},
        "errors": [_finding_to_dict(f) for f in findings if f.severity == "error"],
        "warnings": [_finding_to_dict(f) for f in findings if f.severity == "warning"],
        "info": [_finding_to_dict(f) for f in findings if f.severity == "info"],
    }


def _section(findings: list[ValidationFinding], prefix: str) -> dict[str, object]:
    relevant = [f for f in findings if f.code.startswith(prefix)]
    return {
        "error_count": sum(1 for f in relevant if f.severity == "error"),
        "warning_count": sum(1 for f in relevant if f.severity == "warning"),
    }


def _finding_to_dict(finding: ValidationFinding) -> dict[str, object]:
    return {
        "severity": finding.severity,
        "code": finding.code,
        "message": finding.message,
        "file": finding.file,
        "row_id": finding.row_id,
    }
