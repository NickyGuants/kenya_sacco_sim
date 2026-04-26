from __future__ import annotations

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.core.models import ValidationFinding
from kenya_sacco_sim.validation.balances import validate_balances
from kenya_sacco_sim.validation.clean_baseline import clean_baseline_metrics
from kenya_sacco_sim.validation.distribution import validate_distribution
from kenya_sacco_sim.validation.foreign_keys import validate_foreign_keys
from kenya_sacco_sim.validation.labels import validate_labels
from kenya_sacco_sim.validation.loan_validator import validate_credit_distribution, validate_guarantors, validate_loans
from kenya_sacco_sim.validation.schema import validate_schema


def build_validation_report(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> dict[str, object]:
    findings: list[ValidationFinding] = []
    findings.extend(validate_schema(rows_by_file, config))
    findings.extend(validate_foreign_keys(rows_by_file))
    findings.extend(validate_balances(rows_by_file))
    distribution_findings, distribution_section = validate_distribution(rows_by_file)
    loan_findings, loan_section = validate_loans(rows_by_file)
    guarantor_findings, guarantor_section = validate_guarantors(rows_by_file)
    credit_findings, credit_section = validate_credit_distribution(rows_by_file)
    label_findings, label_section, typology_section = validate_labels(rows_by_file, config.suspicious_ratio)
    findings.extend(distribution_findings)
    findings.extend(loan_findings)
    findings.extend(guarantor_findings)
    findings.extend(credit_findings)
    findings.extend(label_findings)
    has_transactions = "transactions.csv" in rows_by_file

    return {
        "schema_validation": _section(findings, "schema"),
        "row_counts": {filename: len(rows) for filename, rows in rows_by_file.items()},
        "balance_validation": _section(findings, "balance") if has_transactions else {"status": "not_applicable_milestone_1"},
        "graph_validation": _section(findings, "foreign_key"),
        "label_validation": label_section,
        "loan_validation": loan_section,
        "guarantor_validation": guarantor_section,
        "credit_distribution_validation": credit_section,
        "clean_baseline_aml_metrics": clean_baseline_metrics(rows_by_file),
        "distribution_validation": distribution_section,
        "typology_validation": typology_section,
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
