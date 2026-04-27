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
from kenya_sacco_sim.validation.support_entities import validate_support_entities


def build_validation_report(
    rows_by_file: dict[str, list[dict[str, object]]],
    config: WorldConfig,
    typology_runtime_metrics: dict[str, object] | None = None,
    benchmark_validation: dict[str, object] | None = None,
) -> dict[str, object]:
    findings: list[ValidationFinding] = []
    findings.extend(validate_schema(rows_by_file, config))
    findings.extend(validate_foreign_keys(rows_by_file))
    findings.extend(validate_balances(rows_by_file))
    distribution_findings, distribution_section = validate_distribution(rows_by_file, config)
    loan_findings, loan_section = validate_loans(rows_by_file)
    guarantor_findings, guarantor_section = validate_guarantors(rows_by_file)
    credit_findings, credit_section = validate_credit_distribution(rows_by_file)
    label_findings, label_section, typology_section = validate_labels(rows_by_file, config.suspicious_ratio)
    support_findings, support_section, device_section, institution_metrics = validate_support_entities(rows_by_file)
    findings.extend(distribution_findings)
    findings.extend(loan_findings)
    findings.extend(guarantor_findings)
    findings.extend(credit_findings)
    findings.extend(label_findings)
    findings.extend(support_findings)
    findings.extend(_benchmark_findings(benchmark_validation))
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
        "support_entity_validation": support_section,
        "device_validation": device_section,
        "institution_archetype_metrics": institution_metrics,
        "clean_baseline_aml_metrics": clean_baseline_metrics(rows_by_file),
        "distribution_validation": distribution_section,
        "typology_validation": typology_section,
        "typology_runtime_metrics": typology_runtime_metrics or {"status": "not_applicable"},
        "near_miss_validation": _near_miss_section(typology_runtime_metrics),
        "fake_affordability_validation": _fake_affordability_section(typology_runtime_metrics),
        "device_sharing_mule_network_validation": _device_sharing_section(typology_runtime_metrics),
        "guarantor_fraud_ring_validation": _guarantor_ring_section(typology_runtime_metrics),
        "wallet_funneling_validation": _wallet_funneling_section(typology_runtime_metrics),
        "benchmark_validation": benchmark_validation or {"status": "not_applicable"},
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


def _benchmark_findings(benchmark_validation: dict[str, object] | None) -> list[ValidationFinding]:
    if not benchmark_validation:
        return []
    findings: list[ValidationFinding] = []
    if benchmark_validation.get("no_member_id_split_leakage") is False:
        findings.append(ValidationFinding("error", "benchmark.member_split_leakage", "Benchmark member split leakage check failed", "split_manifest.json"))
    if benchmark_validation.get("no_pattern_id_split_leakage") is False:
        findings.append(ValidationFinding("error", "benchmark.pattern_split_leakage", "Benchmark pattern split leakage check failed", "split_manifest.json"))
    reference_leakage = benchmark_validation.get("reference_leakage")
    if isinstance(reference_leakage, dict) and int(reference_leakage.get("mirrored_reference_count") or 0) > 0:
        findings.append(ValidationFinding("error", "benchmark.reference_leakage", "Benchmark reference mirroring check failed", "baseline_model_results.json"))
    txn_id_leakage = benchmark_validation.get("txn_id_leakage")
    if isinstance(txn_id_leakage, dict):
        threshold_rule = txn_id_leakage.get("best_txn_id_threshold_rule")
        if isinstance(threshold_rule, dict) and float(threshold_rule.get("precision") or 0) > 0.70 and float(threshold_rule.get("recall") or 0) > 0.70:
            findings.append(ValidationFinding("error", "benchmark.txn_id_threshold_leakage", "Benchmark txn_id threshold leakage check failed", "baseline_model_results.json"))
    if float(benchmark_validation.get("institution_split_max_share") or 0) > 0.80:
        institution_id = benchmark_validation.get("institution_split_max_institution_id")
        max_split = benchmark_validation.get("institution_split_max_split")
        max_share = benchmark_validation.get("institution_split_max_share")
        findings.append(
            ValidationFinding(
                "warning",
                "benchmark.institution_split_drift",
                f"Institution {institution_id} has split {max_split} share {max_share}, above 0.80 review threshold",
                "split_manifest.json",
            )
        )
    evaluation_validity = benchmark_validation.get("evaluation_validity")
    if isinstance(evaluation_validity, dict) and evaluation_validity.get("valid_for_ml_evaluation") is False and not evaluation_validity.get("smoke_only"):
        findings.append(
            ValidationFinding(
                "error",
                "benchmark.evaluation_label_density_low",
                "Benchmark evaluation label density is below the current benchmark validity contract",
                "split_manifest.json",
            )
        )
    confounders = benchmark_validation.get("confounder_diagnostics")
    smoke_only = isinstance(evaluation_validity, dict) and bool(evaluation_validity.get("smoke_only"))
    if isinstance(confounders, dict):
        risk = confounders.get("risk_summary")
        if isinstance(risk, dict) and risk.get("temporal_confounding_review_required") and not smoke_only:
            findings.append(
                ValidationFinding(
                    "warning",
                    "benchmark.temporal_label_concentration",
                    "Suspicious labels are temporally concentrated; ML scores may reflect time-window shortcuts",
                    "benchmark_confounder_diagnostics.json",
                )
            )
        if isinstance(risk, dict) and risk.get("persona_confounding_review_required") and not smoke_only:
            findings.append(
                ValidationFinding(
                    "warning",
                    "benchmark.persona_label_concentration",
                    "Suspicious labels are concentrated by persona/static attributes; ML scores may reflect generator assignment shortcuts",
                    "benchmark_confounder_diagnostics.json",
                )
            )
    return findings


def _fake_affordability_section(rule_results: dict[str, object] | None) -> dict[str, object]:
    if not rule_results or not isinstance(rule_results.get("FAKE_AFFORDABILITY_BEFORE_LOAN"), dict):
        return {"status": "not_applicable"}
    section = rule_results["FAKE_AFFORDABILITY_BEFORE_LOAN"]
    return {
        "candidate_count": section.get("candidate_count", 0),
        "true_positive_count": section.get("true_positive_count", 0),
        "false_positive_count": section.get("false_positive_count", 0),
        "false_negative_count": section.get("false_negative_count", 0),
        "precision": section.get("precision", 0),
        "recall": section.get("recall", 0),
    }


def _near_miss_section(rule_results: dict[str, object] | None) -> dict[str, object]:
    if not rule_results or not isinstance(rule_results.get("near_miss_disclosure"), dict):
        return {"status": "not_applicable"}
    disclosure = rule_results["near_miss_disclosure"]
    families = disclosure.get("families", {})
    return {
        "status": disclosure.get("status", "available"),
        "family_count": disclosure.get("family_count", 0),
        "near_miss_member_count": disclosure.get("near_miss_member_count", 0),
        "near_miss_transaction_count": disclosure.get("near_miss_transaction_count", 0),
        "near_miss_guarantee_count": disclosure.get("near_miss_guarantee_count", 0),
        "families": families if isinstance(families, dict) else {},
    }


def _device_sharing_section(rule_results: dict[str, object] | None) -> dict[str, object]:
    if not rule_results or not isinstance(rule_results.get("DEVICE_SHARING_MULE_NETWORK"), dict):
        return {"status": "not_applicable"}
    section = rule_results["DEVICE_SHARING_MULE_NETWORK"]
    return {
        "candidate_count": section.get("candidate_count", 0),
        "candidate_member_ids": section.get("candidate_member_ids", []),
        "true_positive_count": section.get("true_positive_count", 0),
        "false_positive_count": section.get("false_positive_count", 0),
        "false_negative_count": section.get("false_negative_count", 0),
        "false_positive_member_ids": section.get("false_positive_member_ids", []),
        "false_negative_member_ids": section.get("truth_members_missed", []),
        "precision": section.get("precision", 0),
        "recall": section.get("recall", 0),
    }


def _guarantor_ring_section(rule_results: dict[str, object] | None) -> dict[str, object]:
    if not rule_results or not isinstance(rule_results.get("GUARANTOR_FRAUD_RING"), dict):
        return {"status": "not_applicable"}
    section = rule_results["GUARANTOR_FRAUD_RING"]
    return {
        "candidate_count": section.get("candidate_count", 0),
        "candidate_member_ids": section.get("candidate_member_ids", []),
        "true_positive_count": section.get("true_positive_count", 0),
        "false_positive_count": section.get("false_positive_count", 0),
        "false_negative_count": section.get("false_negative_count", 0),
        "false_positive_member_ids": section.get("false_positive_member_ids", []),
        "false_negative_member_ids": section.get("truth_members_missed", []),
        "precision": section.get("precision", 0),
        "recall": section.get("recall", 0),
    }


def _wallet_funneling_section(rule_results: dict[str, object] | None) -> dict[str, object]:
    if not rule_results or not isinstance(rule_results.get("WALLET_FUNNELING"), dict):
        return {"status": "not_applicable"}
    section = rule_results["WALLET_FUNNELING"]
    return {
        "candidate_count": section.get("candidate_count", 0),
        "candidate_member_ids": section.get("candidate_member_ids", []),
        "true_positive_count": section.get("true_positive_count", 0),
        "false_positive_count": section.get("false_positive_count", 0),
        "false_negative_count": section.get("false_negative_count", 0),
        "false_positive_member_ids": section.get("false_positive_member_ids", []),
        "false_negative_member_ids": section.get("truth_members_missed", []),
        "precision": section.get("precision", 0),
        "recall": section.get("recall", 0),
    }


def _finding_to_dict(finding: ValidationFinding) -> dict[str, object]:
    return {
        "severity": finding.severity,
        "code": finding.code,
        "message": finding.message,
        "file": finding.file,
        "row_id": finding.row_id,
    }
