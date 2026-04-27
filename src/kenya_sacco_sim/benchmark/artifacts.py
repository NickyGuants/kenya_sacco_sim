from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from datetime import datetime

from kenya_sacco_sim.benchmark.ml_baseline import TYPOLOGY_NAMES, build_ml_baseline_artifacts, build_ml_leakage_ablation_artifact, member_labels_by_typology
from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.validation.labels import _reference_leakage_metrics, _txn_id_leakage_metrics
from kenya_sacco_sim.validation.schema import REQUIRED_COLUMNS


SPLITS = ("train", "validation", "test")
MIN_VALID_BENCHMARK_MEMBERS = 10_000
MIN_VALID_SUSPICIOUS_MEMBERS = 100
MIN_VALID_TYPOLOGY_MEMBERS = 30
MIN_VALID_POSITIVES_PER_SPLIT = 5
MIN_VALID_PATTERNS_PER_SPLIT = 5
MIN_VALID_TXNS_PER_TYPOLOGY_SPLIT = 10
TEMPORAL_MAX_MONTH_SHARE_THRESHOLD = 0.40
TEMPORAL_MIN_WINDOW_SPAN_DAYS = 120.0
TEMPORAL_MIN_ACTIVE_MONTHS = 10


def build_benchmark_artifacts(rows_by_file: dict[str, list[dict[str, object]]], rule_results: dict[str, object], config: WorldConfig) -> dict[str, object]:
    split_manifest = _build_split_manifest(rows_by_file, config)
    confounder_diagnostics = _build_confounder_diagnostics(rows_by_file)
    baseline_results = _build_baseline_results(rows_by_file, rule_results, split_manifest, confounder_diagnostics)
    ml_results, feature_importance = build_ml_baseline_artifacts(rows_by_file, split_manifest, config)
    ml_ablation = build_ml_leakage_ablation_artifact(rows_by_file, split_manifest, config, ml_results)
    comparison = _build_rule_vs_ml_comparison(baseline_results, ml_results, ml_ablation, confounder_diagnostics)
    feature_docs = _build_feature_documentation()
    return {
        "split_manifest.json": split_manifest,
        "baseline_model_results.json": baseline_results,
        "ml_baseline_results.json": ml_results,
        "feature_importance.json": feature_importance,
        "ml_leakage_ablation.json": ml_ablation,
        "rule_vs_ml_comparison.json": comparison,
        "benchmark_confounder_diagnostics.json": confounder_diagnostics,
        "feature_documentation.json": feature_docs,
        "dataset_card.md": _dataset_card(split_manifest, baseline_results, ml_results, ml_ablation, comparison, confounder_diagnostics),
        "known_limitations.md": _known_limitations(),
    }


def _build_split_manifest(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> dict[str, object]:
    member_split = {str(member["member_id"]): _split_for_key(str(member["member_id"]), config.seed) for member in rows_by_file.get("members.csv", [])}
    labels_by_typology = member_labels_by_typology(rows_by_file.get("alerts_truth.csv", []))
    member_split = _stratified_member_split(member_split, labels_by_typology, config.seed)
    pattern_split: dict[str, str] = {}
    pattern_members: dict[str, set[str]] = defaultdict(set)
    pattern_member_splits: dict[str, set[str]] = defaultdict(set)
    for alert in rows_by_file.get("alerts_truth.csv", []):
        pattern_id = str(alert["pattern_id"])
        member_id = str(alert.get("member_id") or "")
        if member_id:
            pattern_members[pattern_id].add(member_id)
    for pattern_id, member_ids in pattern_members.items():
        split_counts = Counter(member_split.get(member_id, "unassigned") for member_id in member_ids)
        pattern_member_splits[pattern_id] = set(split_counts)
        pattern_split[pattern_id] = split_counts.most_common(1)[0][0]

    transaction_splits = [_row_split(row, member_split, config.seed) for row in rows_by_file.get("transactions.csv", [])]
    alert_splits = [pattern_split.get(str(row["pattern_id"]), member_split.get(str(row.get("member_id") or ""), "unassigned")) for row in rows_by_file.get("alerts_truth.csv", [])]
    checks = _split_checks(rows_by_file, member_split, pattern_split, pattern_member_splits, transaction_splits, alert_splits, config)
    return {
        "split_strategy": "label_stratified_member_hash_70_15_15",
        "seed": config.seed,
        "splits": {"train": 0.70, "validation": 0.15, "test": 0.15},
        "member_id_to_split": member_split,
        "pattern_id_to_split": pattern_split,
        "counts": {
            "members": dict(sorted(Counter(member_split.values()).items())),
            "transactions": dict(sorted(Counter(transaction_splits).items())),
            "alerts_truth": dict(sorted(Counter(alert_splits).items())),
            "patterns": dict(sorted(Counter(pattern_split.values()).items())),
        },
        "checks": checks,
    }


def _build_baseline_results(
    rows_by_file: dict[str, list[dict[str, object]]],
    rule_results: dict[str, object],
    split_manifest: dict[str, object],
    confounder_diagnostics: dict[str, object],
) -> dict[str, object]:
    suspicious_txn_ids = {str(alert["txn_id"]) for alert in rows_by_file.get("alerts_truth.csv", []) if alert.get("txn_id")}
    per_typology: dict[str, dict[str, object]] = {}
    precision_values: list[float] = []
    recall_values: list[float] = []
    for typology, section in rule_results.items():
        if not isinstance(section, dict) or "candidate_count" not in section:
            continue
        truth_count = len(section["truth_member_ids"])
        tp = len(section["truth_members_detected"])
        fp = len(section["false_positive_member_ids"])
        fn = len(section["truth_members_missed"])
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / truth_count if truth_count else 0.0
        precision_values.append(precision)
        recall_values.append(recall)
        per_typology[typology] = {
            "truth_member_count": truth_count,
            "candidate_member_count": int(section["candidate_count"]),
            "true_positive_member_count": tp,
            "false_positive_member_count": fp,
            "false_negative_member_count": fn,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "false_positive_member_ids": section["false_positive_member_ids"],
            "false_negative_member_ids": section["truth_members_missed"],
        }

    return {
        "baseline_name": "deterministic_v1_rules",
        "description": "Rule baseline using exported structuring, rapid-pass-through, fake-affordability, device-sharing mule, guarantor fraud ring, and wallet-funneling definitions.",
        "per_typology": per_typology,
        "near_miss_disclosure": rule_results.get("near_miss_disclosure", {"status": "not_available"}),
        "macro_precision": round(sum(precision_values) / len(precision_values), 4) if precision_values else 0,
        "macro_recall": round(sum(recall_values) / len(recall_values), 4) if recall_values else 0,
        "benchmark_checks": {
            **split_manifest["checks"],
            "txn_id_leakage": _txn_id_leakage_metrics(rows_by_file, suspicious_txn_ids),
            "reference_leakage": _reference_leakage_metrics(rows_by_file),
            "confounder_diagnostics": confounder_diagnostics,
        },
    }


def _build_rule_vs_ml_comparison(
    baseline_results: dict[str, object],
    ml_results: dict[str, object],
    ml_ablation: dict[str, object],
    confounder_diagnostics: dict[str, object],
) -> dict[str, object]:
    per_typology: dict[str, object] = {}
    ml_outperforms: list[dict[str, object]] = []
    rules_dominate: list[dict[str, object]] = []
    rule_metrics = baseline_results.get("per_typology", {})
    models = ml_results.get("models", {})
    if not isinstance(rule_metrics, dict) or not isinstance(models, dict):
        return {"status": "not_available", "per_typology": {}, "ml_outperforms_rules": [], "rules_dominate": []}

    for typology in TYPOLOGY_NAMES:
        rule = rule_metrics.get(typology, {})
        if not isinstance(rule, dict):
            continue
        rule_precision = float(rule.get("precision") or 0.0)
        rule_recall = float(rule.get("recall") or 0.0)
        rule_f1 = _f1(rule_precision, rule_recall)
        comparisons: dict[str, object] = {}
        for model_name, typologies in sorted(models.items()):
            if not isinstance(typologies, dict):
                continue
            model_result = typologies.get(typology, {})
            if not isinstance(model_result, dict) or model_result.get("status") != "trained":
                comparisons[model_name] = {"status": model_result.get("status", "skipped") if isinstance(model_result, dict) else "skipped"}
                continue
            split_comparisons = {}
            for split_name, split in dict(model_result.get("splits", {})).items():
                if not isinstance(split, dict) or split.get("status") != "evaluated":
                    split_comparisons[split_name] = {"status": split.get("status", "skipped") if isinstance(split, dict) else "skipped"}
                    continue
                ml_precision = float(split.get("precision") or 0.0)
                ml_recall = float(split.get("recall") or 0.0)
                ml_f1 = float(split.get("f1") or 0.0)
                row = {
                    "status": "evaluated",
                    "rule_precision": round(rule_precision, 4),
                    "ml_precision": round(ml_precision, 4),
                    "precision_delta": round(ml_precision - rule_precision, 4),
                    "rule_recall": round(rule_recall, 4),
                    "ml_recall": round(ml_recall, 4),
                    "recall_delta": round(ml_recall - rule_recall, 4),
                    "rule_f1": round(rule_f1, 4),
                    "ml_f1": round(ml_f1, 4),
                    "f1_delta": round(ml_f1 - rule_f1, 4),
                    "positive_count": split.get("positive_count", 0),
                }
                split_comparisons[split_name] = row
                summary = {"typology": typology, "model": model_name, "split": split_name, "f1_delta": row["f1_delta"]}
                if ml_f1 > rule_f1:
                    ml_outperforms.append(summary)
                elif rule_f1 > ml_f1:
                    rules_dominate.append(summary)
            comparisons[model_name] = split_comparisons
        per_typology[typology] = {
            "rule": {"precision": round(rule_precision, 4), "recall": round(rule_recall, 4), "f1": round(rule_f1, 4)},
            "models": comparisons,
        }
    return {
        "status": "available",
        "claim_status": "descriptive_not_ml_superiority_evidence",
        "comparison_basis": "deterministic rule metrics compared with full-feature member-level ML split metrics",
        "interpretation": (
            "This artifact is descriptive. Full-feature ML scores may benefit from rule-proxy, "
            "temporal, and persona/static-attribute signals. Use ml_leakage_ablation.json and "
            "benchmark_confounder_diagnostics.json before making ML-vs-rule claims."
        ),
        "ablation_risk_summary": ml_ablation.get("risk_summary", {}),
        "confounder_risk_summary": confounder_diagnostics.get("risk_summary", {}),
        "per_typology": per_typology,
        "ml_f1_greater_than_rule_cases": sorted(ml_outperforms, key=lambda row: float(row["f1_delta"]), reverse=True),
        "rule_f1_greater_than_ml_cases": sorted(rules_dominate, key=lambda row: float(row["f1_delta"])),
        "ml_outperforms_rules": sorted(ml_outperforms, key=lambda row: float(row["f1_delta"]), reverse=True),
        "rules_dominate": sorted(rules_dominate, key=lambda row: float(row["f1_delta"])),
    }


def _build_confounder_diagnostics(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    temporal = _temporal_label_concentration(rows_by_file)
    persona = _persona_label_concentration(rows_by_file)
    temporal_review = bool(temporal.get("review_required"))
    persona_review = bool(persona.get("review_required"))
    return {
        "status": "available",
        "purpose": "Surface non-obvious benchmark shortcuts not caught by identifier leakage checks.",
        "temporal_label_concentration": temporal,
        "persona_label_concentration": persona,
        "risk_summary": {
            "temporal_confounding_review_required": temporal_review,
            "persona_confounding_review_required": persona_review,
            "ml_claim_status": "use_ablation_and_confounder_diagnostics_before_claiming_ml_outperformance",
            "review_required": temporal_review or persona_review,
        },
    }


def _temporal_label_concentration(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    txn_timestamp = {
        str(txn.get("txn_id")): _parse_timestamp(str(txn.get("timestamp") or ""))
        for txn in rows_by_file.get("transactions.csv", [])
        if txn.get("txn_id")
    }
    suspicious_by_typology: dict[str, list[datetime]] = {typology: [] for typology in TYPOLOGY_NAMES}
    all_suspicious: list[datetime] = []
    for alert in rows_by_file.get("alerts_truth.csv", []):
        if not alert.get("txn_id"):
            continue
        typology = str(alert.get("typology") or "")
        timestamp = txn_timestamp.get(str(alert.get("txn_id")))
        if typology not in suspicious_by_typology or timestamp is None:
            continue
        suspicious_by_typology[typology].append(timestamp)
        all_suspicious.append(timestamp)

    per_typology = {typology: _temporal_distribution(timestamps) for typology, timestamps in suspicious_by_typology.items()}
    flagged = [
        typology
        for typology, metrics in per_typology.items()
        if int(metrics.get("suspicious_transaction_count") or 0) >= 10
        and (
            float(metrics.get("max_month_share") or 0.0) > TEMPORAL_MAX_MONTH_SHARE_THRESHOLD
            or float(metrics.get("window_span_days") or 0.0) < TEMPORAL_MIN_WINDOW_SPAN_DAYS
            or int(metrics.get("active_month_count") or 0) < TEMPORAL_MIN_ACTIVE_MONTHS
        )
    ]
    return {
        "review_required": bool(flagged),
        "review_rule": (
            "Flag typologies with >=10 suspicious transactions and "
            f"max_month_share > {TEMPORAL_MAX_MONTH_SHARE_THRESHOLD:.2f} or "
            f"window_span_days < {TEMPORAL_MIN_WINDOW_SPAN_DAYS:.0f} or "
            f"active_month_count < {TEMPORAL_MIN_ACTIVE_MONTHS}."
        ),
        "flagged_typologies": flagged,
        "overall": _temporal_distribution(all_suspicious),
        "per_typology": per_typology,
    }


def _temporal_distribution(timestamps: list[datetime]) -> dict[str, object]:
    timestamps = sorted(timestamps)
    if not timestamps:
        return {
            "suspicious_transaction_count": 0,
            "month_counts": {},
            "active_month_count": 0,
            "max_month_share": 0.0,
            "start_timestamp": None,
            "end_timestamp": None,
            "window_span_days": 0.0,
        }
    month_counts = Counter(timestamp.strftime("%Y-%m") for timestamp in timestamps)
    total = len(timestamps)
    span_days = (timestamps[-1] - timestamps[0]).total_seconds() / 86_400.0
    return {
        "suspicious_transaction_count": total,
        "month_counts": dict(sorted(month_counts.items())),
        "active_month_count": len(month_counts),
        "max_month_share": round(max(month_counts.values()) / total, 4),
        "start_timestamp": timestamps[0].isoformat(timespec="seconds"),
        "end_timestamp": timestamps[-1].isoformat(timespec="seconds"),
        "window_span_days": round(span_days, 2),
    }


def _persona_label_concentration(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    persona_by_member = {str(member.get("member_id")): str(member.get("persona_type") or "UNKNOWN") for member in rows_by_file.get("members.csv", [])}
    population_counts = Counter(persona_by_member.values())
    total_members = sum(population_counts.values())
    labels_by_typology = member_labels_by_typology(rows_by_file.get("alerts_truth.csv", []))
    suspicious_members = sorted({member_id for members in labels_by_typology.values() for member_id in members})
    suspicious_counts = Counter(persona_by_member.get(member_id, "UNKNOWN") for member_id in suspicious_members)
    per_typology: dict[str, object] = {}
    max_typology_persona_share = 0.0
    max_enrichment = 0.0
    for typology in TYPOLOGY_NAMES:
        members = labels_by_typology.get(typology, set())
        counts = Counter(persona_by_member.get(member_id, "UNKNOWN") for member_id in members)
        total = sum(counts.values())
        typology_rows: dict[str, object] = {}
        for persona, population_count in sorted(population_counts.items()):
            typology_count = counts.get(persona, 0)
            population_share = population_count / total_members if total_members else 0.0
            typology_share = typology_count / total if total else 0.0
            enrichment = typology_share / population_share if population_share else 0.0
            if population_share >= 0.05 and typology_share >= 0.25:
                max_enrichment = max(max_enrichment, enrichment)
            typology_rows[persona] = {
                "population_count": population_count,
                "typology_count": typology_count,
                "population_share": round(population_share, 4),
                "typology_share": round(typology_share, 4),
                "enrichment": round(enrichment, 4),
            }
        typology_max_share = max((float(row["typology_share"]) for row in typology_rows.values()), default=0.0)
        max_typology_persona_share = max(max_typology_persona_share, typology_max_share)
        per_typology[typology] = {
            "positive_member_count": total,
            "max_persona_share": round(typology_max_share, 4),
            "personas": typology_rows,
        }

    zero_suspicious_personas = [
        persona
        for persona, population_count in sorted(population_counts.items())
        if population_count / total_members >= 0.05 and suspicious_counts.get(persona, 0) == 0
    ]
    review_required = bool(zero_suspicious_personas) or max_typology_persona_share > 0.60 or max_enrichment > 3.0
    return {
        "review_required": review_required,
        "review_rule": "Flag if any >=5% population persona has zero suspicious members, any typology has >60% in one persona, or a persona with >=5% population share and >=25% typology share exceeds 3x enrichment.",
        "population_counts": dict(sorted(population_counts.items())),
        "suspicious_counts": dict(sorted(suspicious_counts.items())),
        "zero_suspicious_personas_over_5pct_population": zero_suspicious_personas,
        "max_typology_persona_share": round(max_typology_persona_share, 4),
        "max_enrichment": round(max_enrichment, 4),
        "per_typology": per_typology,
    }


def _parse_timestamp(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _build_feature_documentation() -> dict[str, object]:
    return {
        "files": {
            filename: {
                "columns": columns,
                "file_role": "label" if filename == "alerts_truth.csv" else "feature",
                "label_file": filename == "alerts_truth.csv",
                "split_key": _split_key_for_file(filename),
            }
            for filename, columns in REQUIRED_COLUMNS.items()
        },
        "feature_files": {
            filename: {"columns": columns, "file_role": "feature", "split_key": _split_key_for_file(filename)}
            for filename, columns in REQUIRED_COLUMNS.items()
            if filename != "alerts_truth.csv"
        },
        "label_files": {
            "alerts_truth.csv": {
                "columns": REQUIRED_COLUMNS["alerts_truth.csv"],
                "file_role": "label",
                "label_file": True,
                "split_key": _split_key_for_file("alerts_truth.csv"),
                "purpose": "Ground-truth suspicious pattern labels. Do not use as model features.",
            }
        },
        "blocked_feature_columns": {
            "transactions.csv": ["is_suspicious", "typology", "pattern_id", "alert_id", "source_is_illicit", "synthetic_flag"],
            "members.csv": ["criminal_flag", "shell_flag", "suspicious_member", "injected_typology"],
            "accounts.csv": ["mule_account_flag", "laundering_account_flag"],
            "ml_feature_table": ["member_id", "txn_id", "reference", "pattern_id", "alert_id", "account_id", "device_id", "node_id", "edge_id", "typology", "label"],
        },
        "derived_ml_features": {
            "temporal": ["max_txns_24h", "max_txns_7d", "max_inflow_7d_kes", "max_outflow_7d_kes", "max_48h_exit_ratio", "max_wallet_fan_in_value_7d_kes"],
            "graph": ["graph_degree", "account_degree", "guarantor_out_degree", "guarantor_in_degree", "distinct_counterparty_count", "device_peer_member_count"],
            "device": ["transaction_device_count", "shared_device_txn_share", "max_members_per_used_device", "device_network_value_kes"],
            "behavioral": ["persona_txn_count_ratio", "persona_inflow_ratio", "external_credit_share_before_loan", "balance_growth_30d_before_loan_kes", "max_wallet_funnel_exit_ratio_7d"],
        },
        "ml_leakage_ablation": {
            "artifact": "ml_leakage_ablation.json",
            "purpose": "Retrain baselines without typology-specific rule-proxy features and report validation/test F1 drops.",
        },
        "recommended_split_source": "split_manifest.json",
        "recommended_split_entity": "member",
        "recommended_split_key_by_file": {
            "institutions.csv": "institution_id",
            "branches.csv": "institution_id",
            "agents.csv": "branch_id",
            "employers.csv": "institution_id",
            "devices.csv": "member_id",
            "members.csv": "member_id",
            "accounts.csv": "member_id",
            "transactions.csv": "member_id_primary",
            "loans.csv": "member_id",
            "guarantors.csv": ["borrower_member_id", "guarantor_member_id"],
            "alerts_truth.csv": "pattern_id",
        },
    }


def _dataset_card(
    split_manifest: dict[str, object],
    baseline_results: dict[str, object],
    ml_results: dict[str, object],
    ml_ablation: dict[str, object],
    comparison: dict[str, object],
    confounder_diagnostics: dict[str, object],
) -> str:
    rule_summary = _rule_performance_summary(baseline_results)
    near_miss_summary = _near_miss_summary(baseline_results.get("near_miss_disclosure", {}))
    ml_summary = _ml_performance_summary(ml_results)
    ablation_summary = _ablation_summary(ml_ablation)
    comparison_summary = _comparison_summary(comparison)
    confounder_summary = _confounder_summary(confounder_diagnostics)
    validity = split_manifest.get("checks", {}).get("evaluation_validity", {})
    return f"""# KENYA_SACCO_SIM v1 Dataset Card

## Intended Use

Synthetic Kenyan SACCO AML benchmark data for deterministic rule testing, member-level typology detection, leakage testing, and early transaction-monitoring model experiments.

## Not Intended Use

Do not use this dataset for real customer risk decisions, regulatory filings, production model claims, or institution-specific calibration without independent validation against real operational data.

## Scope

The benchmark contains normal SACCO activity, support entity metadata, device baselines, loan lifecycle behavior, guarantor relationships, and labeled suspicious typologies: `STRUCTURING`, `RAPID_PASS_THROUGH`, `FAKE_AFFORDABILITY_BEFORE_LOAN`, `DEVICE_SHARING_MULE_NETWORK`, `GUARANTOR_FRAUD_RING`, and `WALLET_FUNNELING` when v1 typologies are enabled.

## Benchmark Task

The primary benchmark task is member-level one-vs-rest typology detection. Labels are read only from `alerts_truth.csv`; feature construction uses exported feature files and excludes raw identifiers, references, pattern IDs, alert IDs, and typology fields.

## Splits

Splits are assigned by deterministic member hash using a 70/15/15 train/validation/test allocation. Pattern labels are assigned to the same split as their labeled member, and `split_manifest.json` reports member and pattern leakage checks that must pass before using the release for benchmarking.

```text
members: {split_manifest["counts"]["members"]}
transactions: {split_manifest["counts"]["transactions"]}
patterns: {split_manifest["counts"]["patterns"]}
evaluation status: {validity.get("status", "not_reported")}
```

## Baseline

The deterministic rule baseline macro precision is `{baseline_results["macro_precision"]}` and macro recall is `{baseline_results["macro_recall"]}` for the latest generated artifact.

### Deterministic Rule Performance

{rule_summary}

### Near-Miss And Negative-Control Coverage

{near_miss_summary}

### ML Baseline Performance

The ML baseline is `{ml_results.get("baseline_name", "not_available")}`. It trains member-level LogisticRegression and RandomForestClassifier one-vs-rest models per typology when train labels are sufficient; split-level label scarcity is reported explicitly.

{ml_summary}

### Rule-Proxy Dependence

{ablation_summary}

### Rule vs ML Comparison

{comparison_summary}

### Confounder Diagnostics

{confounder_summary}

## Leakage Controls

Feature files exclude explicit suspicious labels. The validator checks transaction-ID threshold leakage and reference mirroring, and benchmark artifacts report those metrics. `benchmark_confounder_diagnostics.json` also reports temporal and persona/static-attribute concentration risks that can make ML scores look stronger than they are.

## Seed Stability

Single generated packages are one-seed artifacts. Multi-seed stability is reported by the benchmark harness in `multi_seed_results.json`.

## Known Biases and Failure Modes

`FAKE_AFFORDABILITY_BEFORE_LOAN` intentionally has low rule precision because normal borrowers can receive legitimate large external inflows before loan applications. Small samples can distort positive-label availability and model metrics.

## Minimum Valid Sample Size

100-member runs are smoke tests only. Use 10,000-member runs for benchmark evaluation and multi-seed stability claims.
"""


def _rule_performance_summary(baseline_results: dict[str, object]) -> str:
    per_typology = baseline_results.get("per_typology", {})
    if not isinstance(per_typology, dict) or not per_typology:
        return "No deterministic rule metrics were emitted for this package."

    lines = ["```text"]
    for typology, metrics in sorted(per_typology.items()):
        if not isinstance(metrics, dict):
            continue
        lines.append(
            f"{typology}: precision {_metric(metrics.get('precision'))} / "
            f"recall {_metric(metrics.get('recall'))} / "
            f"truth {metrics.get('truth_member_count', 0)} / "
            f"candidates {metrics.get('candidate_member_count', 0)}"
        )
    lines.append("```")
    return "\n".join(lines)


def _near_miss_summary(disclosure: object) -> str:
    if not isinstance(disclosure, dict) or disclosure.get("status") != "available":
        return "Near-miss disclosure was not emitted for this package."
    families = disclosure.get("families", {})
    if not isinstance(families, dict) or not families:
        return "No near-miss families were emitted for this package."
    lines = [
        "```text",
        f"near_miss_member_count: {disclosure.get('near_miss_member_count', 0)}",
        f"near_miss_transaction_count: {disclosure.get('near_miss_transaction_count', 0)}",
        f"near_miss_guarantee_count: {disclosure.get('near_miss_guarantee_count', 0)}",
    ]
    for family, section in sorted(families.items()):
        if not isinstance(section, dict):
            continue
        lines.append(
            f"{family}: target {section.get('target_typology')} / "
            f"effect {section.get('expected_rule_effect')} / "
            f"members {section.get('member_count', 0)} / "
            f"txns {section.get('transaction_count', 0)} / "
            f"guarantees {section.get('guarantee_count', 0)}"
        )
    lines.append("```")
    return "\n".join(lines)


def _ml_performance_summary(ml_results: dict[str, object]) -> str:
    models = ml_results.get("models", {})
    if not isinstance(models, dict) or not models:
        return "No ML metrics were emitted for this package."

    lines = ["```text"]
    for model_name, typologies in sorted(models.items()):
        if not isinstance(typologies, dict):
            continue
        for typology, result in sorted(typologies.items()):
            if not isinstance(result, dict):
                continue
            if result.get("status") != "trained":
                lines.append(f"{model_name} {typology}: {result.get('status', 'skipped')}")
                continue
            splits = result.get("splits", {})
            if not isinstance(splits, dict):
                continue
            for split_name in ("train", "validation", "test"):
                split = splits.get(split_name)
                if not isinstance(split, dict):
                    continue
                if split.get("status") == "evaluated":
                    lines.append(
                        f"{model_name} {typology} {split_name}: "
                        f"precision {_metric(split.get('precision'))} / "
                        f"recall {_metric(split.get('recall'))} / "
                        f"f1 {_metric(split.get('f1'))} / "
                        f"positives {split.get('positive_count', 0)}"
                    )
                else:
                    lines.append(
                        f"{model_name} {typology} {split_name}: "
                        f"{split.get('status', 'skipped')} / positives {split.get('positive_count', 0)}"
                    )
    lines.append("```")
    return "\n".join(lines)


def _comparison_summary(comparison: dict[str, object]) -> str:
    if comparison.get("status") != "available":
        return "Rule-vs-ML comparison was not emitted for this package."
    ml_wins = comparison.get("ml_f1_greater_than_rule_cases", comparison.get("ml_outperforms_rules", []))
    rule_wins = comparison.get("rule_f1_greater_than_ml_cases", comparison.get("rules_dominate", []))
    return (
        "```text\n"
        "claim_status: descriptive_not_ml_superiority_evidence\n"
        f"ml_f1_greater_than_rule_cases: {len(ml_wins) if isinstance(ml_wins, list) else 0}\n"
        f"rule_f1_greater_than_ml_cases: {len(rule_wins) if isinstance(rule_wins, list) else 0}\n"
        "Interpret split-level ML scores with the ablation and confounder diagnostics.\n"
        "```"
    )


def _ablation_summary(ml_ablation: dict[str, object]) -> str:
    if not isinstance(ml_ablation, dict) or ml_ablation.get("baseline_name") is None:
        return "Rule-proxy ablation was not emitted for this package."
    risk = ml_ablation.get("risk_summary", {})
    notable = []
    if isinstance(risk, dict):
        notable = risk.get("high_dependency_cases", risk.get("notable_f1_drops", []))
    rows = [
        row
        for row in notable
        if isinstance(row, dict)
        and row.get("split") in {"validation", "test"}
        and isinstance(row.get("f1_drop"), (int, float))
    ]
    rows = sorted(rows, key=lambda row: float(row["f1_drop"]), reverse=True)[:6]
    lines = [
        "```text",
        "interpretation: Full-feature ML scores can reflect rule-proxy features; use ablated scores before claiming ML advantage.",
        f"high_rule_proxy_dependency: {bool(risk.get('high_rule_proxy_dependency')) if isinstance(risk, dict) else False}",
        f"max_validation_or_test_f1_drop: {_metric(risk.get('max_validation_or_test_f1_drop')) if isinstance(risk, dict) else 'n/a'}",
        "largest_validation_test_drops:",
    ]
    if rows:
        for row in rows:
            lines.append(
                f"- {row.get('model')} {row.get('typology')} {row.get('split')}: "
                f"full_f1 {_metric(row.get('full_f1'))} -> "
                f"ablated_f1 {_metric(row.get('ablated_f1'))} "
                f"(drop {_metric(row.get('f1_drop'))})"
            )
    else:
        lines.append("- none")
    lines.append("```")
    return "\n".join(lines)


def _confounder_summary(confounder_diagnostics: dict[str, object]) -> str:
    risk = confounder_diagnostics.get("risk_summary", {})
    temporal = confounder_diagnostics.get("temporal_label_concentration", {})
    persona = confounder_diagnostics.get("persona_label_concentration", {})
    flagged_temporal = temporal.get("flagged_typologies", []) if isinstance(temporal, dict) else []
    zero_personas = persona.get("zero_suspicious_personas_over_5pct_population", []) if isinstance(persona, dict) else []
    return (
        "```text\n"
        f"review_required: {bool(risk.get('review_required'))}\n"
        f"temporal_review_required: {bool(risk.get('temporal_confounding_review_required'))}\n"
        f"persona_review_required: {bool(risk.get('persona_confounding_review_required'))}\n"
        f"temporal_flagged_typologies: {flagged_temporal}\n"
        f"zero_suspicious_personas_over_5pct_population: {zero_personas}\n"
        f"max_typology_persona_share: {persona.get('max_typology_persona_share') if isinstance(persona, dict) else 'n/a'}\n"
        "```"
    )


def _metric(value: object) -> str:
    return f"{float(value):.4f}" if isinstance(value, (int, float)) else "n/a"


def _known_limitations() -> str:
    return """# Known Limitations

- v1 includes `STRUCTURING`, `RAPID_PASS_THROUGH`, `FAKE_AFFORDABILITY_BEFORE_LOAN`, `DEVICE_SHARING_MULE_NETWORK`, `GUARANTOR_FRAUD_RING`, and `WALLET_FUNNELING` suspicious typologies.
- `FAKE_AFFORDABILITY_BEFORE_LOAN` is intentionally ambiguous: normal borrowers can have large pre-loan external inflows, so the deterministic baseline is expected to have low precision and non-zero false positives.
- `WALLET_FUNNELING` includes legitimate chama, welfare, church, and project collection near-misses, so deterministic wallet fan-in/fan-out rules can produce false positives even when the ledger behavior is legitimate.
- Dormant reactivation abuse, remittance layering, and church/charity misuse remain deferred.
- Device-sharing mule networks are implemented as the first v1 typology, but full device session tables and device-sharing mule subtypes remain deferred.
- `baseline_model_results.json` contains deterministic rule results; `ml_baseline_results.json` contains trained member-level ML baseline scores.
- `ml_leakage_ablation.json` tests rule-proxy dependence, but it is still an internal benchmark diagnostic rather than proof of model validity.
- `rule_vs_ml_comparison.json` is descriptive and should not be read as proof that ML outperforms rules.
- `benchmark_confounder_diagnostics.json` reports temporal and persona/static-attribute concentration risks that can inflate ML metrics without explicit label leakage.
- The benchmark is calibrated for 10,000 members and should be re-audited before scaling materially beyond that.
"""


def _stratified_member_split(member_split: dict[str, str], labels_by_typology: dict[str, set[str]], seed: int) -> dict[str, str]:
    adjusted = dict(member_split)
    for typology in TYPOLOGY_NAMES:
        members = sorted(
            (member_id for member_id in labels_by_typology.get(typology, set()) if member_id in adjusted),
            key=lambda member_id: hashlib.sha256(f"{seed}:{typology}:{member_id}".encode("utf-8")).hexdigest(),
        )
        desired_counts = _desired_label_split_counts(len(members))
        target_splits = [
            *["train"] * desired_counts["train"],
            *["validation"] * desired_counts["validation"],
            *["test"] * desired_counts["test"],
        ]
        for member_id, split in zip(members, target_splits):
            adjusted[member_id] = split
    return adjusted


def _desired_label_split_counts(total: int) -> dict[str, int]:
    if total <= 0:
        return {"train": 0, "validation": 0, "test": 0}
    if total >= MIN_VALID_TYPOLOGY_MEMBERS:
        validation = MIN_VALID_POSITIVES_PER_SPLIT
        test = MIN_VALID_POSITIVES_PER_SPLIT
        return {"train": total - validation - test, "validation": validation, "test": test}
    test = 1 if total >= 3 else 0
    validation = 1 if total >= 2 else 0
    return {"train": total - validation - test, "validation": validation, "test": test}


def _row_split(row: dict[str, object], member_split: dict[str, str], seed: int) -> str:
    member_id = str(row.get("member_id_primary") or "")
    if member_id in member_split:
        return member_split[member_id]
    return _split_for_key(str(row.get("txn_id") or ""), seed)


def _split_checks(
    rows_by_file: dict[str, list[dict[str, object]]],
    member_split: dict[str, str],
    pattern_split: dict[str, str],
    pattern_member_splits: dict[str, set[str]],
    transaction_splits: list[str],
    alert_splits: list[str],
    config: WorldConfig,
) -> dict[str, object]:
    member_observed_splits: dict[str, set[str]] = defaultdict(set)
    unknown_member_ids: set[str] = set()
    for row, split in zip(rows_by_file.get("transactions.csv", []), transaction_splits):
        member_id = str(row.get("member_id_primary") or "")
        if not member_id:
            continue
        if member_id not in member_split:
            unknown_member_ids.add(member_id)
            continue
        member_observed_splits[member_id].add(split)
    for row, split in zip(rows_by_file.get("alerts_truth.csv", []), alert_splits):
        member_id = str(row.get("member_id") or "")
        if not member_id:
            continue
        if member_id not in member_split:
            unknown_member_ids.add(member_id)
            continue
        member_observed_splits[member_id].add(split)

    member_leaks = sorted(member_id for member_id, splits in member_observed_splits.items() if len(splits) > 1 or next(iter(splits)) != member_split[member_id])
    pattern_leaks = sorted(
        pattern_id
        for pattern_id, splits in pattern_member_splits.items()
        if not splits or "unassigned" in splits or len(splits) > 1 or pattern_split.get(pattern_id) == "unassigned"
    )
    unknown_pattern_ids = sorted(pattern_id for pattern_id, split in pattern_split.items() if split == "unassigned")
    institution_split_drift = _institution_split_drift_metrics(rows_by_file, member_split)
    evaluation_validity = _evaluation_validity_metrics(rows_by_file, member_split, pattern_split, config)
    return {
        "no_member_id_split_leakage": not member_leaks and not unknown_member_ids,
        "no_pattern_id_split_leakage": not pattern_leaks and not unknown_pattern_ids,
        "member_id_split_leakage_count": len(member_leaks),
        "pattern_id_split_leakage_count": len(pattern_leaks),
        "unassigned_member_reference_count": len(unknown_member_ids),
        "unassigned_pattern_count": len(unknown_pattern_ids),
        "member_ids_with_split_leakage_sample": member_leaks[:20],
        "pattern_ids_with_split_leakage_sample": pattern_leaks[:20],
        "unassigned_member_id_sample": sorted(unknown_member_ids)[:20],
        "unassigned_pattern_id_sample": unknown_pattern_ids[:20],
        "evaluation_validity": evaluation_validity,
        **institution_split_drift,
    }


def _institution_split_drift_metrics(rows_by_file: dict[str, list[dict[str, object]]], member_split: dict[str, str]) -> dict[str, object]:
    counts_by_institution: dict[str, Counter[str]] = defaultdict(Counter)
    for member in rows_by_file.get("members.csv", []):
        institution_id = str(member.get("institution_id") or "")
        split = member_split.get(str(member.get("member_id") or ""), "unassigned")
        if institution_id:
            counts_by_institution[institution_id][split] += 1

    institution_split_distribution: dict[str, dict[str, object]] = {}
    max_share = 0.0
    max_institution_id = None
    max_split = None
    for institution_id, counts in sorted(counts_by_institution.items()):
        total = sum(counts.values())
        institution_max_split, institution_max_count = counts.most_common(1)[0] if counts else ("unassigned", 0)
        institution_max_share = institution_max_count / total if total else 0.0
        if institution_max_share > max_share:
            max_share = institution_max_share
            max_institution_id = institution_id
            max_split = institution_max_split
        institution_split_distribution[institution_id] = {
            "counts": dict(sorted(counts.items())),
            "max_split": institution_max_split,
            "max_split_share": round(institution_max_share, 4),
        }

    return {
        "institution_split_distribution": institution_split_distribution,
        "institution_split_max_share": round(max_share, 4),
        "institution_split_max_institution_id": max_institution_id,
        "institution_split_max_split": max_split,
        "institution_split_drift_warning": max_share > 0.80,
    }


def _evaluation_validity_metrics(
    rows_by_file: dict[str, list[dict[str, object]]],
    member_split: dict[str, str],
    pattern_split: dict[str, str],
    config: WorldConfig,
) -> dict[str, object]:
    labels_by_typology = member_labels_by_typology(rows_by_file.get("alerts_truth.csv", []))
    suspicious_members = sorted({member_id for members in labels_by_typology.values() for member_id in members})
    positive_counts: dict[str, dict[str, int]] = {}
    pattern_counts: dict[str, dict[str, int]] = {}
    txn_counts: dict[str, dict[str, int]] = {}

    for typology in TYPOLOGY_NAMES:
        counts = Counter(member_split.get(member_id, "unassigned") for member_id in labels_by_typology.get(typology, set()))
        positive_counts[typology] = {split: int(counts.get(split, 0)) for split in SPLITS}
        positive_counts[typology]["total"] = int(sum(counts.values()))

    pattern_ids_by_typology_split: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    txn_counts_by_typology_split: dict[str, Counter[str]] = defaultdict(Counter)
    for alert in rows_by_file.get("alerts_truth.csv", []):
        typology = str(alert.get("typology") or "")
        if typology not in TYPOLOGY_NAMES:
            continue
        pattern_id = str(alert.get("pattern_id") or "")
        split = pattern_split.get(pattern_id, member_split.get(str(alert.get("member_id") or ""), "unassigned"))
        if pattern_id:
            pattern_ids_by_typology_split[typology][split].add(pattern_id)
        if alert.get("txn_id"):
            txn_counts_by_typology_split[typology][split] += 1

    for typology in TYPOLOGY_NAMES:
        pattern_counts[typology] = {split: len(pattern_ids_by_typology_split[typology].get(split, set())) for split in SPLITS}
        pattern_counts[typology]["total"] = len({pattern_id for split_ids in pattern_ids_by_typology_split[typology].values() for pattern_id in split_ids})
        txn_counts[typology] = {split: int(txn_counts_by_typology_split[typology].get(split, 0)) for split in SPLITS}
        txn_counts[typology]["total"] = int(sum(txn_counts_by_typology_split[typology].values()))

    active_typologies = [typology for typology in TYPOLOGY_NAMES if positive_counts.get(typology, {}).get("total", 0) > 0]
    min_positive = _min_metric(positive_counts, active_typologies)
    min_patterns = _min_metric(pattern_counts, active_typologies)
    min_txns = _min_metric(txn_counts, active_typologies)
    typology_member_minimum_met = all(positive_counts[typology]["total"] >= MIN_VALID_TYPOLOGY_MEMBERS for typology in active_typologies)
    member_count = len(rows_by_file.get("members.csv", []))
    smoke_only = member_count < MIN_VALID_BENCHMARK_MEMBERS or len(suspicious_members) < MIN_VALID_SUSPICIOUS_MEMBERS
    split_minimums_met = (
        min_positive >= MIN_VALID_POSITIVES_PER_SPLIT
        and min_patterns >= MIN_VALID_PATTERNS_PER_SPLIT
        and min_txns >= MIN_VALID_TXNS_PER_TYPOLOGY_SPLIT
    )
    valid = bool(active_typologies) and not smoke_only and typology_member_minimum_met and split_minimums_met
    if valid:
        status = "valid"
    elif smoke_only:
        status = "smoke_only"
    else:
        status = "invalid"

    return {
        "status": status,
        "valid_for_ml_evaluation": valid,
        "smoke_only": smoke_only,
        "member_count": member_count,
        "suspicious_member_count": len(suspicious_members),
        "required_member_count": MIN_VALID_BENCHMARK_MEMBERS,
        "required_suspicious_member_count": MIN_VALID_SUSPICIOUS_MEMBERS,
        "required_typology_member_count": MIN_VALID_TYPOLOGY_MEMBERS,
        "required_positive_labels_per_split": MIN_VALID_POSITIVES_PER_SPLIT,
        "required_patterns_per_split": MIN_VALID_PATTERNS_PER_SPLIT,
        "required_txns_per_typology_per_split": MIN_VALID_TXNS_PER_TYPOLOGY_SPLIT,
        "typology_member_minimum_met": typology_member_minimum_met,
        "split_minimums_met": split_minimums_met,
        "min_positive_labels_per_split": min_positive,
        "min_patterns_per_split": min_patterns,
        "min_txns_per_typology_per_split": min_txns,
        "positive_member_counts_by_typology_split": positive_counts,
        "pattern_counts_by_typology_split": pattern_counts,
        "txn_label_counts_by_typology_split": txn_counts,
    }


def _min_metric(metrics: dict[str, dict[str, int]], typologies: list[str]) -> int:
    values = [int(metrics[typology].get(split, 0)) for typology in typologies for split in SPLITS]
    return min(values) if values else 0


def _split_key_for_file(filename: str) -> str | list[str] | None:
    return {
        "institutions.csv": "institution_id",
        "branches.csv": "institution_id",
        "agents.csv": "branch_id",
        "employers.csv": "institution_id",
        "devices.csv": "member_id",
        "members.csv": "member_id",
        "accounts.csv": "member_id",
        "transactions.csv": "member_id_primary",
        "loans.csv": "member_id",
        "guarantors.csv": ["borrower_member_id", "guarantor_member_id"],
        "alerts_truth.csv": "pattern_id",
    }.get(filename)


def _split_for_key(key: str, seed: int) -> str:
    bucket = int(hashlib.sha256(f"{seed}:{key}".encode("utf-8")).hexdigest()[:8], 16) % 10_000
    if bucket < 7_000:
        return "train"
    if bucket < 8_500:
        return "validation"
    return "test"


def _f1(precision: float, recall: float) -> float:
    return 0.0 if precision + recall <= 0 else 2 * precision * recall / (precision + recall)
