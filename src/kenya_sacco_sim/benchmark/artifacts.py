from __future__ import annotations

import hashlib
from collections import Counter, defaultdict

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.validation.labels import _reference_leakage_metrics, _txn_id_leakage_metrics
from kenya_sacco_sim.validation.schema import REQUIRED_COLUMNS


def build_benchmark_artifacts(rows_by_file: dict[str, list[dict[str, object]]], rule_results: dict[str, object], config: WorldConfig) -> dict[str, object]:
    split_manifest = _build_split_manifest(rows_by_file, config)
    baseline_results = _build_baseline_results(rows_by_file, rule_results, split_manifest)
    feature_docs = _build_feature_documentation()
    return {
        "split_manifest.json": split_manifest,
        "baseline_model_results.json": baseline_results,
        "feature_documentation.json": feature_docs,
        "dataset_card.md": _dataset_card(split_manifest, baseline_results),
        "known_limitations.md": _known_limitations(),
    }


def _build_split_manifest(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> dict[str, object]:
    member_split = {str(member["member_id"]): _split_for_key(str(member["member_id"]), config.seed) for member in rows_by_file.get("members.csv", [])}
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
    checks = _split_checks(rows_by_file, member_split, pattern_split, pattern_member_splits, transaction_splits, alert_splits)
    return {
        "split_strategy": "member_hash_70_15_15",
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


def _build_baseline_results(rows_by_file: dict[str, list[dict[str, object]]], rule_results: dict[str, object], split_manifest: dict[str, object]) -> dict[str, object]:
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
        "baseline_name": "deterministic_v0_1_rules",
        "description": "Rule baseline using exported v0.1 structuring and rapid-pass-through definitions.",
        "per_typology": per_typology,
        "macro_precision": round(sum(precision_values) / len(precision_values), 4) if precision_values else 0,
        "macro_recall": round(sum(recall_values) / len(recall_values), 4) if recall_values else 0,
        "benchmark_checks": {
            **split_manifest["checks"],
            "txn_id_leakage": _txn_id_leakage_metrics(rows_by_file, suspicious_txn_ids),
            "reference_leakage": _reference_leakage_metrics(rows_by_file),
        },
    }


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


def _dataset_card(split_manifest: dict[str, object], baseline_results: dict[str, object]) -> str:
    return f"""# KENYA_SACCO_SIM v0.2 Dataset Card

## Purpose

Synthetic Kenyan SACCO AML benchmark data for deterministic rule testing and early transaction-monitoring experiments.

## Scope

The benchmark contains normal SACCO activity, support entity metadata, device baselines, loan lifecycle behavior, guarantor relationships, and labeled suspicious typologies: `STRUCTURING`, `RAPID_PASS_THROUGH`, and `FAKE_AFFORDABILITY_BEFORE_LOAN` when v0.2 typologies are enabled.

## Splits

Splits are assigned by deterministic member hash using a 70/15/15 train/validation/test allocation. Pattern labels are assigned to the same split as their labeled member, and `split_manifest.json` reports member and pattern leakage checks that must pass before using the release for benchmarking.

```text
members: {split_manifest["counts"]["members"]}
transactions: {split_manifest["counts"]["transactions"]}
patterns: {split_manifest["counts"]["patterns"]}
```

## Baseline

The included baseline is a deterministic rule baseline, not an ML model. Macro precision is `{baseline_results["macro_precision"]}` and macro recall is `{baseline_results["macro_recall"]}` for the latest generated artifact.

## Leakage Controls

Feature files exclude explicit suspicious labels. The validator checks transaction-ID threshold leakage and reference mirroring, and benchmark artifacts report those metrics.
"""


def _known_limitations() -> str:
    return """# Known Limitations

- v0.2 includes `STRUCTURING`, `RAPID_PASS_THROUGH`, and `FAKE_AFFORDABILITY_BEFORE_LOAN` suspicious typologies.
- Guarantor fraud rings, wallet funneling, dormant reactivation abuse, remittance layering, and church/charity misuse are deferred to v1.
- Device identifiers are populated for normal digital activity, but device-sharing typologies are deferred to v1.
- Baseline results are deterministic rule results, not trained machine-learning model scores.
- The benchmark is calibrated for 10,000 members and should be re-audited before scaling materially beyond that.
"""


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
    }


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
