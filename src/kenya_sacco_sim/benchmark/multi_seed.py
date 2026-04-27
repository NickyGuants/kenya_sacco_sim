from __future__ import annotations

import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path
from statistics import mean, pstdev
from time import perf_counter
from typing import Any, Callable

from kenya_sacco_sim.benchmark import build_benchmark_artifacts
from kenya_sacco_sim.core.config import WorldConfig, start_timestamp, with_cli_overrides
from kenya_sacco_sim.export.csv import write_csvs, write_json
from kenya_sacco_sim.generators.accounts import generate_accounts
from kenya_sacco_sim.generators.devices import generate_devices
from kenya_sacco_sim.generators.edges import generate_edges
from kenya_sacco_sim.generators.institutions import generate_institution_world
from kenya_sacco_sim.generators.loans import generate_loans_and_guarantors
from kenya_sacco_sim.generators.members import generate_members
from kenya_sacco_sim.generators.nodes import generate_nodes
from kenya_sacco_sim.generators.transactions import generate_transactions
from kenya_sacco_sim.generators.typologies import inject_typologies
from kenya_sacco_sim.validation.report import build_validation_report


STABILITY_THRESHOLD = 0.10


ProgressCallback = Callable[[str], None]


def run_multi_seed_benchmark(
    config: WorldConfig,
    seeds: list[int],
    output_dir: Path,
    write_seed_datasets: bool = False,
    max_workers: int | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    seeds = _validate_seeds(seeds)
    output_dir.mkdir(parents=True, exist_ok=True)
    worker_count = _worker_count(max_workers, len(seeds))
    _emit_progress(progress, f"running {len(seeds)} seeds with {worker_count} worker(s)")
    seed_results = _run_seeds_parallel(config, seeds, output_dir, write_seed_datasets, worker_count, progress)

    result = _multi_seed_result(config, seeds, seed_results)
    write_json(output_dir / "multi_seed_results.json", result)
    return result


def _validate_seeds(seeds: list[int]) -> list[int]:
    if not seeds:
        raise ValueError("At least one seed is required for multi-seed benchmarking")
    duplicates = sorted({seed for seed in seeds if seeds.count(seed) > 1})
    if duplicates:
        duplicate_text = ", ".join(str(seed) for seed in duplicates)
        raise ValueError(f"Duplicate seeds are not allowed: {duplicate_text}")
    return list(seeds)


def _worker_count(max_workers: int | None, seed_count: int) -> int:
    if max_workers is not None and max_workers < 1:
        raise ValueError("--jobs must be at least 1")
    if seed_count <= 1:
        return 1
    if max_workers is not None:
        return min(max_workers, seed_count)
    cpu_count = os.cpu_count() or 2
    return min(seed_count, max(1, cpu_count - 1), 4)


def _run_seeds_parallel(
    config: WorldConfig,
    seeds: list[int],
    output_dir: Path,
    write_seed_datasets: bool,
    worker_count: int,
    progress: ProgressCallback | None,
) -> list[dict[str, object]]:
    if worker_count == 1:
        results_by_seed = {}
        for seed in seeds:
            _emit_progress(progress, f"seed {seed} started")
            started_at = perf_counter()
            results_by_seed[seed] = _run_seed_job(config, seed, output_dir, write_seed_datasets)
            _emit_progress(progress, f"seed {seed} finished in {perf_counter() - started_at:.1f}s")
        return [results_by_seed[seed] for seed in seeds]

    started_at_by_seed: dict[int, float] = {}
    results_by_seed: dict[int, dict[str, object]] = {}
    with ProcessPoolExecutor(max_workers=worker_count) as executor:
        futures = {}
        for seed in seeds:
            _emit_progress(progress, f"seed {seed} queued")
            future = executor.submit(_run_seed_job, config, seed, output_dir, write_seed_datasets)
            futures[future] = seed
            started_at_by_seed[seed] = perf_counter()
        for future in as_completed(futures):
            seed = futures[future]
            try:
                results_by_seed[seed] = future.result()
            except Exception as exc:
                for pending in futures:
                    pending.cancel()
                raise RuntimeError(f"Benchmark seed {seed} failed") from exc
            _emit_progress(progress, f"seed {seed} finished in {perf_counter() - started_at_by_seed[seed]:.1f}s")
    return [results_by_seed[seed] for seed in seeds]


def _run_seed_job(config: WorldConfig, seed: int, output_dir: Path, write_seed_datasets: bool) -> dict[str, object]:
    seed_config = with_cli_overrides(config, seed=seed)
    seed_output = output_dir / f"seed_{seed}" if write_seed_datasets else None
    return _run_seed(seed_config, seed_output)


def _emit_progress(progress: ProgressCallback | None, message: str) -> None:
    if progress is not None:
        progress(message)


def stderr_progress(message: str) -> None:
    print(f"[benchmark] {message}", file=sys.stderr, flush=True)


def _run_seed(config: WorldConfig, output_dir: Path | None = None) -> dict[str, object]:
    institution_world = generate_institution_world(config)
    members = generate_members(config, institution_world)
    devices = generate_devices(config, members)
    institution_world = replace(institution_world, devices=devices)
    accounts = generate_accounts(config, members, institution_world)
    loans, guarantors = generate_loans_and_guarantors(config, members, accounts, institution_world)
    transactions = generate_transactions(config, members, accounts, institution_world, loans)
    alerts_truth, rule_results = inject_typologies(config, members, accounts, transactions, institution_world, loans, guarantors)
    nodes = generate_nodes(institution_world, members, accounts)
    graph_edges = generate_edges(members, accounts, institution_world, nodes, guarantors)
    rows_by_file = {
        "institutions.csv": institution_world.institutions,
        "branches.csv": institution_world.branches,
        "agents.csv": institution_world.agents,
        "employers.csv": institution_world.employers,
        "devices.csv": institution_world.devices,
        "members.csv": members,
        "accounts.csv": accounts,
        "nodes.csv": nodes,
        "graph_edges.csv": graph_edges,
        "transactions.csv": transactions,
        "loans.csv": loans,
        "guarantors.csv": guarantors,
        "alerts_truth.csv": alerts_truth,
    }
    benchmark_artifacts = build_benchmark_artifacts(rows_by_file, rule_results, config)
    benchmark_validation = benchmark_artifacts["baseline_model_results.json"]["benchmark_checks"]
    report = build_validation_report(rows_by_file, config, rule_results, benchmark_validation)

    if output_dir is not None:
        write_csvs(output_dir, rows_by_file)
        write_json(output_dir / "rule_results.json", rule_results)
        for filename, artifact in benchmark_artifacts.items():
            if isinstance(artifact, dict):
                write_json(output_dir / filename, artifact)
            else:
                (output_dir / filename).write_text(str(artifact), encoding="utf-8")
        write_json(output_dir / "validation_report.json", report)
        write_json(output_dir / "manifest.json", _seed_manifest(config, rows_by_file, report, benchmark_artifacts))

    return _seed_summary(config, rows_by_file, report, rule_results, benchmark_artifacts)


def _seed_summary(
    config: WorldConfig,
    rows_by_file: dict[str, list[dict[str, object]]],
    report: dict[str, object],
    rule_results: dict[str, object],
    benchmark_artifacts: dict[str, object],
) -> dict[str, object]:
    baseline_results = benchmark_artifacts["baseline_model_results.json"]
    distribution = report.get("distribution_validation", {})
    device = report.get("device_validation", {})
    credit = report.get("credit_distribution_validation", {})
    typologies: dict[str, object] = {}
    for typology, section in rule_results.items():
        if not isinstance(section, dict) or "precision" not in section:
            continue
        typologies[typology] = {
            "truth_member_count": len(section.get("truth_member_ids", [])),
            "candidate_count": section.get("candidate_count", 0),
            "precision": section.get("precision", 0.0),
            "recall": section.get("recall", 0.0),
            "f1": section.get("f1", 0.0),
            "false_positive_count": section.get("false_positive_count", 0),
            "false_negative_count": section.get("false_negative_count", 0),
        }

    return {
        "seed": config.seed,
        "validation_error_count": len(report.get("errors", [])),
        "validation_warning_count": len(report.get("warnings", [])),
        "row_counts": {filename: len(rows) for filename, rows in rows_by_file.items()},
        "distribution_metrics": {
            "cash_rail_share": distribution.get("cash_rail_share"),
            "active_member_share": distribution.get("active_member_share"),
            "counterparty_id_hash_coverage": distribution.get("counterparty_id_hash_coverage"),
        },
        "device_metrics": {
            "digital_device_coverage": device.get("digital_device_coverage"),
            "shared_device_member_share": device.get("shared_device_member_share"),
            "device_required_missing_device_id_count": device.get("device_required_missing_device_id_count"),
            "max_members_per_device": device.get("max_members_per_device"),
        },
        "credit_metrics": {
            "loan_active_member_ratio": credit.get("loan_active_member_ratio"),
            "arrears_share": credit.get("arrears_share"),
            "default_share": credit.get("default_share"),
            "repayment_success_rate": credit.get("repayment_success_rate"),
        },
        "baseline_metrics": {
            "macro_precision": baseline_results.get("macro_precision"),
            "macro_recall": baseline_results.get("macro_recall"),
            "per_typology": typologies,
        },
        "near_miss_metrics": _near_miss_summary(rule_results.get("near_miss_disclosure", {})),
        "evaluation_validity": baseline_results.get("benchmark_checks", {}).get("evaluation_validity", {}),
        "confounder_diagnostics": baseline_results.get("benchmark_checks", {}).get("confounder_diagnostics", {}).get("risk_summary", {}),
        "ml_metrics": _ml_metric_summary(benchmark_artifacts.get("ml_baseline_results.json", {}), metric_key="f1"),
        "ml_ablation_metrics": _ml_metric_summary(benchmark_artifacts.get("ml_leakage_ablation.json", {}), metric_key="ablated_f1"),
    }


def _multi_seed_result(config: WorldConfig, seeds: list[int], seed_results: list[dict[str, object]]) -> dict[str, object]:
    stability = _stability_report(seed_results)
    return {
        "benchmark_name": "KENYA_SACCO_SIM_v1_multi_seed",
        "created_at": start_timestamp(config),
        "member_count": config.member_count,
        "seed_count": len(seeds),
        "seeds": seeds,
        "acceptance": {
            "validation_error_free": all(int(seed["validation_error_count"]) == 0 for seed in seed_results),
            "precision_recall_variance_within_threshold": bool(stability["acceptance"]["precision_recall_variance_within_threshold"]),
            "threshold": STABILITY_THRESHOLD,
        },
        "seed_results": seed_results,
        "stability_report": stability,
    }


def _stability_report(seed_results: list[dict[str, object]]) -> dict[str, object]:
    typologies = sorted(
        {
            typology
            for seed in seed_results
            for typology in _per_typology(seed)
        }
    )
    per_typology: dict[str, dict[str, object]] = {}
    all_precision_recall_stable = True
    for typology in typologies:
        precision_values = [float(_per_typology(seed).get(typology, {}).get("precision") or 0.0) for seed in seed_results]
        recall_values = [float(_per_typology(seed).get(typology, {}).get("recall") or 0.0) for seed in seed_results]
        precision_stats = _series_stats(precision_values)
        recall_stats = _series_stats(recall_values)
        stable = precision_stats["range"] <= STABILITY_THRESHOLD and recall_stats["range"] <= STABILITY_THRESHOLD
        all_precision_recall_stable = all_precision_recall_stable and stable
        per_typology[typology] = {
            "precision": precision_stats,
            "recall": recall_stats,
            "within_threshold": stable,
        }

    metric_stability = {
        "cash_rail_share": _seed_metric_stats(seed_results, ("distribution_metrics", "cash_rail_share")),
        "active_member_share": _seed_metric_stats(seed_results, ("distribution_metrics", "active_member_share")),
        "digital_device_coverage": _seed_metric_stats(seed_results, ("device_metrics", "digital_device_coverage")),
        "shared_device_member_share": _seed_metric_stats(seed_results, ("device_metrics", "shared_device_member_share")),
        "loan_active_member_ratio": _seed_metric_stats(seed_results, ("credit_metrics", "loan_active_member_ratio")),
        "arrears_share": _seed_metric_stats(seed_results, ("credit_metrics", "arrears_share")),
        "default_share": _seed_metric_stats(seed_results, ("credit_metrics", "default_share")),
    }
    return {
        "typology_precision_recall": per_typology,
        "distribution_stability": metric_stability,
        "ml_stability": _ml_stability_report(seed_results),
        "near_miss_stability": _near_miss_stability_report(seed_results),
        "acceptance": {
            "precision_recall_variance_within_threshold": all_precision_recall_stable,
            "threshold": STABILITY_THRESHOLD,
        },
    }


def _ml_metric_summary(artifact: object, metric_key: str) -> dict[str, object]:
    if not isinstance(artifact, dict):
        return {}
    models = artifact.get("models", {})
    if not isinstance(models, dict):
        return {}
    summary: dict[str, object] = {}
    for model_name, typologies in models.items():
        if not isinstance(typologies, dict):
            continue
        summary[str(model_name)] = {}
        for typology, section in typologies.items():
            if not isinstance(section, dict) or section.get("status") not in {"trained", "evaluated"}:
                continue
            splits = section.get("splits", {})
            if not isinstance(splits, dict):
                continue
            summary[str(model_name)][str(typology)] = {
                split: split_metrics.get(metric_key)
                for split, split_metrics in splits.items()
                if isinstance(split_metrics, dict) and split_metrics.get("status") == "evaluated"
            }
    return summary


def _near_miss_summary(disclosure: object) -> dict[str, object]:
    if not isinstance(disclosure, dict):
        return {"status": "not_available"}
    families = disclosure.get("families", {})
    family_counts = {}
    if isinstance(families, dict):
        family_counts = {
            str(family): {
                "member_count": section.get("member_count", 0),
                "transaction_count": section.get("transaction_count", 0),
                "guarantee_count": section.get("guarantee_count", 0),
                "expected_rule_effect": section.get("expected_rule_effect"),
                "target_typology": section.get("target_typology"),
            }
            for family, section in families.items()
            if isinstance(section, dict)
        }
    return {
        "status": disclosure.get("status", "not_available"),
        "near_miss_member_count": disclosure.get("near_miss_member_count", 0),
        "near_miss_transaction_count": disclosure.get("near_miss_transaction_count", 0),
        "near_miss_guarantee_count": disclosure.get("near_miss_guarantee_count", 0),
        "device_sharing_near_miss_group_count": disclosure.get("device_sharing_near_miss_group_count", 0),
        "device_sharing_near_miss_member_count": disclosure.get("device_sharing_near_miss_member_count", 0),
        "device_sharing_near_miss_transaction_count": disclosure.get("device_sharing_near_miss_transaction_count", 0),
        "family_counts": family_counts,
    }


def _near_miss_stability_report(seed_results: list[dict[str, object]]) -> dict[str, object]:
    families = sorted(
        {
            family
            for seed in seed_results
            for family in (
                seed.get("near_miss_metrics", {}).get("family_counts", {})
                if isinstance(seed.get("near_miss_metrics"), dict)
                else {}
            )
        }
    )
    return {
        "near_miss_member_count": _seed_metric_stats(seed_results, ("near_miss_metrics", "near_miss_member_count")),
        "near_miss_transaction_count": _seed_metric_stats(seed_results, ("near_miss_metrics", "near_miss_transaction_count")),
        "near_miss_guarantee_count": _seed_metric_stats(seed_results, ("near_miss_metrics", "near_miss_guarantee_count")),
        "families": {
            family: {
                "member_count": _near_miss_family_stats(seed_results, family, "member_count"),
                "transaction_count": _near_miss_family_stats(seed_results, family, "transaction_count"),
            }
            for family in families
        },
    }


def _near_miss_family_stats(seed_results: list[dict[str, object]], family: str, metric: str) -> dict[str, object]:
    values: list[float] = []
    for seed in seed_results:
        near_miss = seed.get("near_miss_metrics", {})
        if not isinstance(near_miss, dict):
            continue
        families = near_miss.get("family_counts", {})
        if not isinstance(families, dict):
            continue
        section = families.get(family, {})
        if isinstance(section, dict) and isinstance(section.get(metric), (int, float)):
            values.append(float(section[metric]))
    return _series_stats(values)


def _ml_stability_report(seed_results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "full_feature_f1": _nested_ml_stability(seed_results, "ml_metrics"),
        "ablated_feature_f1": _nested_ml_stability(seed_results, "ml_ablation_metrics"),
        "ablation_f1_drop": _ml_ablation_drop_stability(seed_results),
        "confounder_diagnostic_stability": _confounder_stability(seed_results),
        "interpretation": "Single-seed ML scores are anecdotal; use full, ablated, and confounder ranges to assess seed sensitivity.",
    }


def _nested_ml_stability(seed_results: list[dict[str, object]], key: str) -> dict[str, object]:
    bucket: dict[tuple[str, str, str], list[float]] = {}
    for seed in seed_results:
        models = seed.get(key, {})
        if not isinstance(models, dict):
            continue
        for model_name, typologies in models.items():
            if not isinstance(typologies, dict):
                continue
            for typology, splits in typologies.items():
                if not isinstance(splits, dict):
                    continue
                for split, value in splits.items():
                    if isinstance(value, (int, float)):
                        bucket.setdefault((str(model_name), str(typology), str(split)), []).append(float(value))
    rows: dict[str, object] = {}
    for (model_name, typology, split), values in sorted(bucket.items()):
        rows.setdefault(model_name, {}).setdefault(typology, {})[split] = _series_stats(values)
    return rows


def _ml_ablation_drop_stability(seed_results: list[dict[str, object]]) -> dict[str, object]:
    bucket: dict[tuple[str, str, str], list[float]] = {}
    for seed in seed_results:
        full_models = seed.get("ml_metrics", {})
        ablated_models = seed.get("ml_ablation_metrics", {})
        if not isinstance(full_models, dict) or not isinstance(ablated_models, dict):
            continue
        for model_name, typologies in full_models.items():
            if not isinstance(typologies, dict):
                continue
            ablated_typologies = ablated_models.get(model_name, {})
            if not isinstance(ablated_typologies, dict):
                continue
            for typology, splits in typologies.items():
                if not isinstance(splits, dict):
                    continue
                ablated_splits = ablated_typologies.get(typology, {})
                if not isinstance(ablated_splits, dict):
                    continue
                for split, full_value in splits.items():
                    ablated_value = ablated_splits.get(split)
                    if isinstance(full_value, (int, float)) and isinstance(ablated_value, (int, float)):
                        drop = round(max(0.0, float(full_value) - float(ablated_value)), 4)
                        bucket.setdefault((str(model_name), str(typology), str(split)), []).append(drop)

    rows: dict[str, object] = {}
    for (model_name, typology, split), values in sorted(bucket.items()):
        rows.setdefault(model_name, {}).setdefault(typology, {})[split] = _series_stats(values)
    return rows


def _confounder_stability(seed_results: list[dict[str, object]]) -> dict[str, object]:
    per_seed = []
    review_count = 0
    temporal_count = 0
    persona_count = 0
    for seed in seed_results:
        diagnostics = seed.get("confounder_diagnostics", {})
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        review = bool(diagnostics.get("review_required"))
        temporal = bool(diagnostics.get("temporal_confounding_review_required"))
        persona = bool(diagnostics.get("persona_confounding_review_required"))
        review_count += int(review)
        temporal_count += int(temporal)
        persona_count += int(persona)
        per_seed.append(
            {
                "seed": seed.get("seed"),
                "review_required": review,
                "temporal_confounding_review_required": temporal,
                "persona_confounding_review_required": persona,
            }
        )
    seed_count = len(seed_results)
    return {
        "seed_count": seed_count,
        "all_clear": review_count == 0,
        "review_required_count": review_count,
        "temporal_confounding_review_required_count": temporal_count,
        "persona_confounding_review_required_count": persona_count,
        "per_seed": per_seed,
    }


def _per_typology(seed_result: dict[str, object]) -> dict[str, Any]:
    baseline = seed_result.get("baseline_metrics", {})
    if not isinstance(baseline, dict):
        return {}
    per_typology = baseline.get("per_typology", {})
    return per_typology if isinstance(per_typology, dict) else {}


def _seed_metric_stats(seed_results: list[dict[str, object]], path: tuple[str, str]) -> dict[str, object]:
    values: list[float] = []
    for seed in seed_results:
        section = seed.get(path[0], {})
        if isinstance(section, dict) and section.get(path[1]) is not None:
            values.append(float(section[path[1]]))
    return _series_stats(values)


def _series_stats(values: list[float]) -> dict[str, object]:
    if not values:
        return {"count": 0, "mean": None, "std": None, "min": None, "max": None, "range": None}
    return {
        "count": len(values),
        "mean": round(mean(values), 4),
        "std": round(pstdev(values), 4) if len(values) > 1 else 0.0,
        "min": round(min(values), 4),
        "max": round(max(values), 4),
        "range": round(max(values) - min(values), 4),
    }


def _seed_manifest(config: WorldConfig, rows_by_file: dict[str, list[dict[str, object]]], report: dict[str, object], benchmark_artifacts: dict[str, object]) -> dict[str, object]:
    return {
        "dataset_name": "KENYA_SACCO_SIM",
        "version": "1.0.0-dev",
        "seed": config.seed,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "members": config.member_count,
        "institutions": config.institution_count,
        "suspicious_ratio": config.suspicious_ratio,
        "difficulty": config.difficulty,
        "files": list(rows_by_file) + ["rule_results.json", *benchmark_artifacts, "manifest.json", "validation_report.json"],
        "validation": {
            "error_count": len(report["errors"]),
            "warning_count": len(report["warnings"]),
        },
    }
