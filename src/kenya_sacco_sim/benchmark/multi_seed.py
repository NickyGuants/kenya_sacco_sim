from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

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


def run_multi_seed_benchmark(config: WorldConfig, seeds: list[int], output_dir: Path, write_seed_datasets: bool = False) -> dict[str, object]:
    seeds = _validate_seeds(seeds)
    output_dir.mkdir(parents=True, exist_ok=True)
    seed_results: list[dict[str, object]] = []
    for seed in seeds:
        seed_config = with_cli_overrides(config, seed=seed)
        seed_output = output_dir / f"seed_{seed}" if write_seed_datasets else None
        seed_results.append(_run_seed(seed_config, seed_output))

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


def _run_seed(config: WorldConfig, output_dir: Path | None = None) -> dict[str, object]:
    institution_world = generate_institution_world(config)
    members = generate_members(config, institution_world)
    devices = generate_devices(config, members)
    institution_world = replace(institution_world, devices=devices)
    accounts = generate_accounts(config, members, institution_world)
    loans, guarantors = generate_loans_and_guarantors(config, members, accounts, institution_world)
    transactions = generate_transactions(config, members, accounts, institution_world, loans)
    alerts_truth, rule_results = inject_typologies(config, members, accounts, transactions, institution_world, loans)
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

    return _seed_summary(config, rows_by_file, report, rule_results, benchmark_artifacts["baseline_model_results.json"])


def _seed_summary(
    config: WorldConfig,
    rows_by_file: dict[str, list[dict[str, object]]],
    report: dict[str, object],
    rule_results: dict[str, object],
    baseline_results: dict[str, object],
) -> dict[str, object]:
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
    }


def _multi_seed_result(config: WorldConfig, seeds: list[int], seed_results: list[dict[str, object]]) -> dict[str, object]:
    stability = _stability_report(seed_results)
    return {
        "benchmark_name": "KENYA_SACCO_SIM_v0_2_multi_seed",
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
        "acceptance": {
            "precision_recall_variance_within_threshold": all_precision_recall_stable,
            "threshold": STABILITY_THRESHOLD,
        },
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
        "version": "0.2.0",
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
