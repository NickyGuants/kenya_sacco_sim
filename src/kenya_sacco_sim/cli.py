from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import replace
from pathlib import Path

from kenya_sacco_sim.benchmark import build_benchmark_artifacts
from kenya_sacco_sim.core.config import WorldConfig, load_world_config, start_timestamp, with_cli_overrides
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kenya_sacco_sim")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate a KENYA_SACCO_SIM dataset")
    generate.add_argument("--members", type=int, default=None)
    generate.add_argument("--institutions", type=int, default=None)
    generate.add_argument("--months", type=int, default=None)
    generate.add_argument("--seed", type=int, default=None)
    generate.add_argument("--output", type=Path, default=Path("./datasets/KENYA_SACCO_SIM_v0_2"))
    generate.add_argument("--suspicious-ratio", type=float, default=None)
    generate.add_argument("--difficulty", default=None)
    generate.add_argument("--config-dir", type=Path, default=Path("./config"))
    generate.add_argument("--with-transactions", action="store_true", help="Emit normal-pattern transactions.csv and run balance validation")
    generate.add_argument("--with-loans", action="store_true", help="Emit loans.csv, guarantors.csv, and loan lifecycle transactions")
    generate.add_argument("--with-typologies", action="store_true", help="Inject v0.1 suspicious typologies and emit alerts_truth.csv/rule_results.json; combine with --with-loans for the full credit package")
    generate.add_argument("--with-benchmark", action="store_true", help="Emit Milestone 5 split manifest, baseline results, feature docs, dataset card, and known limitations")
    return parser


def generate(args: argparse.Namespace) -> int:
    if args.with_benchmark and not args.with_typologies:
        raise SystemExit("--with-benchmark requires --with-typologies")

    config = with_cli_overrides(
        load_world_config(args.config_dir),
        member_count=args.members,
        institution_count=args.institutions,
        months=args.months,
        seed=args.seed,
        suspicious_ratio=args.suspicious_ratio,
        difficulty=args.difficulty,
    )

    institution_world = generate_institution_world(config)
    members = generate_members(config, institution_world)
    devices = generate_devices(config, members)
    institution_world = replace(institution_world, devices=devices)
    accounts = generate_accounts(config, members, institution_world)
    loans: list[dict[str, object]] | None = None
    guarantors: list[dict[str, object]] | None = None
    if args.with_loans:
        loans, guarantors = generate_loans_and_guarantors(config, members, accounts, institution_world)
    transactions = generate_transactions(config, members, accounts, institution_world, loans or []) if args.with_transactions or args.with_loans or args.with_typologies else None
    alerts_truth: list[dict[str, object]] | None = None
    rule_results: dict[str, object] | None = None
    if args.with_typologies:
        assert transactions is not None
        alerts_truth, rule_results = inject_typologies(config, members, accounts, transactions, institution_world, loans or [])
    nodes = generate_nodes(institution_world, members, accounts)
    graph_edges = generate_edges(members, accounts, institution_world, nodes, guarantors or [])

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
    }
    if transactions is not None:
        rows_by_file["transactions.csv"] = transactions
    if loans is not None:
        rows_by_file["loans.csv"] = loans
    if guarantors is not None:
        rows_by_file["guarantors.csv"] = guarantors
    if alerts_truth is not None:
        rows_by_file["alerts_truth.csv"] = alerts_truth
    benchmark_artifacts = build_benchmark_artifacts(rows_by_file, rule_results, config) if args.with_benchmark and rule_results is not None else {}
    benchmark_validation = benchmark_artifacts.get("baseline_model_results.json", {}).get("benchmark_checks") if benchmark_artifacts else None
    report = build_validation_report(rows_by_file, config, rule_results if rule_results else None, benchmark_validation)

    write_csvs(args.output, rows_by_file)
    if rule_results is not None:
        write_json(args.output / "rule_results.json", rule_results)
    for filename, artifact in benchmark_artifacts.items():
        if isinstance(artifact, dict):
            write_json(args.output / filename, artifact)
        else:
            (args.output / filename).write_text(str(artifact), encoding="utf-8")
    write_json(args.output / "validation_report.json", report)
    extra_files = (["rule_results.json"] if rule_results is not None else []) + list(benchmark_artifacts)
    write_json(args.output / "manifest.json", _manifest(config, rows_by_file, report, args.output, extra_files))

    error_count = len(report["errors"])
    print(json.dumps({"output": str(args.output), "errors": error_count, "warnings": len(report["warnings"])}, indent=2))
    return 1 if error_count else 0


def _manifest(config: WorldConfig, rows_by_file: dict[str, list[dict[str, object]]], report: dict[str, object], output_dir: Path, extra_files: list[str] | None = None) -> dict[str, object]:
    extra_files = extra_files or []
    payload_files = list(rows_by_file) + extra_files + ["manifest.json", "validation_report.json"]
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
        "config_dir": config.config_dir,
        "loaded_config_files": list(config.loaded_config_files),
        "created_at": start_timestamp(config),
        "files": payload_files,
        "file_hashes_md5": _file_hashes(output_dir, list(rows_by_file) + extra_files + ["validation_report.json"]),
        "validation": {
            "error_count": len(report["errors"]),
            "warning_count": len(report["warnings"]),
        },
    }


def _file_hashes(output_dir: Path, filenames: list[str]) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for filename in filenames:
        path = output_dir / filename
        if path.exists():
            hashes[filename] = hashlib.md5(path.read_bytes()).hexdigest()
    return hashes


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "generate":
        return generate(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2
