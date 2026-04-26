from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.export.csv import write_csvs, write_json
from kenya_sacco_sim.generators.accounts import generate_accounts
from kenya_sacco_sim.generators.edges import generate_edges
from kenya_sacco_sim.generators.institutions import generate_institution_world
from kenya_sacco_sim.generators.loans import generate_loans_and_guarantors
from kenya_sacco_sim.generators.members import generate_members
from kenya_sacco_sim.generators.nodes import generate_nodes
from kenya_sacco_sim.generators.transactions import generate_transactions
from kenya_sacco_sim.validation.report import build_validation_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kenya_sacco_sim")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Generate a Milestone 1 dataset")
    generate.add_argument("--members", type=int, default=10_000)
    generate.add_argument("--institutions", type=int, default=5)
    generate.add_argument("--months", type=int, default=12)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--output", type=Path, default=Path("./datasets/KENYA_SACCO_SIM_v0_1"))
    generate.add_argument("--suspicious-ratio", type=float, default=0.01)
    generate.add_argument("--difficulty", default="medium")
    generate.add_argument("--with-transactions", action="store_true", help="Emit normal-pattern transactions.csv and run balance validation")
    generate.add_argument("--with-loans", action="store_true", help="Emit loans.csv, guarantors.csv, and loan lifecycle transactions")
    return parser


def generate(args: argparse.Namespace) -> int:
    config = WorldConfig(
        member_count=args.members,
        institution_count=args.institutions,
        months=args.months,
        seed=args.seed,
        suspicious_ratio=args.suspicious_ratio,
        difficulty=args.difficulty,
    )

    institution_world = generate_institution_world(config)
    members = generate_members(config, institution_world)
    accounts = generate_accounts(config, members, institution_world)
    loans: list[dict[str, object]] | None = None
    guarantors: list[dict[str, object]] | None = None
    if args.with_loans:
        loans, guarantors = generate_loans_and_guarantors(config, members, accounts, institution_world)
    transactions = generate_transactions(config, members, accounts, institution_world, loans or []) if args.with_transactions or args.with_loans else None
    nodes = generate_nodes(institution_world, members, accounts)
    graph_edges = generate_edges(members, accounts, institution_world, nodes, guarantors or [])

    rows_by_file = {
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
    report = build_validation_report(rows_by_file, config)

    write_csvs(args.output, rows_by_file)
    write_json(args.output / "validation_report.json", report)
    write_json(args.output / "manifest.json", _manifest(config, rows_by_file, report, args.output))

    error_count = len(report["errors"])
    print(json.dumps({"output": str(args.output), "errors": error_count, "warnings": len(report["warnings"])}, indent=2))
    return 1 if error_count else 0


def _manifest(config: WorldConfig, rows_by_file: dict[str, list[dict[str, object]]], report: dict[str, object], output_dir: Path) -> dict[str, object]:
    return {
        "dataset_name": "KENYA_SACCO_SIM",
        "version": "0.1.0",
        "seed": config.seed,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "members": config.member_count,
        "institutions": config.institution_count,
        "suspicious_ratio": config.suspicious_ratio,
        "difficulty": config.difficulty,
        "files": list(rows_by_file) + ["manifest.json", "validation_report.json"],
        "file_hashes_md5": _file_hashes(output_dir, list(rows_by_file) + ["validation_report.json"]),
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
