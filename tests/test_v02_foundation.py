from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from kenya_sacco_sim.benchmark.artifacts import build_benchmark_artifacts
from kenya_sacco_sim.benchmark.baseline_rules import build_rule_results
from kenya_sacco_sim.core.config import load_world_config, with_cli_overrides
from kenya_sacco_sim.generators.devices import generate_devices
from kenya_sacco_sim.generators.edges import generate_edges
from kenya_sacco_sim.generators.institutions import generate_institution_world
from kenya_sacco_sim.generators.members import generate_members
from kenya_sacco_sim.generators.nodes import generate_nodes
from kenya_sacco_sim.validation.support_entities import validate_support_entities


class V02FoundationTests(unittest.TestCase):
    def test_config_loader_uses_defaults_and_cli_overrides(self) -> None:
        config = load_world_config(Path("missing-config-dir"))
        self.assertEqual(config.member_count, 10_000)
        self.assertEqual(config.institution_count, 5)
        self.assertIn("SALARIED_TEACHER", config.personas)

        overridden = with_cli_overrides(config, member_count=123, seed=99)

        self.assertEqual(overridden.member_count, 123)
        self.assertEqual(overridden.seed, 99)
        self.assertEqual(overridden.institution_count, 5)

    def test_config_loader_reads_yaml_when_present(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir)
            (path / "world.yaml").write_text("world:\n  members:\n    count: 77\n  institutions:\n    count: 3\n  seed: 11\n", encoding="utf-8")

            config = load_world_config(path)

            self.assertEqual(config.member_count, 77)
            self.assertEqual(config.institution_count, 3)
            self.assertEqual(config.seed, 11)
            self.assertEqual(config.loaded_config_files, (str(path / "world.yaml"),))

    def test_support_entity_validation_accepts_generated_devices_and_edges(self) -> None:
        config = with_cli_overrides(load_world_config(Path("missing-config-dir")), member_count=20, institution_count=3)
        world = generate_institution_world(config)
        members = generate_members(config, world)
        devices = generate_devices(config, members)
        world = replace(world, devices=devices)
        nodes = generate_nodes(world, members, [])
        edges = generate_edges(members, [], world, nodes)

        findings, support_section, device_section, institution_metrics = validate_support_entities(
            {
                "institutions.csv": world.institutions,
                "branches.csv": world.branches,
                "agents.csv": world.agents,
                "employers.csv": world.employers,
                "devices.csv": world.devices,
                "members.csv": members,
                "nodes.csv": nodes,
                "graph_edges.csv": edges,
                "transactions.csv": [],
            }
        )

        self.assertEqual([finding.code for finding in findings], [])
        self.assertEqual(support_section["error_count"], 0)
        self.assertEqual(device_section["devices_without_uses_device_edge_count"], 0)
        self.assertEqual(institution_metrics["institution_count"], 3)

    def test_device_validation_reports_required_missing_device_ids(self) -> None:
        rows = _device_validation_rows()
        rows["transactions.csv"] = [
            {
                "txn_id": "TXN000000000001",
                "member_id_primary": "MEM0000001",
                "channel": "PAYBILL",
                "txn_type": "MPESA_PAYBILL_IN",
                "device_id": None,
            }
        ]

        findings, _, device_section, _ = validate_support_entities(rows)

        self.assertIn("device.transaction_device_id_required", [finding.code for finding in findings])
        self.assertEqual(device_section["digital_transaction_count"], 1)
        self.assertEqual(device_section["device_required_transaction_count"], 1)
        self.assertEqual(device_section["device_required_missing_device_id_count"], 1)
        self.assertEqual(device_section["missing_transaction_device_id_count"], 1)
        self.assertEqual(device_section["unresolved_transaction_device_id_count"], 0)
        self.assertEqual(device_section["unresolved_transaction_device_id_distinct_count"], 0)

    def test_shared_device_validation_requires_shared_group(self) -> None:
        rows = _device_validation_rows(member_count=2)
        rows["transactions.csv"] = [
            {"txn_id": "TXN000000000001", "member_id_primary": "MEM0000001", "channel": "MOBILE_APP", "txn_type": "MPESA_WALLET_TOPUP", "device_id": "DEVICE000001"},
            {"txn_id": "TXN000000000002", "member_id_primary": "MEM0000002", "channel": "MOBILE_APP", "txn_type": "MPESA_WALLET_TOPUP", "device_id": "DEVICE000001"},
        ]

        findings, _, device_section, _ = validate_support_entities(rows)

        self.assertIn("device.shared_group_missing", [finding.code for finding in findings])
        self.assertEqual(device_section["devices_used_by_multiple_members_count"], 1)
        self.assertEqual(device_section["max_members_per_device"], 2)
        self.assertEqual(device_section["shared_device_group_missing_count"], 1)

    def test_split_manifest_reports_institution_split_drift(self) -> None:
        config = with_cli_overrides(load_world_config(Path("missing-config-dir")), seed=42)
        members = [{"member_id": "MEM0000001", "institution_id": "INST0001"}]

        artifacts = build_benchmark_artifacts({"members.csv": members, "alerts_truth.csv": []}, {}, config)
        checks = artifacts["split_manifest.json"]["checks"]

        self.assertEqual(checks["institution_split_max_share"], 1.0)
        self.assertTrue(checks["institution_split_drift_warning"])

    def test_fake_affordability_rule_reconstructs_truth_member(self) -> None:
        accounts = [_account("ACC00000001", "MEM0000001")]
        loans = [
            {
                "loan_id": "LOAN000001",
                "member_id": "MEM0000001",
                "product_code": "DEVELOPMENT_LOAN",
                "application_date": "2024-06-15",
            }
        ]
        transactions = [
            _txn("TXN000000000001", "MEM0000001", "SRC", "ACC00000001", "PESALINK_IN", 80_000, "2024-06-01T09:00:00+03:00"),
            _txn("TXN000000000002", "MEM0000001", "SRC", "ACC00000001", "MPESA_PAYBILL_IN", 70_000, "2024-06-07T09:00:00+03:00"),
        ]
        alerts = [_pattern_alert("ALT00000001", "PAT00000001", "FAKE_AFFORDABILITY_BEFORE_LOAN", "MEM0000001")]

        results = build_rule_results(transactions, accounts, alerts, loans)

        section = results["FAKE_AFFORDABILITY_BEFORE_LOAN"]
        self.assertEqual(section["true_positive_count"], 1)
        self.assertEqual(section["false_negative_count"], 0)
        self.assertEqual(section["precision"], 1.0)
        self.assertEqual(section["recall"], 1.0)


def _account(account_id: str, member_id: str) -> dict[str, object]:
    return {"account_id": account_id, "member_id": member_id, "account_owner_type": "MEMBER"}


def _device_validation_rows(member_count: int = 1) -> dict[str, list[dict[str, object]]]:
    members = [{"member_id": f"MEM{index:07d}"} for index in range(1, member_count + 1)]
    return {
        "institutions.csv": [
            {
                "institution_id": "INST0001",
                "archetype": "TEACHER_PUBLIC_SECTOR",
                "digital_maturity": 0.8,
                "cash_intensity": 0.2,
                "loan_guarantor_intensity": 0.6,
            }
        ],
        "devices.csv": [
            {
                "device_id": "DEVICE000001",
                "member_id": "MEM0000001",
                "institution_id": "INST0001",
                "shared_device_group": None,
            }
        ],
        "members.csv": members,
        "nodes.csv": [{"node_id": "NODE000001", "entity_id": "DEVICE000001", "node_type": "DEVICE"}],
        "graph_edges.csv": [{"edge_id": "EDGE000001", "src_node_id": "NODE000002", "dst_node_id": "NODE000001", "edge_type": "USES_DEVICE"}],
    }


def _txn(txn_id: str, member_id: str, account_id_dr: str, account_id_cr: str, txn_type: str, amount: float, timestamp: str) -> dict[str, object]:
    return {
        "txn_id": txn_id,
        "member_id_primary": member_id,
        "account_id_dr": account_id_dr,
        "account_id_cr": account_id_cr,
        "txn_type": txn_type,
        "amount_kes": amount,
        "timestamp": timestamp,
        "counterparty_id_hash": "CP",
    }


def _pattern_alert(alert_id: str, pattern_id: str, typology: str, member_id: str) -> dict[str, object]:
    return {"alert_id": alert_id, "pattern_id": pattern_id, "typology": typology, "entity_type": "PATTERN", "member_id": member_id}


if __name__ == "__main__":
    unittest.main()
