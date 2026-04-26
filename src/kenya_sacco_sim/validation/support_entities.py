from __future__ import annotations

from collections import Counter, defaultdict

from kenya_sacco_sim.core.models import ValidationFinding


DIGITAL_CHANNELS = {"MOBILE_APP", "USSD", "PAYBILL", "TILL", "BANK_TRANSFER"}


def validate_support_entities(rows_by_file: dict[str, list[dict[str, object]]]) -> tuple[list[ValidationFinding], dict[str, object], dict[str, object], dict[str, object]]:
    findings: list[ValidationFinding] = []
    institutions = rows_by_file.get("institutions.csv", [])
    branches = rows_by_file.get("branches.csv", [])
    agents = rows_by_file.get("agents.csv", [])
    employers = rows_by_file.get("employers.csv", [])
    devices = rows_by_file.get("devices.csv", [])

    if not any([institutions, branches, agents, employers, devices]):
        return [], {"status": "not_applicable"}, {"status": "not_applicable"}, {"status": "not_applicable"}

    institution_ids = {str(row["institution_id"]) for row in institutions}
    branch_ids = {str(row["branch_id"]) for row in branches}
    member_ids = {str(row["member_id"]) for row in rows_by_file.get("members.csv", [])}
    device_ids = {str(row["device_id"]) for row in devices}
    node_entity_types: dict[str, set[str]] = defaultdict(set)
    node_id_by_entity: dict[str, str] = {}
    for node in rows_by_file.get("nodes.csv", []):
        node_entity_types[str(node["entity_id"])].add(str(node["node_type"]))
        node_id_by_entity[str(node["entity_id"])] = str(node["node_id"])

    for branch in branches:
        if str(branch["institution_id"]) not in institution_ids:
            findings.append(_error("support.branch_institution_missing", "branch institution_id must resolve to institutions.csv", "branches.csv", str(branch["branch_id"])))
    for agent in agents:
        if str(agent["institution_id"]) not in institution_ids:
            findings.append(_error("support.agent_institution_missing", "agent institution_id must resolve to institutions.csv", "agents.csv", str(agent["agent_id"])))
        if str(agent["branch_id"]) not in branch_ids:
            findings.append(_error("support.agent_branch_missing", "agent branch_id must resolve to branches.csv", "agents.csv", str(agent["agent_id"])))
    for employer in employers:
        if str(employer["institution_id"]) not in institution_ids:
            findings.append(_error("support.employer_institution_missing", "employer institution_id must resolve to institutions.csv", "employers.csv", str(employer["employer_id"])))
    for device in devices:
        if str(device["institution_id"]) not in institution_ids:
            findings.append(_error("support.device_institution_missing", "device institution_id must resolve to institutions.csv", "devices.csv", str(device["device_id"])))
        if str(device["member_id"]) not in member_ids:
            findings.append(_error("support.device_member_missing", "device member_id must resolve to members.csv", "devices.csv", str(device["device_id"])))
        if "DEVICE" not in node_entity_types.get(str(device["device_id"]), set()):
            findings.append(_error("support.device_node_missing", "device_id must resolve to DEVICE node", "devices.csv", str(device["device_id"])))

    device_findings, device_section = _device_metrics(rows_by_file, device_ids, node_id_by_entity)
    findings.extend(device_findings)
    return findings, _support_section(findings, institutions, branches, agents, employers, devices), device_section, _institution_metrics(institutions)


def _device_metrics(rows_by_file: dict[str, list[dict[str, object]]], device_ids: set[str], node_id_by_entity: dict[str, str]) -> tuple[list[ValidationFinding], dict[str, object]]:
    findings: list[ValidationFinding] = []
    transactions = rows_by_file.get("transactions.csv", [])
    digital_txns = [row for row in transactions if row.get("channel") in DIGITAL_CHANNELS]
    device_required_txns = digital_txns
    device_exempt_txns: list[dict[str, object]] = []
    digital_with_device = [row for row in digital_txns if row.get("device_id")]
    required_missing_device_rows = [row for row in device_required_txns if not row.get("device_id")]
    unresolved_device_rows = [row for row in digital_with_device if str(row["device_id"]) not in device_ids]
    unresolved_device_ids = sorted({str(row["device_id"]) for row in unresolved_device_rows})
    for txn in required_missing_device_rows[:20]:
        findings.append(_error("device.transaction_device_id_required", "digital transaction channel requires device_id", "transactions.csv", str(txn.get("txn_id") or "")))
    for device_id in unresolved_device_ids[:20]:
        findings.append(_error("device.transaction_device_missing", "transaction device_id must resolve to devices.csv", "transactions.csv", device_id))

    uses_device_edges = {str(edge["dst_node_id"]) for edge in rows_by_file.get("graph_edges.csv", []) if edge["edge_type"] == "USES_DEVICE"}
    devices_without_edges = sorted(device_id for device_id in device_ids if node_id_by_entity.get(device_id) not in uses_device_edges)
    for device_id in devices_without_edges[:20]:
        findings.append(_error("device.edge_missing", "DEVICE node must have a USES_DEVICE edge", "devices.csv", device_id))

    device_owner = {str(row["device_id"]): str(row["member_id"]) for row in rows_by_file.get("devices.csv", [])}
    device_group = {str(row["device_id"]): str(row.get("shared_device_group") or "") for row in rows_by_file.get("devices.csv", [])}
    groups_by_member: dict[str, set[str]] = defaultdict(set)
    for device in rows_by_file.get("devices.csv", []):
        group = str(device.get("shared_device_group") or "")
        if group:
            groups_by_member[str(device["member_id"])].add(group)
    transaction_users_by_device: dict[str, set[str]] = defaultdict(set)
    for txn in digital_with_device:
        transaction_users_by_device[str(txn["device_id"])].add(str(txn.get("member_id_primary") or ""))
    users_by_device = {device_id: {user for user in users if user} for device_id, users in transaction_users_by_device.items()}
    shared_devices = {
        device_id
        for device_id, users in users_by_device.items()
        if len(users) > 1 or (device_id in device_owner and any(user != device_owner[device_id] for user in users))
    }
    shared_device_group_missing = sorted(device_id for device_id in shared_devices if not device_group.get(device_id))
    unexplained_shared_usage: list[str] = []
    for device_id in shared_devices:
        group = device_group.get(device_id, "")
        owner = device_owner.get(device_id, "")
        for member_id in users_by_device.get(device_id, set()):
            if member_id == owner:
                continue
            if not group or group not in groups_by_member.get(member_id, set()):
                unexplained_shared_usage.append(f"{device_id}:{member_id}")
    for device_id in shared_device_group_missing[:20]:
        findings.append(_error("device.shared_group_missing", "device used by multiple members must have shared_device_group", "devices.csv", device_id))
    for usage in unexplained_shared_usage[:20]:
        findings.append(_error("device.shared_usage_unexplained", "shared device transaction member must be represented in the same shared_device_group", "transactions.csv", usage))
    active_members = {str(row.get("member_id_primary") or "") for row in digital_with_device if row.get("member_id_primary")}
    shared_members = {member_id for device_id in shared_devices for member_id in users_by_device.get(device_id, set()) if member_id}
    device_coverage = len(digital_with_device) / len(digital_txns) if digital_txns else 1.0
    shared_member_share = len(shared_members) / len(active_members) if active_members else 0.0
    max_members_per_device = max((len(users) for users in users_by_device.values()), default=0)
    if digital_txns and device_coverage <= 0:
        findings.append(_error("device.coverage_zero", "digital transaction device coverage must be greater than zero", "transactions.csv"))
    if shared_member_share > 0.10:
        findings.append(ValidationFinding("warning", "device.shared_member_share_high", f"shared_device_member_share {shared_member_share:.3f} is above 0.10 baseline review threshold", "devices.csv"))
    return findings, {
        "digital_transaction_count": len(digital_txns),
        "digital_transaction_device_count": len(digital_with_device),
        "digital_device_coverage": round(device_coverage, 4),
        "device_required_transaction_count": len(device_required_txns),
        "device_required_missing_device_id_count": len(required_missing_device_rows),
        "device_exempt_transaction_count": len(device_exempt_txns),
        "device_exempt_txn_type_counts": dict(sorted(Counter(str(row.get("txn_type") or "") for row in device_exempt_txns).items())),
        "device_count": len(device_ids),
        "shared_device_count": len(shared_devices),
        "devices_used_by_multiple_members_count": len(shared_devices),
        "max_members_per_device": max_members_per_device,
        "shared_device_group_missing_count": len(shared_device_group_missing),
        "shared_device_unexplained_member_count": len(unexplained_shared_usage),
        "shared_device_member_share": round(shared_member_share, 4),
        "devices_without_uses_device_edge_count": len(devices_without_edges),
        "missing_transaction_device_id_count": len(required_missing_device_rows),
        "unresolved_transaction_device_id_count": len(unresolved_device_rows),
        "unresolved_transaction_device_id_distinct_count": len(unresolved_device_ids),
    }


def _support_section(findings: list[ValidationFinding], institutions: list[dict[str, object]], branches: list[dict[str, object]], agents: list[dict[str, object]], employers: list[dict[str, object]], devices: list[dict[str, object]]) -> dict[str, object]:
    return {
        "row_counts": {
            "institutions.csv": len(institutions),
            "branches.csv": len(branches),
            "agents.csv": len(agents),
            "employers.csv": len(employers),
            "devices.csv": len(devices),
        },
        "error_count": sum(1 for finding in findings if finding.severity == "error" and (finding.code.startswith("support") or finding.code.startswith("device"))),
        "warning_count": sum(1 for finding in findings if finding.severity == "warning" and (finding.code.startswith("support") or finding.code.startswith("device"))),
    }


def _institution_metrics(institutions: list[dict[str, object]]) -> dict[str, object]:
    archetype_counts = Counter(str(row["archetype"]) for row in institutions)
    if not institutions:
        return {"status": "not_applicable"}
    return {
        "institution_count": len(institutions),
        "archetype_counts": dict(sorted(archetype_counts.items())),
        "avg_digital_maturity": round(sum(float(row["digital_maturity"]) for row in institutions) / len(institutions), 4),
        "avg_cash_intensity": round(sum(float(row["cash_intensity"]) for row in institutions) / len(institutions), 4),
        "avg_loan_guarantor_intensity": round(sum(float(row["loan_guarantor_intensity"]) for row in institutions) / len(institutions), 4),
    }


def _error(code: str, message: str, filename: str | None = None, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, filename, row_id)
