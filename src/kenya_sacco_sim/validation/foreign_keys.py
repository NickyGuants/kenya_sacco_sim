from __future__ import annotations

from collections import defaultdict

from kenya_sacco_sim.core.models import ValidationFinding


def validate_foreign_keys(rows_by_file: dict[str, list[dict[str, object]]]) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    members = {str(row["member_id"]) for row in rows_by_file.get("members.csv", [])}
    accounts = {str(row["account_id"]) for row in rows_by_file.get("accounts.csv", [])}
    loans = {str(row["loan_id"]) for row in rows_by_file.get("loans.csv", [])}
    nodes = rows_by_file.get("nodes.csv", [])
    node_ids = {str(row["node_id"]) for row in nodes}
    entity_to_types: dict[str, set[str]] = defaultdict(set)
    for node in nodes:
        entity_to_types[str(node["entity_id"])].add(str(node["node_type"]))

    _check_entity_type(rows_by_file.get("members.csv", []), "members.csv", "institution_id", entity_to_types, {"INSTITUTION"}, findings)
    _check_entity_type(rows_by_file.get("members.csv", []), "members.csv", "employer_id", entity_to_types, {"EMPLOYER"}, findings, nullable=True)
    _check_entity_type(rows_by_file.get("accounts.csv", []), "accounts.csv", "institution_id", entity_to_types, {"INSTITUTION"}, findings, nullable=True)
    _check_entity_type(rows_by_file.get("accounts.csv", []), "accounts.csv", "branch_id", entity_to_types, {"BRANCH"}, findings, nullable=True)

    for account in rows_by_file.get("accounts.csv", []):
        row_id = str(account["account_id"])
        member_id = account.get("member_id")
        if account["account_owner_type"] == "MEMBER":
            if not member_id or str(member_id) not in members:
                findings.append(_error("foreign_key.account_member_missing", "Member-owned account must resolve to members.csv", "accounts.csv", row_id))
        if str(account["account_id"]) not in entity_to_types:
            findings.append(_error("foreign_key.account_node_missing", "Account must resolve to nodes.entity_id", "accounts.csv", row_id))

    for member in rows_by_file.get("members.csv", []):
        if str(member["member_id"]) not in entity_to_types:
            findings.append(_error("foreign_key.member_node_missing", "Member must resolve to nodes.entity_id", "members.csv", str(member["member_id"])))

    for edge in rows_by_file.get("graph_edges.csv", []):
        row_id = str(edge["edge_id"])
        if str(edge["src_node_id"]) not in node_ids:
            findings.append(_error("foreign_key.edge_src_missing", "Edge src_node_id must resolve to nodes.node_id", "graph_edges.csv", row_id))
        if str(edge["dst_node_id"]) not in node_ids:
            findings.append(_error("foreign_key.edge_dst_missing", "Edge dst_node_id must resolve to nodes.node_id", "graph_edges.csv", row_id))

    _check_infrastructure_graph(rows_by_file, node_ids, findings)

    for txn in rows_by_file.get("transactions.csv", []):
        row_id = str(txn["txn_id"])
        for column in ("account_id_dr", "account_id_cr"):
            account_id = txn.get(column)
            if account_id and str(account_id) not in accounts:
                findings.append(_error("foreign_key.txn_account_missing", f"{column} must resolve to accounts.csv", "transactions.csv", row_id))
        member_id = txn.get("member_id_primary")
        if member_id and str(member_id) not in members:
            findings.append(_error("foreign_key.txn_member_missing", "member_id_primary must resolve to members.csv", "transactions.csv", row_id))
        _check_txn_entity(txn, "institution_id", entity_to_types, {"INSTITUTION"}, findings)
        _check_txn_entity(txn, "branch_id", entity_to_types, {"BRANCH"}, findings, nullable=True)
        _check_txn_entity(txn, "agent_id", entity_to_types, {"AGENT"}, findings, nullable=True)
        _check_txn_entity(txn, "device_id", entity_to_types, {"DEVICE"}, findings, nullable=True)
        if txn.get("rail") == "CASH_AGENT" and not txn.get("agent_id"):
            findings.append(_error("foreign_key.cash_agent_missing_agent", "CASH_AGENT transactions must have agent_id", "transactions.csv", row_id))

    for loan in rows_by_file.get("loans.csv", []):
        row_id = str(loan["loan_id"])
        if str(loan["member_id"]) not in members:
            findings.append(_error("foreign_key.loan_member_missing", "loan member_id must resolve to members.csv", "loans.csv", row_id))
        if str(loan["loan_account_id"]) not in accounts:
            findings.append(_error("foreign_key.loan_account_missing", "loan_account_id must resolve to accounts.csv", "loans.csv", row_id))
        _check_entity_type([loan], "loans.csv", "institution_id", entity_to_types, {"INSTITUTION"}, findings)

    for guarantee in rows_by_file.get("guarantors.csv", []):
        row_id = str(guarantee["guarantee_id"])
        if str(guarantee["loan_id"]) not in loans:
            findings.append(_error("foreign_key.guarantee_loan_missing", "guarantee loan_id must resolve to loans.csv", "guarantors.csv", row_id))
        for column in ("borrower_member_id", "guarantor_member_id"):
            if str(guarantee[column]) not in members:
                findings.append(_error("foreign_key.guarantee_member_missing", f"{column} must resolve to members.csv", "guarantors.csv", row_id))

    _check_required_accounts(rows_by_file.get("members.csv", []), rows_by_file.get("accounts.csv", []), findings)
    return findings


def _check_infrastructure_graph(rows_by_file: dict[str, list[dict[str, object]]], node_ids: set[str], findings: list[ValidationFinding]) -> None:
    degree: dict[str, int] = defaultdict(int)
    entity_node: dict[str, str] = {}
    node_type_by_id: dict[str, str] = {}
    for node in rows_by_file.get("nodes.csv", []):
        entity_node[str(node["entity_id"])] = str(node["node_id"])
        node_type_by_id[str(node["node_id"])] = str(node["node_type"])
    for edge in rows_by_file.get("graph_edges.csv", []):
        degree[str(edge["src_node_id"])] += 1
        degree[str(edge["dst_node_id"])] += 1
    for node in rows_by_file.get("nodes.csv", []):
        if node["node_type"] in {"INSTITUTION", "BRANCH"} and degree[str(node["node_id"])] < 1:
            findings.append(_error("foreign_key.infrastructure_node_isolated", f"{node['node_type']} node has no graph edges", "nodes.csv", str(node["node_id"])))

    account_edges = {(str(edge["src_node_id"]), str(edge["dst_node_id"]), str(edge["edge_type"])) for edge in rows_by_file.get("graph_edges.csv", [])}
    for account in rows_by_file.get("accounts.csv", []):
        account_node = entity_node.get(str(account["account_id"]))
        branch_node = entity_node.get(str(account.get("branch_id") or ""))
        institution_node = entity_node.get(str(account.get("institution_id") or ""))
        if account_node and branch_node and (account_node, branch_node, "ACCOUNT_AT_BRANCH") not in account_edges:
            findings.append(_error("foreign_key.account_branch_edge_missing", "Account branch_id must have ACCOUNT_AT_BRANCH graph edge", "accounts.csv", str(account["account_id"])))
        if account_node and institution_node and (account_node, institution_node, "ACCOUNT_BELONGS_TO_INSTITUTION") not in account_edges:
            findings.append(_error("foreign_key.account_institution_edge_missing", "Account institution_id must have ACCOUNT_BELONGS_TO_INSTITUTION graph edge", "accounts.csv", str(account["account_id"])))


def _check_entity_type(
    rows: list[dict[str, object]],
    filename: str,
    column: str,
    entity_to_types: dict[str, set[str]],
    allowed_types: set[str],
    findings: list[ValidationFinding],
    nullable: bool = False,
) -> None:
    for row in rows:
        value = row.get(column)
        if value in (None, ""):
            if not nullable:
                findings.append(_error("foreign_key.required_missing", f"{column} is required", filename, _row_id(row)))
            continue
        actual_types = entity_to_types.get(str(value), set())
        if not actual_types.intersection(allowed_types):
            findings.append(_error("foreign_key.type_mismatch", f"{column}={value} must resolve to node type {sorted(allowed_types)}", filename, _row_id(row)))


def _check_required_accounts(members: list[dict[str, object]], accounts: list[dict[str, object]], findings: list[ValidationFinding]) -> None:
    by_member: dict[str, set[str]] = defaultdict(set)
    for account in accounts:
        if account.get("member_id"):
            by_member[str(account["member_id"])].add(str(account["account_type"]))
    for member in members:
        member_id = str(member["member_id"])
        account_types = by_member[member_id]
        if member["member_type"] == "ORGANIZATION":
            if "FOSA_CURRENT" not in account_types:
                findings.append(_error("foreign_key.required_account_missing", "Organization member requires FOSA_CURRENT", "members.csv", member_id))
        else:
            required = {"BOSA_DEPOSIT", "SHARE_CAPITAL"}
            missing = required.difference(account_types)
            if missing:
                findings.append(_error("foreign_key.required_account_missing", f"Member missing required accounts: {sorted(missing)}", "members.csv", member_id))
            if not {"FOSA_SAVINGS", "FOSA_CURRENT"}.intersection(account_types):
                findings.append(_error("foreign_key.required_account_missing", "Member requires FOSA_SAVINGS or FOSA_CURRENT", "members.csv", member_id))


def _check_txn_entity(
    txn: dict[str, object],
    column: str,
    entity_to_types: dict[str, set[str]],
    allowed_types: set[str],
    findings: list[ValidationFinding],
    nullable: bool = False,
) -> None:
    value = txn.get(column)
    if value in (None, ""):
        if not nullable:
            findings.append(_error("foreign_key.txn_required_missing", f"{column} is required", "transactions.csv", str(txn["txn_id"])))
        return
    if not entity_to_types.get(str(value), set()).intersection(allowed_types):
        findings.append(_error("foreign_key.txn_type_mismatch", f"{column}={value} must resolve to node type {sorted(allowed_types)}", "transactions.csv", str(txn["txn_id"])))


def _row_id(row: dict[str, object]) -> str | None:
    for key in ("member_id", "account_id", "edge_id", "node_id", "txn_id", "guarantee_id", "loan_id"):
        if row.get(key):
            return str(row[key])
    return None


def _error(code: str, message: str, filename: str | None = None, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, filename, row_id)
