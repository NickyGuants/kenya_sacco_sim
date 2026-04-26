from __future__ import annotations

import json

from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_edges(
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    world: InstitutionWorld,
    nodes: list[dict[str, object]],
    guarantors: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    node_by_entity = {str(node["entity_id"]): str(node["node_id"]) for node in nodes}
    ids = IdFactory()
    edges: list[dict[str, object]] = []

    for branch in world.branches:
        _append_edge(
            ids,
            edges,
            node_by_entity[str(branch["institution_id"])],
            node_by_entity[str(branch["branch_id"])],
            "INSTITUTION_HAS_BRANCH",
            str(branch["created_at"])[:10],
            {},
        )

    for employer in world.employers:
        _append_edge(
            ids,
            edges,
            node_by_entity[str(employer["employer_id"])],
            node_by_entity[str(employer["institution_id"])],
            "EMPLOYER_BELONGS_TO_INSTITUTION",
            str(employer["created_at"])[:10],
            {},
        )

    for agent in world.agents:
        _append_edge(
            ids,
            edges,
            node_by_entity[str(agent["agent_id"])],
            node_by_entity[str(agent["branch_id"])],
            "USES_AGENT",
            str(agent["created_at"])[:10],
            {},
        )

    for account in accounts:
        member_id = account["member_id"]
        account_id = str(account["account_id"])
        if member_id:
            edge_type = "HAS_WALLET" if str(account["account_type"]).endswith("_WALLET") else "HAS_ACCOUNT"
            _append_edge(ids, edges, node_by_entity[str(member_id)], node_by_entity[account_id], edge_type, str(account["open_date"]), {"account_type": account["account_type"]})
        if account.get("institution_id"):
            _append_edge(ids, edges, node_by_entity[account_id], node_by_entity[str(account["institution_id"])], "ACCOUNT_BELONGS_TO_INSTITUTION", str(account["open_date"]), {"account_type": account["account_type"]})
        if account.get("branch_id"):
            _append_edge(ids, edges, node_by_entity[account_id], node_by_entity[str(account["branch_id"])], "ACCOUNT_AT_BRANCH", str(account["open_date"]), {"account_type": account["account_type"]})

    for member in members:
        employer_id = member.get("employer_id")
        if employer_id:
            _append_edge(ids, edges, node_by_entity[str(member["member_id"])], node_by_entity[str(employer_id)], "EMPLOYED_BY", str(member["join_date"]), {})

    for guarantee in guarantors or []:
        _append_edge(
            ids,
            edges,
            node_by_entity[str(guarantee["guarantor_member_id"])],
            node_by_entity[str(guarantee["borrower_member_id"])],
            "GUARANTEES",
            str(guarantee["pledge_date"]),
            {"loan_id": guarantee["loan_id"], "guarantee_amount_kes": guarantee["guarantee_amount_kes"]},
        )

    source_nodes = [node_by_entity[str(account["account_id"])] for account in accounts if account["account_type"] == "SOURCE_ACCOUNT"]
    sink_nodes = [node_by_entity[str(account["account_id"])] for account in accounts if account["account_type"] == "SINK_ACCOUNT"]
    for account in accounts:
        if account["account_type"] in {"FOSA_SAVINGS", "FOSA_CURRENT", "MPESA_WALLET"}:
            account_node = node_by_entity[str(account["account_id"])]
            for source_node in source_nodes:
                _append_edge(ids, edges, source_node, account_node, "SOURCE_FUNDS_ACCOUNT", str(account["open_date"]), {"account_type": account["account_type"]})
            for sink_node in sink_nodes:
                _append_edge(ids, edges, account_node, sink_node, "ACCOUNT_PAYS_SINK", str(account["open_date"]), {"account_type": account["account_type"]})

    return edges


def _append_edge(ids: IdFactory, edges: list[dict[str, object]], src_node_id: str, dst_node_id: str, edge_type: str, start_date: str, metadata: dict[str, object]) -> None:
    edges.append(
        {
            "edge_id": ids.next("EDGE"),
            "src_node_id": src_node_id,
            "dst_node_id": dst_node_id,
            "edge_type": edge_type,
            "start_date": start_date,
            "end_date": None,
            "weight": 1.0,
            "metadata_json": json.dumps(metadata, sort_keys=True),
        }
    )
