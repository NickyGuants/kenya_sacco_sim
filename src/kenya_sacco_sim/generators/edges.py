from __future__ import annotations

import json

from kenya_sacco_sim.core.models import InstitutionWorld


def generate_edges(
    members: list[dict[str, object]],
    accounts: list[dict[str, object]],
    world: InstitutionWorld,
    nodes: list[dict[str, object]],
) -> list[dict[str, object]]:
    node_by_entity = {str(node["entity_id"]): str(node["node_id"]) for node in nodes}
    edges: list[dict[str, object]] = []

    for account in accounts:
        member_id = account["member_id"]
        if not member_id:
            continue
        edge_type = "HAS_WALLET" if str(account["account_type"]).endswith("_WALLET") else "HAS_ACCOUNT"
        _append_edge(edges, node_by_entity[str(member_id)], node_by_entity[str(account["account_id"])], edge_type, str(account["open_date"]), {"account_type": account["account_type"]})

    for member in members:
        employer_id = member.get("employer_id")
        if employer_id:
            _append_edge(edges, node_by_entity[str(member["member_id"])], node_by_entity[str(employer_id)], "EMPLOYED_BY", str(member["join_date"]), {})

    return edges


def _append_edge(edges: list[dict[str, object]], src_node_id: str, dst_node_id: str, edge_type: str, start_date: str, metadata: dict[str, object]) -> None:
    edges.append(
        {
            "edge_id": f"EDGE_{len(edges) + 1:06d}",
            "src_node_id": src_node_id,
            "dst_node_id": dst_node_id,
            "edge_type": edge_type,
            "start_date": start_date,
            "end_date": None,
            "weight": 1.0,
            "metadata_json": json.dumps(metadata, sort_keys=True),
        }
    )
