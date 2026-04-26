from __future__ import annotations

from kenya_sacco_sim.core.id_factory import IdFactory
from kenya_sacco_sim.core.models import InstitutionWorld


def generate_nodes(world: InstitutionWorld, members: list[dict[str, object]], accounts: list[dict[str, object]]) -> list[dict[str, object]]:
    ids = IdFactory()
    nodes: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    for institution in world.institutions:
        _append_node(ids, nodes, seen, str(institution["institution_id"]), "INSTITUTION", str(institution["institution_id"]), institution["county"], institution["urban_rural"], institution["created_at"])
    for branch in world.branches:
        _append_node(ids, nodes, seen, str(branch["branch_id"]), "BRANCH", str(branch["institution_id"]), branch["county"], branch["urban_rural"], branch["created_at"])
    for employer in world.employers:
        _append_node(ids, nodes, seen, str(employer["employer_id"]), "EMPLOYER", str(employer["institution_id"]), employer["county"], employer["urban_rural"], employer["created_at"])
    for agent in world.agents:
        _append_node(ids, nodes, seen, str(agent["agent_id"]), "AGENT", str(agent["institution_id"]), agent["county"], agent["urban_rural"], agent["created_at"])
    for device in world.devices:
        _append_node(ids, nodes, seen, str(device["device_id"]), "DEVICE", str(device["institution_id"]), None, None, device["created_at"])
    for member in members:
        _append_node(ids, nodes, seen, str(member["member_id"]), "MEMBER", str(member["institution_id"]), member["county"], member["urban_rural"], member["created_at"])
    for account in accounts:
        node_type = "WALLET" if str(account["account_type"]).endswith("_WALLET") else "ACCOUNT"
        if account["account_type"] == "SOURCE_ACCOUNT":
            node_type = "SOURCE"
        elif account["account_type"] == "SINK_ACCOUNT":
            node_type = "SINK"
        _append_node(ids, nodes, seen, str(account["account_id"]), node_type, account["institution_id"], None, None, f"{account['open_date']}T00:00:00+03:00")

    return nodes


def _append_node(
    ids: IdFactory,
    nodes: list[dict[str, object]],
    seen: set[tuple[str, str]],
    entity_id: str,
    node_type: str,
    institution_id: object | None,
    county: object | None,
    urban_rural: object | None,
    created_at: object,
) -> None:
    key = (entity_id, node_type)
    if key in seen:
        return
    seen.add(key)
    nodes.append(
        {
            "node_id": ids.next("NODE"),
            "node_type": node_type,
            "entity_id": entity_id,
            "institution_id": institution_id,
            "county": county,
            "urban_rural": urban_rural,
            "created_at": created_at,
        }
    )
