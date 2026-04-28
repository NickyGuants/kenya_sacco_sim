from __future__ import annotations

import json
from collections import Counter, defaultdict


def build_pattern_labels(alerts: list[dict[str, object]]) -> list[dict[str, object]]:
    """Collapse alert context rows into one benchmark label row per pattern."""
    by_pattern: dict[str, list[dict[str, object]]] = defaultdict(list)
    for alert in alerts:
        pattern_id = str(alert.get("pattern_id") or "")
        if pattern_id:
            by_pattern[pattern_id].append(alert)

    rows: list[dict[str, object]] = []
    for pattern_id, pattern_alerts in sorted(by_pattern.items()):
        summary = _pattern_summary(pattern_alerts)
        counts = Counter(str(alert.get("entity_type") or "") for alert in pattern_alerts)
        rows.append(
            {
                "pattern_id": pattern_id,
                "typology": summary.get("typology"),
                "member_id": summary.get("member_id"),
                "institution_id": summary.get("institution_id"),
                "account_id": summary.get("account_id"),
                "severity": summary.get("severity"),
                "stage": summary.get("stage"),
                "explanation_code": summary.get("explanation_code"),
                "start_timestamp": _min_timestamp(pattern_alerts),
                "end_timestamp": _max_timestamp(pattern_alerts),
                "truth_label": True,
                "transaction_alert_count": counts["TRANSACTION"],
                "member_alert_count": counts["MEMBER"],
                "account_alert_count": counts["ACCOUNT"],
                "edge_alert_count": counts["EDGE"],
                "alert_row_count": len(pattern_alerts),
            }
        )
    return rows


def build_edge_labels(
    alerts: list[dict[str, object]],
    graph_edges: list[dict[str, object]],
    nodes: list[dict[str, object]],
    loans: list[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    """Build graph-edge truth labels from explicit edge alerts and graph-backed typologies."""
    explicit = _explicit_edge_labels(alerts, graph_edges)
    explicit_keys = {(str(row["pattern_id"]), str(row["edge_id"])) for row in explicit}
    derived = _guarantor_ring_edge_labels(alerts, graph_edges, nodes, loans or [], explicit_keys)
    rows = explicit + derived
    rows.sort(key=lambda row: (str(row["pattern_id"]), str(row["edge_id"])))
    for index, row in enumerate(rows, start=1):
        row["edge_label_id"] = f"EDGELBL{index:08d}"
    return rows


def _pattern_summary(pattern_alerts: list[dict[str, object]]) -> dict[str, object]:
    summaries = [
        alert
        for alert in pattern_alerts
        if alert.get("entity_type") == "PATTERN" or alert.get("stage") == "PATTERN_SUMMARY"
    ]
    return summaries[0] if summaries else pattern_alerts[0]


def _min_timestamp(pattern_alerts: list[dict[str, object]]) -> str:
    values = [str(alert.get("start_timestamp") or "") for alert in pattern_alerts if alert.get("start_timestamp")]
    return min(values) if values else ""


def _max_timestamp(pattern_alerts: list[dict[str, object]]) -> str:
    values = [str(alert.get("end_timestamp") or "") for alert in pattern_alerts if alert.get("end_timestamp")]
    return max(values) if values else ""


def _explicit_edge_labels(alerts: list[dict[str, object]], graph_edges: list[dict[str, object]]) -> list[dict[str, object]]:
    edge_by_id = {str(edge.get("edge_id")): edge for edge in graph_edges}
    rows: list[dict[str, object]] = []
    for alert in alerts:
        edge_id = str(alert.get("edge_id") or "")
        if not edge_id:
            continue
        edge = edge_by_id.get(edge_id, {})
        rows.append(_edge_label_row(alert, edge_id, edge))
    return rows


def _guarantor_ring_edge_labels(
    alerts: list[dict[str, object]],
    graph_edges: list[dict[str, object]],
    nodes: list[dict[str, object]],
    loans: list[dict[str, object]],
    existing_keys: set[tuple[str, str]],
) -> list[dict[str, object]]:
    loan_by_account = {str(loan.get("loan_account_id")): str(loan.get("loan_id")) for loan in loans if loan.get("loan_account_id") and loan.get("loan_id")}
    edge_by_loan: dict[str, list[dict[str, object]]] = defaultdict(list)
    for edge in graph_edges:
        if edge.get("edge_type") != "GUARANTEES":
            continue
        metadata = _metadata(edge)
        loan_id = str(metadata.get("loan_id") or "")
        if loan_id:
            edge_by_loan[loan_id].append(edge)

    member_node_ids = {
        str(node.get("node_id"))
        for node in nodes
        if node.get("node_type") == "MEMBER"
    }
    rows: list[dict[str, object]] = []
    for alert in alerts:
        if alert.get("typology") != "GUARANTOR_FRAUD_RING" or alert.get("entity_type") != "PATTERN":
            continue
        loan_id = loan_by_account.get(str(alert.get("account_id") or ""))
        if not loan_id:
            continue
        for edge in edge_by_loan.get(loan_id, []):
            edge_id = str(edge.get("edge_id") or "")
            key = (str(alert.get("pattern_id")), edge_id)
            if key in existing_keys:
                continue
            if str(edge.get("src_node_id")) not in member_node_ids or str(edge.get("dst_node_id")) not in member_node_ids:
                continue
            rows.append(_edge_label_row(alert, edge_id, edge))
            existing_keys.add(key)
    return rows


def _edge_label_row(alert: dict[str, object], edge_id: str, edge: dict[str, object]) -> dict[str, object]:
    return {
        "edge_label_id": "",
        "pattern_id": alert.get("pattern_id"),
        "typology": alert.get("typology"),
        "edge_id": edge_id,
        "src_node_id": edge.get("src_node_id"),
        "dst_node_id": edge.get("dst_node_id"),
        "edge_type": edge.get("edge_type"),
        "member_id": alert.get("member_id"),
        "severity": alert.get("severity"),
        "explanation_code": alert.get("explanation_code"),
        "truth_label": True,
    }


def _metadata(edge: dict[str, object]) -> dict[str, object]:
    try:
        value = json.loads(str(edge.get("metadata_json") or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}
