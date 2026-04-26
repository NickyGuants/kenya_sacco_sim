from __future__ import annotations

from collections import Counter, defaultdict

from kenya_sacco_sim.core.models import ValidationFinding


FORBIDDEN_FEATURE_COLUMNS = {
    "transactions.csv": {"is_suspicious", "typology", "pattern_id", "alert_id", "source_is_illicit", "synthetic_flag"},
    "members.csv": {"criminal_flag", "shell_flag", "suspicious_member", "injected_typology"},
    "accounts.csv": {"mule_account_flag", "laundering_account_flag"},
    "graph_edges.csv": {"typology", "alert_id", "pattern_id", "is_suspicious"},
}


def validate_labels(rows_by_file: dict[str, list[dict[str, object]]], suspicious_ratio: float, tolerance: float = 0.002) -> tuple[list[ValidationFinding], dict[str, object], dict[str, object]]:
    alerts = rows_by_file.get("alerts_truth.csv", [])
    if not alerts:
        return [], {"status": "not_applicable"}, {"status": "not_applicable"}

    findings: list[ValidationFinding] = []
    transactions = {str(row["txn_id"]) for row in rows_by_file.get("transactions.csv", [])}
    members = {str(row["member_id"]) for row in rows_by_file.get("members.csv", [])}
    accounts = {str(row["account_id"]) for row in rows_by_file.get("accounts.csv", [])}
    edges = {str(row["edge_id"]) for row in rows_by_file.get("graph_edges.csv", [])}
    by_pattern: dict[str, list[dict[str, object]]] = defaultdict(list)
    suspicious_txn_ids: set[str] = set()

    for alert in alerts:
        row_id = str(alert["alert_id"])
        by_pattern[str(alert["pattern_id"])].append(alert)
        if alert.get("member_id") and str(alert["member_id"]) not in members:
            findings.append(_error("label.member_missing", "alert member_id must resolve to members.csv", row_id))
        if alert.get("account_id") and str(alert["account_id"]) not in accounts:
            findings.append(_error("label.account_missing", "alert account_id must resolve to accounts.csv", row_id))
        if alert.get("txn_id"):
            txn_id = str(alert["txn_id"])
            suspicious_txn_ids.add(txn_id)
            if txn_id not in transactions:
                findings.append(_error("label.txn_missing", "alert txn_id must resolve to transactions.csv", row_id))
        if alert.get("edge_id") and str(alert["edge_id"]) not in edges:
            findings.append(_error("label.edge_missing", "alert edge_id must resolve to graph_edges.csv", row_id))

    pattern_summary_count = 0
    typology_counts = Counter(str(alert["typology"]) for alert in alerts if alert["entity_type"] == "PATTERN")
    for pattern_id, pattern_alerts in by_pattern.items():
        summaries = [alert for alert in pattern_alerts if alert["entity_type"] == "PATTERN" and alert["stage"] == "PATTERN_SUMMARY"]
        if len(summaries) != 1:
            findings.append(_error("label.pattern_summary_count", "Every suspicious pattern must have exactly one PATTERN_SUMMARY row", pattern_id))
        pattern_summary_count += len(summaries)
        txn_alerts = [alert for alert in pattern_alerts if alert.get("txn_id")]
        if not txn_alerts:
            findings.append(_error("label.pattern_without_txn_labels", "Suspicious pattern must label at least one transaction", pattern_id))

    findings.extend(_label_leakage_findings(rows_by_file))
    suspicious_member_count = len({str(alert["member_id"]) for alert in alerts if alert["entity_type"] == "PATTERN"})
    member_count = len(rows_by_file.get("members.csv", []))
    realized_ratio = suspicious_member_count / member_count if member_count else 0.0
    if abs(realized_ratio - suspicious_ratio) > tolerance:
        findings.append(_error("label.suspicious_ratio_out_of_tolerance", f"Suspicious member ratio {realized_ratio:.4f} is outside target {suspicious_ratio:.4f} +/- {tolerance:.4f}", None))
    typology_section = {
        "pattern_summary_count": pattern_summary_count,
        "labeled_suspicious_transaction_count": len(suspicious_txn_ids),
        "pattern_counts": dict(sorted(typology_counts.items())),
        "structuring_pattern_count": typology_counts["STRUCTURING"],
        "rapid_pass_through_pattern_count": typology_counts["RAPID_PASS_THROUGH"],
    }
    label_section = {
        "alert_count": len(alerts),
        "pattern_count": len(by_pattern),
        "suspicious_member_count": suspicious_member_count,
        "suspicious_member_ratio": round(realized_ratio, 4),
        "target_suspicious_member_ratio": suspicious_ratio,
        "suspicious_transaction_count": len(suspicious_txn_ids),
        "error_count": sum(1 for finding in findings if finding.severity == "error" and finding.code.startswith("label")),
        "warning_count": sum(1 for finding in findings if finding.severity == "warning" and finding.code.startswith("label")),
    }
    return findings, label_section, typology_section


def _label_leakage_findings(rows_by_file: dict[str, list[dict[str, object]]]) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    for filename, forbidden_columns in FORBIDDEN_FEATURE_COLUMNS.items():
        rows = rows_by_file.get(filename, [])
        if not rows:
            continue
        leaked = forbidden_columns.intersection(rows[0].keys())
        for column in sorted(leaked):
            findings.append(_error("label.leakage", f"{column} is only allowed in label files", filename))
    return findings


def _error(code: str, message: str, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, "alerts_truth.csv", row_id)
