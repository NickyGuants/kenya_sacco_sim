from __future__ import annotations

from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP

from kenya_sacco_sim.core.models import ValidationFinding
from kenya_sacco_sim.core.rules import RULE_CONFIGS


FORBIDDEN_FEATURE_COLUMNS = {
    "transactions.csv": {"is_suspicious", "typology", "pattern_id", "alert_id", "source_is_illicit", "synthetic_flag"},
    "members.csv": {"criminal_flag", "shell_flag", "suspicious_member", "injected_typology"},
    "accounts.csv": {"mule_account_flag", "laundering_account_flag"},
    "graph_edges.csv": {"typology", "alert_id", "pattern_id", "is_suspicious"},
}


def validate_labels(rows_by_file: dict[str, list[dict[str, object]]], suspicious_ratio: float, tolerance: float = 0.002) -> tuple[list[ValidationFinding], dict[str, object], dict[str, object]]:
    findings: list[ValidationFinding] = []
    findings.extend(_label_leakage_findings(rows_by_file))
    member_count = len(rows_by_file.get("members.csv", []))
    target_suspicious_member_count = _target_suspicious_count(member_count, suspicious_ratio)
    count_tolerance = 0 if target_suspicious_member_count == 0 else _suspicious_count_tolerance(member_count, tolerance)
    if "alerts_truth.csv" not in rows_by_file:
        section = _empty_label_section("not_applicable", suspicious_ratio, target_suspicious_member_count, count_tolerance, findings)
        return findings, section, {"status": "not_applicable"}

    alerts = rows_by_file.get("alerts_truth.csv", [])
    if not alerts:
        if target_suspicious_member_count > 0:
            findings.append(_error("label.alerts_missing", "alerts_truth.csv is present but empty while suspicious_ratio expects labels", None))
            status = "labels_missing"
        else:
            status = "no_labels_expected"
        section = _empty_label_section(status, suspicious_ratio, target_suspicious_member_count, count_tolerance, findings)
        return findings, section, {"status": status}

    transactions = {str(row["txn_id"]) for row in rows_by_file.get("transactions.csv", [])}
    members = {str(row["member_id"]) for row in rows_by_file.get("members.csv", [])}
    accounts = {str(row["account_id"]) for row in rows_by_file.get("accounts.csv", [])}
    edges = {str(row["edge_id"]) for row in rows_by_file.get("graph_edges.csv", [])}
    by_pattern: dict[str, list[dict[str, object]]] = defaultdict(list)
    suspicious_txn_ids: set[str] = set()
    suspicious_txn_ids_by_member: dict[str, set[str]] = defaultdict(set)

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
            if alert.get("member_id"):
                suspicious_txn_ids_by_member[str(alert["member_id"])].add(txn_id)
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

    suspicious_member_count = len({str(alert["member_id"]) for alert in alerts if alert["entity_type"] == "PATTERN"})
    realized_ratio = suspicious_member_count / member_count if member_count else 0.0
    count_delta = abs(suspicious_member_count - target_suspicious_member_count)
    if count_delta > count_tolerance:
        findings.append(_error("label.suspicious_ratio_out_of_tolerance", f"Suspicious member count {suspicious_member_count} is outside target {target_suspicious_member_count} +/- {count_tolerance}", None))
    blend_metrics = _suspicious_blending_metrics(rows_by_file, suspicious_txn_ids_by_member)
    for member_id in blend_metrics["members_below_50pct_normal_share"]:
        findings.append(_error("label.suspicious_member_blending_low", "Suspicious member normal transaction share must be >= 0.50", member_id))
    id_leakage_metrics = _txn_id_leakage_metrics(rows_by_file, suspicious_txn_ids)
    threshold_rule = id_leakage_metrics["best_txn_id_threshold_rule"]
    if threshold_rule["precision"] > 0.70 and threshold_rule["recall"] > 0.70:
        findings.append(_error("label.txn_id_threshold_leakage", "Simple txn_id threshold recovers suspicious transactions above benchmark safety limit", threshold_rule["threshold_txn_id"], file="transactions.csv"))
    reference_metrics = _reference_leakage_metrics(rows_by_file)
    if reference_metrics["mirrored_reference_count"]:
        findings.append(_error("label.reference_mirrors_txn_id", "reference must not mirror txn_id with a REF prefix", None, file="transactions.csv"))
    typology_section = {
        "pattern_summary_count": pattern_summary_count,
        "labeled_suspicious_transaction_count": len(suspicious_txn_ids),
        "pattern_counts": dict(sorted(typology_counts.items())),
        "structuring_pattern_count": typology_counts["STRUCTURING"],
        "rapid_pass_through_pattern_count": typology_counts["RAPID_PASS_THROUGH"],
        "fake_affordability_pattern_count": typology_counts["FAKE_AFFORDABILITY_BEFORE_LOAN"],
        "rule_configs": RULE_CONFIGS,
    }
    label_section = {
        "alert_count": len(alerts),
        "pattern_count": len(by_pattern),
        "suspicious_member_count": suspicious_member_count,
        "suspicious_member_ratio": round(realized_ratio, 4),
        "target_suspicious_member_ratio": suspicious_ratio,
        "target_suspicious_member_count": target_suspicious_member_count,
        "suspicious_member_count_tolerance": count_tolerance,
        "suspicious_transaction_count": len(suspicious_txn_ids),
        "min_suspicious_member_normal_txn_share": blend_metrics["min_normal_txn_share"],
        "members_below_50pct_normal_share": blend_metrics["members_below_50pct_normal_share"],
        "id_leakage_metrics": id_leakage_metrics,
        "reference_leakage_metrics": reference_metrics,
        "error_count": sum(1 for finding in findings if finding.severity == "error" and finding.code.startswith("label")),
        "warning_count": sum(1 for finding in findings if finding.severity == "warning" and finding.code.startswith("label")),
    }
    return findings, label_section, typology_section


def _empty_label_section(status: str, suspicious_ratio: float, target_count: int, count_tolerance: int, findings: list[ValidationFinding]) -> dict[str, object]:
    return {
        "status": status,
        "alert_count": 0,
        "pattern_count": 0,
        "suspicious_member_count": 0,
        "suspicious_member_ratio": 0.0,
        "target_suspicious_member_ratio": suspicious_ratio,
        "target_suspicious_member_count": target_count,
        "suspicious_member_count_tolerance": count_tolerance,
        "suspicious_transaction_count": 0,
        "error_count": sum(1 for finding in findings if finding.severity == "error" and finding.code.startswith("label")),
        "warning_count": sum(1 for finding in findings if finding.severity == "warning" and finding.code.startswith("label")),
    }


def _target_suspicious_count(member_count: int, suspicious_ratio: float) -> int:
    target = Decimal(str(member_count)) * Decimal(str(suspicious_ratio))
    return max(0, int(target.to_integral_value(rounding=ROUND_HALF_UP)))


def _suspicious_count_tolerance(member_count: int, ratio_tolerance: float) -> int:
    if member_count <= 0:
        return 0
    return max(1, round(member_count * ratio_tolerance))


def _label_leakage_findings(rows_by_file: dict[str, list[dict[str, object]]]) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    for filename, forbidden_columns in FORBIDDEN_FEATURE_COLUMNS.items():
        rows = rows_by_file.get(filename, [])
        if not rows:
            continue
        leaked = forbidden_columns.intersection(rows[0].keys())
        for column in sorted(leaked):
            findings.append(_error("label.leakage", f"{column} is only allowed in label files", filename, file=filename))
    return findings


def _suspicious_blending_metrics(rows_by_file: dict[str, list[dict[str, object]]], suspicious_txn_ids_by_member: dict[str, set[str]]) -> dict[str, object]:
    total_by_member = Counter(str(row["member_id_primary"]) for row in rows_by_file.get("transactions.csv", []) if row.get("member_id_primary"))
    shares: dict[str, float] = {}
    below: list[str] = []
    for member_id, suspicious_txn_ids in suspicious_txn_ids_by_member.items():
        total = total_by_member[member_id]
        normal = max(0, total - len(suspicious_txn_ids))
        share = normal / total if total else 0.0
        shares[member_id] = share
        if share < 0.50:
            below.append(member_id)
    return {
        "min_normal_txn_share": round(min(shares.values()), 4) if shares else 1.0,
        "members_below_50pct_normal_share": sorted(below),
    }


def _txn_id_leakage_metrics(rows_by_file: dict[str, list[dict[str, object]]], suspicious_txn_ids: set[str]) -> dict[str, object]:
    numbered_rows: list[tuple[int, bool]] = []
    suspicious_numbers: list[int] = []
    for row in rows_by_file.get("transactions.csv", []):
        txn_id = str(row["txn_id"])
        number = _txn_number(txn_id)
        if number is None:
            continue
        is_suspicious = txn_id in suspicious_txn_ids
        numbered_rows.append((number, is_suspicious))
        if is_suspicious:
            suspicious_numbers.append(number)

    if not numbered_rows or not suspicious_numbers:
        return {
            "min_suspicious_txn_id_percentile": None,
            "max_suspicious_txn_id_percentile": None,
            "best_txn_id_threshold_rule": _empty_threshold_rule(),
        }

    max_txn_number = max(number for number, _ in numbered_rows)
    threshold_rule = _best_txn_id_threshold_rule(numbered_rows)
    return {
        "min_suspicious_txn_id_percentile": round(min(suspicious_numbers) / max_txn_number, 4),
        "max_suspicious_txn_id_percentile": round(max(suspicious_numbers) / max_txn_number, 4),
        "best_txn_id_threshold_rule": threshold_rule,
    }


def _best_txn_id_threshold_rule(numbered_rows: list[tuple[int, bool]]) -> dict[str, object]:
    rows = sorted(numbered_rows)
    total_suspicious = sum(1 for _, is_suspicious in rows if is_suspicious)
    if not rows or not total_suspicious:
        return _empty_threshold_rule()

    best = _empty_threshold_rule()
    prefix_suspicious = 0
    total_rows = len(rows)
    for index, (number, is_suspicious) in enumerate(rows):
        if is_suspicious:
            prefix_suspicious += 1

        low_predicted = index + 1
        low_rule = _threshold_rule("txn_id_lte", number, prefix_suspicious, low_predicted, total_suspicious)
        best = _better_threshold_rule(best, low_rule)

        high_predicted = total_rows - index
        high_tp = total_suspicious - (prefix_suspicious - (1 if is_suspicious else 0))
        high_rule = _threshold_rule("txn_id_gte", number, high_tp, high_predicted, total_suspicious)
        best = _better_threshold_rule(best, high_rule)
    return best


def _threshold_rule(direction: str, threshold: int, true_positive: int, predicted_positive: int, total_suspicious: int) -> dict[str, object]:
    precision = true_positive / predicted_positive if predicted_positive else 0.0
    recall = true_positive / total_suspicious if total_suspicious else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "direction": direction,
        "threshold_txn_id": f"TXN{threshold:012d}",
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "true_positive": true_positive,
        "predicted_positive": predicted_positive,
    }


def _better_threshold_rule(current: dict[str, object], candidate: dict[str, object]) -> dict[str, object]:
    current_score = (min(float(current["precision"]), float(current["recall"])), float(current["f1"]))
    candidate_score = (min(float(candidate["precision"]), float(candidate["recall"])), float(candidate["f1"]))
    return candidate if candidate_score > current_score else current


def _empty_threshold_rule() -> dict[str, object]:
    return {
        "direction": None,
        "threshold_txn_id": None,
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
        "true_positive": 0,
        "predicted_positive": 0,
    }


def _reference_leakage_metrics(rows_by_file: dict[str, list[dict[str, object]]]) -> dict[str, object]:
    mirrored = [
        str(row["txn_id"])
        for row in rows_by_file.get("transactions.csv", [])
        if str(row.get("reference") or "") == str(row["txn_id"]).replace("TXN", "REF", 1)
    ]
    return {
        "mirrored_reference_count": len(mirrored),
        "mirrored_reference_sample_txn_ids": mirrored[:10],
    }


def _txn_number(txn_id: str) -> int | None:
    if len(txn_id) != 15 or not txn_id.startswith("TXN"):
        return None
    try:
        return int(txn_id[3:])
    except ValueError:
        return None


def _error(code: str, message: str, row_id: str | None = None, file: str = "alerts_truth.csv") -> ValidationFinding:
    return ValidationFinding("error", code, message, file, row_id)
