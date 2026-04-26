from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date

from kenya_sacco_sim.core.models import ValidationFinding


GUARANTEED_PRODUCTS = {"DEVELOPMENT_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE"}


def validate_loans(rows_by_file: dict[str, list[dict[str, object]]]) -> tuple[list[ValidationFinding], dict[str, object]]:
    loans = rows_by_file.get("loans.csv", [])
    if not loans:
        return [], {"status": "not_applicable"}
    findings: list[ValidationFinding] = []
    accounts = {str(row["account_id"]): row for row in rows_by_file.get("accounts.csv", [])}
    transactions = rows_by_file.get("transactions.csv", [])
    disbursement_by_account = defaultdict(float)
    repayment_by_account = defaultdict(float)
    penalty_by_account = defaultdict(float)
    for txn in transactions:
        if txn["txn_type"] == "LOAN_DISBURSEMENT":
            disbursement_by_account[str(txn["account_id_dr"])] += float(txn["amount_kes"])
        if txn["txn_type"] in {"LOAN_REPAYMENT", "CHECKOFF_LOAN_RECOVERY"}:
            repayment_by_account[str(txn["account_id_cr"])] += float(txn["amount_kes"])
        if txn["txn_type"] == "PENALTY_POST":
            penalty_by_account[str(txn["account_id_dr"])] += float(txn["amount_kes"])

    status_counts = defaultdict(int)
    for loan in loans:
        loan_id = str(loan["loan_id"])
        loan_account_id = str(loan["loan_account_id"])
        principal = float(loan["principal_kes"])
        status_counts[str(loan["performing_status"])] += 1
        account = accounts.get(loan_account_id)
        if account is None or account["account_type"] != "LOAN_ACCOUNT":
            findings.append(_error("loan.account_missing", "Loan account must exist and be LOAN_ACCOUNT", loan_id))
            continue
        if principal <= 0:
            findings.append(_error("loan.principal_non_positive", "Loan principal must be positive", loan_id))
        if int(loan["tenor_months"]) < 1:
            findings.append(_error("loan.tenor_invalid", "Loan tenor must be at least one month", loan_id))
        application = date.fromisoformat(str(loan["application_date"]))
        approval = date.fromisoformat(str(loan["approval_date"]))
        disbursement = date.fromisoformat(str(loan["disbursement_date"]))
        if approval < application:
            findings.append(_error("loan.approval_before_application", "approval_date must be >= application_date", loan_id))
        if disbursement < approval:
            findings.append(_error("loan.disbursement_before_approval", "disbursement_date must be >= approval_date", loan_id))
        if round(disbursement_by_account[loan_account_id], 2) != round(principal, 2):
            findings.append(_error("loan.disbursement_principal_mismatch", "LOAN_DISBURSEMENT total must equal principal", loan_id))
        total_due = principal + penalty_by_account[loan_account_id]
        if repayment_by_account[loan_account_id] - total_due > 0.005:
            findings.append(_error("loan.repayment_exceeds_total_due", "Repayments cannot exceed principal plus posted penalties", loan_id))
        outstanding = round(principal + penalty_by_account[loan_account_id] - repayment_by_account[loan_account_id], 2)
        if round(float(account["current_balance_kes"]), 2) != outstanding:
            findings.append(_error("loan.outstanding_balance_mismatch", "LOAN_ACCOUNT current balance must equal outstanding principal plus penalties", loan_id))
        if outstanding <= 0.01 and loan["performing_status"] != "CLOSED":
            findings.append(_error("loan.zero_balance_not_closed", "Zero-balance loans must have performing_status CLOSED", loan_id))
        if loan["performing_status"] == "CLOSED" and outstanding > 0.01:
            findings.append(_error("loan.closed_positive_balance", "Closed loans must have zero outstanding balance", loan_id))
        if loan["performing_status"] == "CURRENT" and int(loan["arrears_days"]) != 0:
            findings.append(_error("loan.current_with_arrears", "CURRENT loans must have zero arrears_days", loan_id))
        if loan["performing_status"] in {"IN_ARREARS", "DEFAULTED"} and int(loan["arrears_days"]) <= 0:
            findings.append(_error("loan.arrears_status_without_days", "Arrears/default status requires positive arrears_days", loan_id))

    total = len(loans)
    arrears_count = status_counts["IN_ARREARS"] + status_counts["DEFAULTED"]
    return findings, {
        "loan_count": total,
        "status_counts": dict(sorted(status_counts.items())),
        "arrears_share": round(arrears_count / total, 4) if total else 0,
        "default_share": round(status_counts["DEFAULTED"] / total, 4) if total else 0,
        "repayment_transaction_count": sum(1 for txn in transactions if txn["txn_type"] in {"LOAN_REPAYMENT", "CHECKOFF_LOAN_RECOVERY"}),
        "penalty_transaction_count": sum(1 for txn in transactions if txn["txn_type"] == "PENALTY_POST"),
    }


def validate_guarantors(rows_by_file: dict[str, list[dict[str, object]]]) -> tuple[list[ValidationFinding], dict[str, object]]:
    guarantors = rows_by_file.get("guarantors.csv", [])
    loans = {str(row["loan_id"]): row for row in rows_by_file.get("loans.csv", [])}
    if not loans:
        return [], {"status": "not_applicable"}
    findings: list[ValidationFinding] = []
    members = {str(row["member_id"]) for row in rows_by_file.get("members.csv", [])}
    edge_pairs = set()
    for edge in rows_by_file.get("graph_edges.csv", []):
        if edge["edge_type"] == "GUARANTEES":
            edge_pairs.add((str(edge["src_node_id"]), str(edge["dst_node_id"])))
    node_by_entity = {str(node["entity_id"]): str(node["node_id"]) for node in rows_by_file.get("nodes.csv", [])}
    by_loan = defaultdict(list)
    active_counts_by_guarantor = Counter()
    for guarantee in guarantors:
        loan_id = str(guarantee["loan_id"])
        by_loan[loan_id].append(guarantee)
        borrower = str(guarantee["borrower_member_id"])
        guarantor = str(guarantee["guarantor_member_id"])
        row_id = str(guarantee["guarantee_id"])
        if borrower == guarantor:
            findings.append(_error("guarantor.self_guarantee", "Borrower cannot guarantee own loan", row_id))
        if borrower not in members or guarantor not in members:
            findings.append(_error("guarantor.member_missing", "Borrower and guarantor must exist in members.csv", row_id))
        if float(guarantee["guarantor_capacity_remaining_kes"]) < -0.005:
            findings.append(_error("guarantor.capacity_negative", "Guarantor capacity cannot be negative", row_id))
        pair = (node_by_entity.get(guarantor), node_by_entity.get(borrower))
        if pair not in edge_pairs:
            findings.append(_error("guarantor.edge_missing", "GUARANTEES graph edge must exist", row_id))
        loan = loans.get(loan_id)
        if loan and loan["performing_status"] != "CLOSED":
            active_counts_by_guarantor[guarantor] += 1

    guaranteed_loans = 0
    total_guarantors = 0
    for loan in loans.values():
        if loan["product_code"] not in GUARANTEED_PRODUCTS:
            continue
        guaranteed_loans += 1
        entries = by_loan[str(loan["loan_id"])]
        total_guarantors += len(entries)
        guaranteed_amount = sum(float(entry["guarantee_amount_kes"]) for entry in entries)
        if guaranteed_amount + 0.005 < float(loan["principal_kes"]) * 0.45:
            findings.append(_error("guarantor.coverage_low", "Guaranteed loan must have at least 45% guarantee coverage", str(loan["loan_id"])))

    for guarantor_id, count in active_counts_by_guarantor.items():
        if count > 10:
            findings.append(ValidationFinding("error", "guarantor.concentration_error", f"Guarantor backs {count} active loans; clean maximum is 10", "guarantors.csv", guarantor_id))
        elif count > 5:
            findings.append(ValidationFinding("warning", "guarantor.concentration_warning", f"Guarantor backs {count} active loans; normal review threshold is 5", "guarantors.csv", guarantor_id))

    return findings, {
        "guarantor_row_count": len(guarantors),
        "guaranteed_loan_count": guaranteed_loans,
        "avg_guarantors_per_guaranteed_loan": round(total_guarantors / guaranteed_loans, 3) if guaranteed_loans else 0,
        "max_active_guarantees_per_member": max(active_counts_by_guarantor.values(), default=0),
        "guarantors_backing_gt5_active_loans": sum(1 for count in active_counts_by_guarantor.values() if count > 5),
        "guarantors_backing_gt10_active_loans": sum(1 for count in active_counts_by_guarantor.values() if count > 10),
    }


def validate_credit_distribution(rows_by_file: dict[str, list[dict[str, object]]]) -> tuple[list[ValidationFinding], dict[str, object]]:
    loans = rows_by_file.get("loans.csv", [])
    members = rows_by_file.get("members.csv", [])
    if not loans:
        return [], {"status": "not_applicable"}

    findings: list[ValidationFinding] = []
    member_by_id = {str(row["member_id"]): row for row in members}
    guarantors = rows_by_file.get("guarantors.csv", [])
    guaranteed_loan_ids = {str(row["loan_id"]) for row in guarantors}
    status_counts = Counter(str(row["performing_status"]) for row in loans)
    product_counts = Counter(str(row["product_code"]) for row in loans)
    purpose_counts = Counter(str(row["purpose_code"]) for row in loans)
    persona_counts = Counter(str(member_by_id[str(row["member_id"])]["persona_type"]) for row in loans if str(row["member_id"]) in member_by_id)
    total_loans = len(loans)
    total_members = len(members)
    active_member_ratio = total_loans / total_members if total_members else 0.0
    arrears_count = status_counts["IN_ARREARS"] + status_counts["DEFAULTED"]
    arrears_share = arrears_count / total_loans if total_loans else 0.0
    default_share = status_counts["DEFAULTED"] / total_loans if total_loans else 0.0
    repayment_success_rate = (status_counts["CURRENT"] + status_counts["CLOSED"]) / total_loans if total_loans else 0.0
    avg_guarantors = len(guarantors) / len(guaranteed_loan_ids) if guaranteed_loan_ids else 0.0

    _warn_range(findings, "credit.loan_active_member_ratio", active_member_ratio, 0.18, 0.40, "loans.csv")
    if guaranteed_loan_ids:
        _warn_range(findings, "credit.avg_guarantors_per_guaranteed_loan", avg_guarantors, 1.5, 3.0, "guarantors.csv")
    _warn_range(findings, "credit.arrears_share", arrears_share, 0.04, 0.12, "loans.csv")
    _warn_range(findings, "credit.default_share", default_share, 0.01, 0.05, "loans.csv")
    _warn_range(findings, "credit.repayment_success_rate", repayment_success_rate, 0.80, 0.95, "loans.csv")

    return findings, {
        "loan_active_member_ratio": round(active_member_ratio, 4),
        "avg_guarantors_per_guaranteed_loan": round(avg_guarantors, 3),
        "arrears_share": round(arrears_share, 4),
        "default_share": round(default_share, 4),
        "repayment_success_rate": round(repayment_success_rate, 4),
        "loan_count": total_loans,
        "guarantor_count": len(guarantors),
        "guaranteed_loan_count": len(guaranteed_loan_ids),
        "status_counts": dict(sorted(status_counts.items())),
        "product_counts": dict(sorted(product_counts.items())),
        "purpose_counts": dict(sorted(purpose_counts.items())),
        "persona_counts": dict(sorted(persona_counts.items())),
    }


def _error(code: str, message: str, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, "loans.csv", row_id)


def _warn_range(findings: list[ValidationFinding], metric: str, value: float, low: float, high: float, filename: str) -> None:
    if low <= value <= high:
        return
    findings.append(
        ValidationFinding(
            "warning",
            f"{metric}_out_of_range",
            f"{metric} {value:.3f} is outside target range {low:.2f}-{high:.2f}",
            filename,
        )
    )
