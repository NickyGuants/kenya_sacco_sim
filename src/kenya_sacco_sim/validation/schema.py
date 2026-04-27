from __future__ import annotations

import re
from datetime import date, datetime

from kenya_sacco_sim.core.config import WorldConfig
from kenya_sacco_sim.core.enums import (
    ACCOUNT_OWNER_TYPES,
    ACCOUNT_STATUSES,
    ACCOUNT_TYPES,
    ALERT_ENTITY_TYPES,
    ALERT_SEVERITIES,
    ALERT_STAGES,
    CHANNELS,
    COUNTERPARTY_TYPES,
    EDGE_TYPES,
    KYC_LEVELS,
    MEMBER_TYPES,
    NODE_TYPES,
    PERSONA_TYPES,
    RAILS,
    RISK_SEGMENTS,
    TXN_TYPES,
    TYPOLOGIES,
)
from kenya_sacco_sim.core.models import ValidationFinding


REQUIRED_COLUMNS = {
    "members.csv": [
        "member_id",
        "institution_id",
        "member_type",
        "persona_type",
        "county",
        "urban_rural",
        "gender",
        "age",
        "occupation",
        "employer_id",
        "join_date",
        "kyc_level",
        "risk_segment",
        "phone_hash",
        "id_hash",
        "declared_monthly_income_kes",
        "income_stability_score",
        "dormant_flag",
        "created_at",
    ],
    "accounts.csv": [
        "account_id",
        "member_id",
        "institution_id",
        "account_owner_type",
        "account_type",
        "product_code",
        "open_date",
        "status",
        "linked_wallet_id",
        "branch_id",
        "currency",
        "opening_balance_kes",
        "current_balance_kes",
        "external_account_label",
    ],
    "nodes.csv": ["node_id", "node_type", "entity_id", "institution_id", "county", "urban_rural", "created_at"],
    "graph_edges.csv": ["edge_id", "src_node_id", "dst_node_id", "edge_type", "start_date", "end_date", "weight", "metadata_json"],
    "transactions.csv": [
        "txn_id",
        "timestamp",
        "institution_id",
        "account_id_dr",
        "account_id_cr",
        "member_id_primary",
        "txn_type",
        "rail",
        "channel",
        "provider",
        "counterparty_type",
        "counterparty_id_hash",
        "amount_kes",
        "fee_kes",
        "currency",
        "narrative",
        "reference",
        "branch_id",
        "agent_id",
        "device_id",
        "geo_bucket",
        "batch_id",
        "balance_after_dr_kes",
        "balance_after_cr_kes",
        "is_reversal",
    ],
    "loans.csv": [
        "loan_id",
        "member_id",
        "institution_id",
        "loan_account_id",
        "product_code",
        "application_date",
        "approval_date",
        "disbursement_date",
        "principal_kes",
        "tenor_months",
        "interest_rate_annual",
        "repayment_mode",
        "disbursement_channel",
        "purpose_code",
        "deposit_balance_at_application_kes",
        "loan_to_deposit_multiple",
        "performing_status",
        "arrears_days",
        "restructure_flag",
        "default_flag",
    ],
    "guarantors.csv": [
        "guarantee_id",
        "loan_id",
        "borrower_member_id",
        "guarantor_member_id",
        "guarantee_amount_kes",
        "guarantee_pct",
        "pledge_date",
        "release_date",
        "guarantor_deposit_balance_at_pledge_kes",
        "relationship_type",
        "guarantor_capacity_remaining_kes",
    ],
    "alerts_truth.csv": [
        "alert_id",
        "pattern_id",
        "typology",
        "entity_type",
        "entity_id",
        "member_id",
        "account_id",
        "txn_id",
        "edge_id",
        "start_timestamp",
        "end_timestamp",
        "severity",
        "truth_label",
        "stage",
        "explanation_code",
    ],
    "institutions.csv": [
        "institution_id",
        "name",
        "archetype",
        "county",
        "urban_rural",
        "digital_maturity",
        "cash_intensity",
        "loan_guarantor_intensity",
        "created_at",
    ],
    "branches.csv": ["branch_id", "institution_id", "county", "urban_rural", "branch_type", "opening_date", "created_at"],
    "agents.csv": ["agent_id", "institution_id", "branch_id", "provider", "county", "urban_rural", "location_type", "active_from", "active_to", "created_at"],
    "employers.csv": ["employer_id", "institution_id", "employer_type", "sector", "public_private", "county", "urban_rural", "payroll_frequency", "checkoff_supported", "created_at"],
    "devices.csv": ["device_id", "member_id", "institution_id", "first_seen", "last_seen", "os_family", "app_user_flag", "shared_device_group", "created_at"],
}

PRIMARY_KEYS = {
    "members.csv": "member_id",
    "accounts.csv": "account_id",
    "nodes.csv": "node_id",
    "graph_edges.csv": "edge_id",
    "transactions.csv": "txn_id",
    "loans.csv": "loan_id",
    "guarantors.csv": "guarantee_id",
    "alerts_truth.csv": "alert_id",
    "institutions.csv": "institution_id",
    "branches.csv": "branch_id",
    "agents.csv": "agent_id",
    "employers.csv": "employer_id",
    "devices.csv": "device_id",
}

OPTIONAL_FILES = {"transactions.csv", "loans.csv", "guarantors.csv", "alerts_truth.csv", "institutions.csv", "branches.csv", "agents.csv", "employers.csv", "devices.csv"}

STRICT_ENUMS = {
    ("members.csv", "member_type"): MEMBER_TYPES,
    ("members.csv", "persona_type"): PERSONA_TYPES,
    ("members.csv", "kyc_level"): KYC_LEVELS,
    ("members.csv", "risk_segment"): RISK_SEGMENTS,
    ("accounts.csv", "account_owner_type"): ACCOUNT_OWNER_TYPES,
    ("accounts.csv", "account_type"): ACCOUNT_TYPES,
    ("accounts.csv", "status"): ACCOUNT_STATUSES,
    ("accounts.csv", "currency"): {"KES"},
    ("nodes.csv", "node_type"): NODE_TYPES,
    ("graph_edges.csv", "edge_type"): EDGE_TYPES,
    ("transactions.csv", "txn_type"): TXN_TYPES,
    ("transactions.csv", "rail"): RAILS,
    ("transactions.csv", "channel"): CHANNELS,
    ("transactions.csv", "counterparty_type"): COUNTERPARTY_TYPES,
    ("transactions.csv", "currency"): {"KES"},
    ("loans.csv", "product_code"): {"DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "EMERGENCY_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE", "SALARY_ADVANCE"},
    ("loans.csv", "repayment_mode"): {"PAYROLL_CHECKOFF", "MANUAL_FOSA_TRANSFER", "MPESA_PAYBILL", "CASH_BRANCH"},
    ("loans.csv", "disbursement_channel"): {"FOSA_ACCOUNT", "MPESA_WALLET", "BANK_TRANSFER", "CASH_BRANCH"},
    ("loans.csv", "purpose_code"): {"SCHOOL_FEES", "BUSINESS_WORKING_CAPITAL", "ASSET_PURCHASE", "MEDICAL_EMERGENCY", "HOUSEHOLD_NEED", "AGRICULTURE_INPUTS", "DEVELOPMENT_PROJECT", "OTHER"},
    ("loans.csv", "performing_status"): {"CURRENT", "IN_ARREARS", "RESTRUCTURED", "DEFAULTED", "CLOSED", "WRITTEN_OFF"},
    ("guarantors.csv", "relationship_type"): {"COWORKER", "FAMILY", "FRIEND", "SACCO_MEMBER", "CHURCH_MEMBER", "BUSINESS_ASSOCIATE", "UNKNOWN"},
    ("alerts_truth.csv", "typology"): TYPOLOGIES,
    ("alerts_truth.csv", "entity_type"): ALERT_ENTITY_TYPES,
    ("alerts_truth.csv", "severity"): ALERT_SEVERITIES,
    ("alerts_truth.csv", "truth_label"): {True},
    ("alerts_truth.csv", "stage"): ALERT_STAGES,
    ("alerts_truth.csv", "explanation_code"): {
        "STRUCTURED_SUB_THRESHOLD_DEPOSITS",
        "RAPID_IN_OUT_MOVEMENT",
        "HIGH_EXIT_RATIO",
        "MULTIPLE_OUTBOUND_COUNTERPARTIES",
        "PRE_LOAN_AFFORDABILITY_BOOST",
        "SHARED_DEVICE_MULE_ACTIVITY",
        "RECIPROCAL_GUARANTEE_RING",
        "WALLET_FUNNEL_ACTIVITY",
        "SUSPICIOUS_PATTERN_SUMMARY",
    },
    ("institutions.csv", "archetype"): {"TEACHER_PUBLIC_SECTOR", "UNIFORMED_SERVICES", "UTILITY_PRIVATE_SECTOR", "COMMUNITY_CHURCH", "FARMER_COOPERATIVE", "SME_BIASHARA", "DIASPORA_FACING"},
    ("branches.csv", "branch_type"): {"HQ", "BRANCH", "AGENT_DESK"},
    ("employers.csv", "public_private"): {"PUBLIC", "PRIVATE"},
    ("employers.csv", "payroll_frequency"): {"MONTHLY", "BIWEEKLY", "WEEKLY"},
    ("devices.csv", "os_family"): {"ANDROID", "IOS", "FEATURE_PHONE"},
}

RECOMMENDED_ENUMS = {
    ("members.csv", "gender"): {"MALE", "FEMALE", "OTHER", "UNKNOWN"},
    ("members.csv", "urban_rural"): {"URBAN", "PERI_URBAN", "RURAL"},
}

ACCOUNT_PRODUCT_CODES = {
    "BOSA_DEPOSIT": {"BOSA_STANDARD"},
    "FOSA_SAVINGS": {"FOSA_SAVINGS_STANDARD"},
    "FOSA_CURRENT": {"FOSA_CURRENT_STANDARD"},
    "SHARE_CAPITAL": {"SHARE_CAPITAL_STANDARD"},
    "MPESA_WALLET": {"MPESA_WALLET"},
    "AIRTEL_WALLET": {"AIRTEL_WALLET"},
    "SOURCE_ACCOUNT": {"EXTERNAL_SOURCE"},
    "SINK_ACCOUNT": {"EXTERNAL_SINK"},
    "LOAN_ACCOUNT": {"DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "EMERGENCY_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE", "SALARY_ADVANCE"},
}


def validate_schema(rows_by_file: dict[str, list[dict[str, object]]], config: WorldConfig) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    for filename, columns in REQUIRED_COLUMNS.items():
        rows = rows_by_file.get(filename)
        if rows is None:
            if filename in OPTIONAL_FILES:
                continue
            findings.append(_error("schema.missing_file", f"Missing required file {filename}", filename))
            continue
        if rows:
            missing = [column for column in columns if column not in rows[0]]
            for column in missing:
                findings.append(_error("schema.missing_column", f"Missing required column {column}", filename))
        _validate_pk(filename, rows, findings)
        _validate_enums(filename, rows, findings)
        if filename == "accounts.csv":
            _validate_account_product_codes(rows, findings)
        _validate_dates(filename, rows, config, findings)
        if filename == "transactions.csv":
            _validate_transaction_amounts(rows, findings)
    return findings


def _validate_pk(filename: str, rows: list[dict[str, object]], findings: list[ValidationFinding]) -> None:
    pk = PRIMARY_KEYS[filename]
    patterns = {
        "members.csv": re.compile(r"^MEM\d{7}$"),
        "accounts.csv": re.compile(r"^ACC\d{8}$"),
        "nodes.csv": re.compile(r"^NODE\d{8}$"),
        "graph_edges.csv": re.compile(r"^EDGE\d{8}$"),
        "transactions.csv": re.compile(r"^TXN\d{12}$"),
        "loans.csv": re.compile(r"^LOAN\d{6}$"),
        "guarantors.csv": re.compile(r"^GUA\d{6}$"),
        "alerts_truth.csv": re.compile(r"^ALT\d{8}$"),
        "institutions.csv": re.compile(r"^INST\d{4}$"),
        "branches.csv": re.compile(r"^BRANCH\d{6}$"),
        "agents.csv": re.compile(r"^AGENT\d{6}$"),
        "employers.csv": re.compile(r"^EMPLOYER\d{6}$"),
        "devices.csv": re.compile(r"^DEVICE\d{6}$"),
    }
    pattern = patterns.get(filename)
    seen: set[str] = set()
    for row in rows:
        value = str(row.get(pk) or "")
        if not value:
            findings.append(_error("schema.pk_missing", f"Missing primary key {pk}", filename))
        elif value in seen:
            findings.append(_error("schema.pk_duplicate", f"Duplicate primary key {value}", filename, value))
        elif pattern and not pattern.fullmatch(value):
            findings.append(_error("schema.id_format_invalid", f"{pk}={value!r} does not match required format", filename, value))
        seen.add(value)


def _validate_enums(filename: str, rows: list[dict[str, object]], findings: list[ValidationFinding]) -> None:
    strict_rules = [(column, allowed) for (enum_file, column), allowed in STRICT_ENUMS.items() if enum_file == filename]
    recommended_rules = [(column, recommended) for (enum_file, column), recommended in RECOMMENDED_ENUMS.items() if enum_file == filename]
    if not strict_rules and not recommended_rules:
        return
    for row in rows:
        for column, allowed in strict_rules:
            if row.get(column) not in allowed:
                findings.append(_error("schema.enum_invalid", f"{column}={row.get(column)!r} is not allowed", filename, _row_id(row)))
        for column, recommended in recommended_rules:
            if row.get(column) not in recommended:
                findings.append(ValidationFinding("warning", "schema.recommended_value", f"{column}={row.get(column)!r} is outside recommended values", filename, _row_id(row)))


def _validate_dates(filename: str, rows: list[dict[str, object]], config: WorldConfig, findings: list[ValidationFinding]) -> None:
    start = date.fromisoformat(config.start_date)
    end = date.fromisoformat(config.end_date)
    date_columns = [
        column
        for column in (
            "join_date",
            "open_date",
            "opening_date",
            "active_from",
            "active_to",
            "first_seen",
            "last_seen",
            "start_date",
            "end_date",
            "application_date",
            "approval_date",
            "disbursement_date",
            "pledge_date",
            "release_date",
        )
        if rows and column in rows[0]
    ]
    for row in rows:
        for column in date_columns:
            value = row.get(column)
            if value in (None, ""):
                continue
            parsed = date.fromisoformat(str(value))
            if column in {"start_date", "end_date"}:
                continue
            if parsed > end:
                findings.append(_error("schema.date_out_of_window", f"{column}={value} after simulation end date", filename, _row_id(row)))
            if column == "open_date" and parsed < start:
                continue
    if rows and "timestamp" in rows[0]:
        for row in rows:
            value = row.get("timestamp")
            if value in (None, ""):
                findings.append(_error("schema.timestamp_missing", "timestamp is required", filename, _row_id(row)))
                continue
            parsed_dt = datetime.fromisoformat(str(value))
            if parsed_dt.utcoffset() is None:
                findings.append(_error("schema.timezone_missing", "timestamp must include Africa/Nairobi +03:00 offset", filename, _row_id(row)))
            elif parsed_dt.utcoffset().total_seconds() != 10_800:
                findings.append(_error("schema.timezone_invalid", "timestamp must use Africa/Nairobi +03:00 offset", filename, _row_id(row)))
            if not start <= parsed_dt.date() <= end:
                findings.append(_error("schema.date_out_of_window", f"timestamp={value} outside simulation window", filename, _row_id(row)))
    if rows and "created_at" in rows[0]:
        for row in rows:
            value = row.get("created_at")
            if value in (None, ""):
                continue
            parsed_dt = datetime.fromisoformat(str(value))
            if parsed_dt.utcoffset() is None:
                findings.append(_error("schema.timezone_missing", "created_at must include Africa/Nairobi +03:00 offset", filename, _row_id(row)))
    for timestamp_column in ("start_timestamp", "end_timestamp"):
        if rows and timestamp_column in rows[0]:
            for row in rows:
                value = row.get(timestamp_column)
                if value in (None, ""):
                    findings.append(_error("schema.timestamp_missing", f"{timestamp_column} is required", filename, _row_id(row)))
                    continue
                parsed_dt = datetime.fromisoformat(str(value))
                if parsed_dt.utcoffset() is None:
                    findings.append(_error("schema.timezone_missing", f"{timestamp_column} must include Africa/Nairobi +03:00 offset", filename, _row_id(row)))
                elif parsed_dt.utcoffset().total_seconds() != 10_800:
                    findings.append(_error("schema.timezone_invalid", f"{timestamp_column} must use Africa/Nairobi +03:00 offset", filename, _row_id(row)))
                if not start <= parsed_dt.date() <= end:
                    findings.append(_error("schema.date_out_of_window", f"{timestamp_column}={value} outside simulation window", filename, _row_id(row)))


def _validate_account_product_codes(rows: list[dict[str, object]], findings: list[ValidationFinding]) -> None:
    for row in rows:
        account_type = str(row.get("account_type") or "")
        product_code = str(row.get("product_code") or "")
        allowed = ACCOUNT_PRODUCT_CODES.get(account_type)
        if allowed and product_code not in allowed:
            findings.append(
                ValidationFinding(
                    "warning",
                    "schema.product_code_guidance",
                    f"product_code={product_code!r} is not recommended for account_type={account_type!r}",
                    "accounts.csv",
                    _row_id(row),
                )
            )


def _validate_transaction_amounts(rows: list[dict[str, object]], findings: list[ValidationFinding]) -> None:
    for row in rows:
        amount = float(row.get("amount_kes") or 0)
        fee = float(row.get("fee_kes") or 0)
        if str(row.get("txn_type")) in {"KYC_REFRESH", "ACCOUNT_REACTIVATION"}:
            if amount != 0 or fee != 0:
                findings.append(_error("schema.operational_event_amount", "Operational events must have zero amount and fee", "transactions.csv", _row_id(row)))
        elif amount <= 0:
            findings.append(_error("schema.amount_non_positive", "Financial transactions must have positive amount", "transactions.csv", _row_id(row)))
        if fee < 0:
            findings.append(_error("schema.fee_negative", "Transaction fee cannot be negative", "transactions.csv", _row_id(row)))


def _row_id(row: dict[str, object]) -> str | None:
    for key in ("alert_id", "member_id", "account_id", "node_id", "edge_id", "txn_id", "guarantee_id", "loan_id"):
        if row.get(key):
            return str(row[key])
    return None


def _error(code: str, message: str, filename: str | None = None, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, filename, row_id)
