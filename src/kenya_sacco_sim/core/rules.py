from __future__ import annotations


STRUCTURING_RULE_CONFIG = {
    "same_account_only": False,
    "inbound_txn_types": ["FOSA_CASH_DEPOSIT", "BUSINESS_SETTLEMENT_IN", "MPESA_PAYBILL_IN", "PESALINK_IN"],
    "excluded_txn_types": [],
    "window_days": 7,
    "min_deposit_count": 5,
    "max_counted_deposit_amount_kes": 100_000,
    "min_total_deposit_kes": 300_000,
    "count_only_sub_threshold_deposits": True,
}

RAPID_PASS_THROUGH_RULE_CONFIG = {
    "same_account_only": True,
    "inbound_txn_types": ["PESALINK_IN", "MPESA_PAYBILL_IN", "BUSINESS_SETTLEMENT_IN"],
    "outbound_txn_types": ["PESALINK_OUT", "SUPPLIER_PAYMENT_OUT"],
    "excluded_txn_types": ["LOAN_REPAYMENT", "CHECKOFF_LOAN_RECOVERY", "HOUSEHOLD_SPEND_OUT", "BOSA_DEP_TOPUP", "MPESA_WALLET_TOPUP"],
    "window_hours": 48,
    "min_inbound": 100_000,
    "min_exit_ratio": 0.75,
    "max_retained_balance_ratio": 0.25,
    "min_counterparties": 2,
    "include_loan_repayment": False,
    "include_household_spend": False,
    "include_supplier_payment": True,
}

FAKE_AFFORDABILITY_RULE_CONFIG = {
    "lookback_days": 30,
    "inbound_txn_types": ["PESALINK_IN", "MPESA_PAYBILL_IN", "FOSA_CASH_DEPOSIT", "BUSINESS_SETTLEMENT_IN"],
    "excluded_stable_income_types": ["SALARY_IN", "CHECKOFF_DEPOSIT", "LOAN_DISBURSEMENT"],
    "min_external_credit_share": 0.55,
    "min_balance_growth_kes": 50_000,
    "eligible_loan_products": ["DEVELOPMENT_LOAN", "SCHOOL_FEES_LOAN", "BIASHARA_LOAN"],
}

RULE_CONFIGS = {
    "STRUCTURING": STRUCTURING_RULE_CONFIG,
    "RAPID_PASS_THROUGH": RAPID_PASS_THROUGH_RULE_CONFIG,
    "FAKE_AFFORDABILITY_BEFORE_LOAN": FAKE_AFFORDABILITY_RULE_CONFIG,
}
