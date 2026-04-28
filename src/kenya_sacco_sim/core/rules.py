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

DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG = {
    "window_days": 30,
    "min_members_per_device": 3,
    "min_device_txn_count": 6,
    "min_total_value_kes": 450_000,
    "min_outbound_share": 0.45,
    "digital_channels": ["MOBILE_APP", "USSD", "PAYBILL", "TILL", "BANK_TRANSFER"],
    "inbound_txn_types": ["PESALINK_IN", "MPESA_PAYBILL_IN", "BUSINESS_SETTLEMENT_IN"],
    "outbound_txn_types": ["PESALINK_OUT", "SUPPLIER_PAYMENT_OUT", "MPESA_WALLET_TOPUP"],
    "normal_shared_device_member_ceiling": 2,
}

GUARANTOR_FRAUD_RING_RULE_CONFIG = {
    "min_members_per_ring": 3,
    "max_members_per_ring": 6,
    "min_cycle_edges": 3,
    "active_loan_statuses": ["CURRENT", "IN_ARREARS", "DEFAULTED", "RESTRUCTURED"],
    "guaranteed_products": ["DEVELOPMENT_LOAN", "BIASHARA_LOAN", "ASSET_FINANCE"],
    "require_directed_cycle": True,
}

WALLET_FUNNELING_RULE_CONFIG = {
    "same_account_only": True,
    "window_days": 7,
    "dispersion_window_hours": 72,
    "inbound_txn_types": ["MPESA_PAYBILL_IN", "WALLET_P2P_IN", "BUSINESS_SETTLEMENT_IN"],
    "outbound_txn_types": ["MPESA_WALLET_TOPUP", "WALLET_P2P_OUT", "PESALINK_OUT", "SUPPLIER_PAYMENT_OUT"],
    "min_inbound_count": 6,
    "min_inbound_counterparties": 5,
    "min_inbound_value_kes": 350_000,
    "min_outbound_share": 0.55,
    "min_outbound_counterparties": 2,
    "digital_channels": ["MOBILE_APP", "USSD", "PAYBILL", "TILL", "BANK_TRANSFER"],
}

DORMANT_REACTIVATION_ABUSE_RULE_CONFIG = {
    "reactivation_txn_types": ["KYC_REFRESH", "ACCOUNT_REACTIVATION"],
    "inbound_txn_types": ["MPESA_PAYBILL_IN", "PESALINK_IN", "FOSA_CASH_DEPOSIT", "BUSINESS_SETTLEMENT_IN"],
    "outbound_txn_types": ["PESALINK_OUT", "SUPPLIER_PAYMENT_OUT", "MPESA_WALLET_TOPUP", "FOSA_CASH_WITHDRAWAL"],
    "window_days_after_reactivation": 7,
    "min_first_credit_kes": 120_000,
    "min_exit_ratio": 0.70,
    "min_outflow_count": 2,
    "min_outbound_counterparties": 2,
}

REMITTANCE_LAYERING_RULE_CONFIG = {
    "inbound_txn_types": ["PESALINK_IN"],
    "inbound_rails": ["REMITTANCE"],
    "outbound_txn_types": ["PESALINK_OUT", "SUPPLIER_PAYMENT_OUT", "MPESA_WALLET_TOPUP", "FOSA_CASH_WITHDRAWAL"],
    "window_hours": 72,
    "min_inbound_kes": 140_000,
    "min_exit_ratio": 0.68,
    "min_outflow_count": 3,
    "min_outbound_counterparties": 3,
}

CHURCH_CHARITY_MISUSE_RULE_CONFIG = {
    "candidate_personas": ["CHURCH_ORG", "CHAMA_GROUP"],
    "inbound_txn_types": ["PESALINK_IN", "MPESA_PAYBILL_IN"],
    "outbound_txn_types": ["PESALINK_OUT", "SUPPLIER_PAYMENT_OUT"],
    "window_hours": 96,
    "min_inbound_kes": 180_000,
    "min_exit_ratio": 0.55,
    "min_outflow_count": 3,
    "min_outbound_counterparties": 3,
}

RULE_CONFIGS = {
    "STRUCTURING": STRUCTURING_RULE_CONFIG,
    "RAPID_PASS_THROUGH": RAPID_PASS_THROUGH_RULE_CONFIG,
    "FAKE_AFFORDABILITY_BEFORE_LOAN": FAKE_AFFORDABILITY_RULE_CONFIG,
    "DEVICE_SHARING_MULE_NETWORK": DEVICE_SHARING_MULE_NETWORK_RULE_CONFIG,
    "GUARANTOR_FRAUD_RING": GUARANTOR_FRAUD_RING_RULE_CONFIG,
    "WALLET_FUNNELING": WALLET_FUNNELING_RULE_CONFIG,
    "DORMANT_REACTIVATION_ABUSE": DORMANT_REACTIVATION_ABUSE_RULE_CONFIG,
    "REMITTANCE_LAYERING": REMITTANCE_LAYERING_RULE_CONFIG,
    "CHURCH_CHARITY_MISUSE": CHURCH_CHARITY_MISUSE_RULE_CONFIG,
}
