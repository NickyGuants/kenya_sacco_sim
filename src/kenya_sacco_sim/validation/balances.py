from __future__ import annotations

from kenya_sacco_sim.core.models import ValidationFinding


def validate_balances(rows_by_file: dict[str, list[dict[str, object]]]) -> list[ValidationFinding]:
    transactions = rows_by_file.get("transactions.csv")
    if not transactions:
        return []
    findings: list[ValidationFinding] = []
    accounts = {str(row["account_id"]): row for row in rows_by_file.get("accounts.csv", [])}
    balances = {account_id: float(row["opening_balance_kes"]) for account_id, row in accounts.items()}

    for txn in sorted(transactions, key=lambda row: (str(row["timestamp"]), str(row["txn_id"]))):
        txn_id = str(txn["txn_id"])
        amount = float(txn["amount_kes"])
        debit_id = str(txn["account_id_dr"] or "")
        credit_id = str(txn["account_id_cr"] or "")
        if not debit_id and not credit_id:
            findings.append(_error("balance.double_entry_missing", "Transaction must have at least one debit or credit account", txn_id))
            continue
        if debit_id:
            if debit_id not in balances:
                findings.append(_error("balance.debit_account_missing", f"Debit account {debit_id} not found", txn_id))
            else:
                _apply_movement(balances, accounts, debit_id, "dr", amount)
                if round(balances[debit_id], 2) != round(float(txn["balance_after_dr_kes"]), 2):
                    findings.append(_error("balance.after_debit_mismatch", "balance_after_dr_kes does not match ledger state", txn_id))
                if accounts[debit_id]["account_type"] not in {"SOURCE_ACCOUNT"} and balances[debit_id] < -0.005:
                    findings.append(_error("balance.negative_customer_account", f"Account {debit_id} went negative", txn_id))
        if credit_id:
            if credit_id not in balances:
                findings.append(_error("balance.credit_account_missing", f"Credit account {credit_id} not found", txn_id))
            else:
                _apply_movement(balances, accounts, credit_id, "cr", amount)
                if round(balances[credit_id], 2) != round(float(txn["balance_after_cr_kes"]), 2):
                    findings.append(_error("balance.after_credit_mismatch", "balance_after_cr_kes does not match ledger state", txn_id))

    for account_id, calculated in balances.items():
        exported = round(float(accounts[account_id]["current_balance_kes"]), 2)
        if round(calculated, 2) != exported:
            findings.append(_error("balance.current_balance_mismatch", f"Exported current balance mismatch for {account_id}", account_id))
    return findings


def _apply_movement(balances: dict[str, float], accounts: dict[str, dict[str, object]], account_id: str, side: str, amount: float) -> None:
    if accounts[account_id]["account_type"] == "LOAN_ACCOUNT":
        balances[account_id] += amount if side == "dr" else -amount
    else:
        balances[account_id] += -amount if side == "dr" else amount


def _error(code: str, message: str, row_id: str | None = None) -> ValidationFinding:
    return ValidationFinding("error", code, message, "transactions.csv", row_id)
