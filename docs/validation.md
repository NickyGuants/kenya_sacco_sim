# Validation

Every `generate` run writes `validation_report.json`. The report is the verdict
on whether the dataset is usable. The CLI exits `1` if the report has any
errors. Warnings do not fail the run but should be reviewed.

## Top-Level Shape

```text
{
  "schema_validation": ...,
  "row_counts": {...},
  "balance_validation": ...,
  "graph_validation": ...,
  "label_validation": ...,
  "loan_validation": ...,
  "guarantor_validation": ...,
  "credit_distribution_validation": ...,
  "support_entity_validation": ...,
  "device_validation": ...,
  "institution_archetype_metrics": ...,
  "clean_baseline_aml_metrics": ...,
  "distribution_validation": ...,
  "typology_validation": ...,
  "typology_runtime_metrics": ...,
  "fake_affordability_validation": ...,
  "device_sharing_mule_network_validation": ...,
  "benchmark_validation": ...,
  "errors": [...],
  "warnings": [...]
}
```

Each section has its own `status` plus the metrics that drove the verdict. The
`errors` and `warnings` arrays are flat lists of every finding, with `code`,
`message`, and file or row context when available.

## What Each Section Checks

| Section | What it asserts |
| --- | --- |
| `schema_validation` | Required files/columns exist, primary keys are unique, strict enums are valid, product-code mappings are coherent, date windows are valid, and transaction signs are valid. |
| `row_counts` | Sanity counts for every emitted file. |
| `balance_validation` | Debit and credit legs reconcile and final balances replay from the ledger. |
| `graph_validation` | Nodes and graph edges resolve, and infrastructure entities are projected into the graph. |
| `label_validation` | Suspicious-member counts are within tolerance, pattern summaries exist, labels do not leak into feature files, and simple `txn_id` thresholds cannot recover labels. |
| `loan_validation` | Loan dates, principal, status, arrears, disbursement, and repayments are coherent. |
| `guarantor_validation` | Guarantor pledges point at real loans and members, with bounded concentration. |
| `credit_distribution_validation` | Loan portfolio distribution, arrears, defaults, and repayment behavior stay in expected ranges. |
| `support_entity_validation` | Institutions, branches, agents, employers, and devices have valid keys and relationships. |
| `device_validation` | Digital transactions carry required device IDs, devices resolve, and shared-device groups explain multi-member device usage. |
| `institution_archetype_metrics` | Per-archetype digital, cash, and guarantor behavior is reported. |
| `clean_baseline_aml_metrics` | Clean baseline suspicious-looking candidates are counted for context. |
| `distribution_validation` | Persona, rural/urban, wallet, rail, transaction mix, and seasonality metrics are checked. |
| `typology_validation` | Injected typology counts match target policy and active typology requirements. |
| `typology_runtime_metrics` | Rule-baseline precision and recall against truth labels. |
| `fake_affordability_validation` | Pre-loan window and external-credit invariants are reported. |
| `device_sharing_mule_network_validation` | Shared-device mule candidate IDs, precision/recall, and misses are reported. |
| `benchmark_validation` | Whether benchmark artifacts form a valid evaluation or only a smoke run. |

`benchmark_validation` also embeds confounder diagnostics from
`benchmark_confounder_diagnostics.json`. These do not look for explicit label
columns. They look for benchmark shortcuts such as suspicious labels clustering
in a narrow time window or being heavily predictable from persona/static
attributes.

## Device Validation

Digital channels requiring `device_id`:

```text
MOBILE_APP
USSD
PAYBILL
TILL
BANK_TRANSFER
```

Important metrics:

```text
digital_transaction_count
device_required_transaction_count
device_required_missing_device_id_count
unresolved_transaction_device_id_count
unresolved_transaction_device_id_distinct_count
devices_used_by_multiple_members_count
max_members_per_device
shared_device_group_missing_count
shared_device_unexplained_member_count
shared_device_member_share
```

Required digital device coverage is `100%`.

## Benchmark Validity

A run is a valid benchmark only if all of these hold:

```text
member_count >= 10,000
suspicious_member_count >= 100
typology_member_count >= 30 for each active typology
positive labels per split >= 5 for each active typology
patterns per split >= 5 for each active typology
labeled transactions per typology split >= 10
```

Smaller runs are `smoke_only`. Do not publish model scores from `smoke_only`
runs.

## Multi-Seed Stability

The `benchmark` subcommand runs the full pipeline across multiple seeds and
writes `multi_seed_results.json`.

The acceptance block has two key booleans:

- `validation_error_free` - every seed produced zero validation errors.
- `precision_recall_variance_within_threshold` - every typology's precision and
  recall range across seeds is at or below `0.10`.

The current five-seed gate also reports stability for:

```text
cash_rail_share
digital_device_coverage
shared_device_member_share
loan_active_member_ratio
arrears_share
default_share
```

The multi-seed report also summarizes full-feature and ablated ML F1 stability.
Those numbers are diagnostic only; they should be read together with
`ml_leakage_ablation.json` and `benchmark_confounder_diagnostics.json`.

## Reading Errors And Warnings

Each finding looks like:

```json
{
  "severity": "error",
  "code": "BALANCE_MISMATCH",
  "message": "Account ACC00000001 closes at -120 KES",
  "file": "transactions.csv",
  "row_id": "TXN000000000001"
}
```

Use the `code` to find the validator under `src/kenya_sacco_sim/validation/`.
The validators are intentionally small and readable.

## Discipline

Do not relax validation just to make a run pass. If a tolerance needs loosening,
first assume the generator or the documentation is wrong and investigate that
path.
