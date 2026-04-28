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
  "guarantor_fraud_ring_validation": ...,
  "wallet_funneling_validation": ...,
  "near_miss_validation": ...,
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
| `label_validation` | Suspicious-member counts are within tolerance, pattern summaries exist, `pattern_labels.csv` and `edge_labels.csv` reconcile to truth/graph files, labels do not leak into feature files, and simple `txn_id` thresholds cannot recover labels. |
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
| `near_miss_validation` | Normal-but-suspicious-looking negative-control family counts plus transaction and guarantor coverage. |
| `fake_affordability_validation` | Pre-loan window and external-credit invariants are reported. |
| `device_sharing_mule_network_validation` | Shared-device mule candidate IDs, precision/recall, and misses are reported. |
| `guarantor_fraud_ring_validation` | Reciprocal guarantor-ring candidate IDs, precision/recall, and misses are reported. |
| `wallet_funneling_validation` | Wallet fan-in/fan-out candidate IDs, precision/recall, and misses are reported. |
| `benchmark_validation` | Whether benchmark artifacts form a valid evaluation or only a smoke run. |

`distribution_validation.dormant_lifecycle` reports dormant-member share,
active dormant share, dormant transactions without prior `KYC_REFRESH` and
`ACCOUNT_REACTIVATION`, and high-throughput unlabeled dormant members.
Benchmark runs fail if dormant share leaves the 5%-15% band, if active dormant
share exceeds 25%, if any dormant transaction appears before reactivation, or
if an unlabeled dormant member shows high-velocity reactivation behavior.

`benchmark_validation` also embeds confounder diagnostics from
`benchmark_confounder_diagnostics.json`. These do not look for explicit label
columns. They look for benchmark shortcuts such as suspicious labels clustering
in a narrow time window or being heavily predictable from persona/static
attributes.

The temporal concentration warning fires when a typology has at least 10
suspicious transactions and either:

```text
max_month_share > 0.40
window_span_days < 120
active_month_count < 10
```

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
`devices.last_seen` is also summarized so consumers can detect whether the
field is behaving as observed device usage or as a constant simulation-end
placeholder.

## Near-Miss Validation

Near-miss rows are normal feature-file transactions, not labels. They are
reported through `rule_results.json.near_miss_disclosure` and copied into
`validation_report.json.near_miss_validation`.

Current near-miss families include:

```text
legitimate_structuring_like
incomplete_structuring
legitimate_sme_liquidity_sweep
near_rapid_low_exit
church_family_bulk_payments
legitimate_preloan_affordability_candidate
near_affordability_low_growth
normal_shared_device_low_value
legitimate_two_member_reciprocal_guarantee
trusted_guarantor_star
legitimate_chama_wallet_collection
near_wallet_funnel_low_fanout
legitimate_dormant_reactivation_low_velocity
legitimate_remittance_family_distribution
legitimate_church_project_disbursement
```

Families marked `false_positive_pressure` are allowed to be rule candidates.
Families marked `negative_control` should look superficially similar while
staying below at least one rule threshold.

For `WALLET_FUNNELING`, `legitimate_chama_wallet_collection` is false-positive
pressure: normal chama, welfare, church, or project collections can have many
wallet/paybill payers and fast legitimate payouts. `near_wallet_funnel_low_fanout`
remains the wallet-funneling negative control.

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

The multi-seed report also summarizes full-feature ML F1, ablated ML F1,
ablation F1-drop stability, and per-seed confounder diagnostic flags. Those
numbers are diagnostic only; they should be read together with
`ml_leakage_ablation.json`, `benchmark_confounder_diagnostics.json`, and the
dataset card's rule-proxy-dependence section.

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
