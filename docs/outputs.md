# Outputs

This page describes every file the generator writes. Files are grouped by the
flag that produces them.

## Always Emitted

These files appear on every `generate` run.

### Support Entities

| File | One row per | Notes |
| --- | --- | --- |
| `institutions.csv` | SACCO institution | Archetype, county, urban/rural segment, digital maturity, cash intensity, and guarantor intensity. |
| `branches.csv` | Branch | Branch type is `HQ`, `BRANCH`, or `AGENT_DESK`. |
| `agents.csv` | Cash or wallet agent | Agents belong to an institution and branch. |
| `employers.csv` | Payroll employer | Used by salary and checkoff transactions. |
| `devices.csv` | Device identity | Linked to a member. May belong to a `shared_device_group`; `last_seen` is derived from actual device transaction usage, not blindly set to simulation end. |

### Members And Accounts

| File | One row per | Notes |
| --- | --- | --- |
| `members.csv` | Member | Persona, KYC level, risk segment, county, declared income, and institution. Organization members have blank `age`; treat age as missing unless `member_type=INDIVIDUAL`. |
| `accounts.csv` | Account | Member BOSA/FOSA/share/wallet/loan accounts plus institution source and sink accounts. Filter `SOURCE_ACCOUNT` and `SINK_ACCOUNT` out of customer-account risk aggregation. |

### Graph Projection

| File | What it is |
| --- | --- |
| `nodes.csv` | One row per graph node. Node types include `INSTITUTION`, `MEMBER`, `ACCOUNT`, `WALLET`, `EMPLOYER`, `BRANCH`, `AGENT`, `DEVICE`, `SOURCE`, and `SINK`. |
| `graph_edges.csv` | One row per typed projection edge. Current edge families cover ownership, device use, employment, guarantees, institution/branch/account relationships, and source/sink funding projections. |

The tabular CSVs are authoritative. The graph files are analytical
projections over the same entities.

## With `--with-transactions`

| File | What it is |
| --- | --- |
| `transactions.csv` | Full ledger. Each row has a typed transaction, rail, channel, debit and credit accounts, amount, fee, timestamp, support entity IDs, device ID where required, and running balances. |

Balance reconciliation is enforced by validation.

## With `--with-loans`

| File | What it is |
| --- | --- |
| `loans.csv` | One row per loan: principal, rate, term, application/approval/disbursement dates, status, arrears, and linked loan account. |
| `guarantors.csv` | One row per guarantor pledge linking a guarantor member to a borrower loan. Normal guaranteed loans usually cover 60% through guarantors; remaining coverage is modeled as deposits or collateral. |

`--with-loans` also adds loan disbursement, repayment, interest, penalty, and
recovery transactions.

## With `--with-typologies`

| File | What it is |
| --- | --- |
| `alerts_truth.csv` | Detailed positive injected truth rows. It can contain PATTERN, MEMBER, ACCOUNT, TRANSACTION, and EDGE context rows for the same case. |
| `pattern_labels.csv` | One row per suspicious case keyed by `pattern_id`, with alert-row counts by granularity. Use this for unique case counts and split joins. |
| `edge_labels.csv` | Sparse graph-edge truth labels for typologies with graph-backed edge context, currently centered on guarantor-ring edge supervision. |
| `rule_results.json` | Deterministic rule baseline output with executable rule config, candidates, true positives, false positives, false negatives, candidate IDs, and `near_miss_disclosure` for transaction, device, guarantor, wallet-funnel, dormant-reactivation, remittance, and charity near-misses. |

The three label CSVs are positive injected truth only and must not be used as
feature files. A suspicious case can appear as several rows in
`alerts_truth.csv`; use `pattern_labels.csv` or aggregate by `pattern_id` when
counting unique cases.

## With `--with-benchmark`

These files require `--with-typologies`.

| File | What it is |
| --- | --- |
| `split_manifest.json` | Train, validation, and test assignments for members and patterns. The source of truth for splits. |
| `baseline_model_results.json` | Per-split rule-baseline precision, recall, and F1. Includes benchmark validity checks. |
| `ml_baseline_results.json` | Member-level Logistic Regression and Random Forest metrics by typology and split. |
| `feature_importance.json` | Logistic Regression coefficient rankings and Random Forest importances. |
| `ml_leakage_ablation.json` | ML baselines retrained with rule-proxy features removed; the dataset card summarizes the largest validation/test F1 drops. |
| `rule_vs_ml_comparison.json` | Descriptive rule-vs-ML precision, recall, and F1 deltas. This is not a superiority claim. |
| `benchmark_confounder_diagnostics.json` | Temporal and persona/static-attribute concentration diagnostics for ML benchmark interpretation. Temporal concentration uses `max_month_share > 0.40`, `window_span_days < 120`, or `active_month_count < 10`. |
| `feature_documentation.json` | Per-file feature dictionary and split guidance. |
| `dataset_card.md` | Human-readable run summary, intended use, near-miss coverage, metrics, and limitations. |
| `known_limitations.md` | Known shortcomings copied into the dataset for downstream readers. |

`feature_documentation.json` also marks static confounder columns such as
`persona_type`, `member_type`, `dormant_flag`, `age`, and `devices.last_seen`.
These fields are useful for audit and slicing, but should be held out or
stratified before claiming ML lift on organization-scoped or dormant-scoped
typologies.

When `--skip-ml-baseline` is passed, `ml_baseline_results.json`,
`feature_importance.json`, and `ml_leakage_ablation.json` are still emitted as
explicit skipped artifacts. Run `python3 -m kenya_sacco_sim ml-baseline --input
<dataset_dir>` later to generate the ML artifacts from the exported CSVs.

## Always Emitted Last

| File | What it is |
| --- | --- |
| `validation_report.json` | The verdict on the run. The top-level `errors` and `warnings` arrays drive the CLI exit code. |
| `manifest.json` | Run metadata, effective config, file list, deterministic `created_at`, and MD5 hashes. |

## From The `benchmark` Command

| File | What it is |
| --- | --- |
| `multi_seed_results.json` | Per-seed validation status, rule metrics, evaluation-validity status, and distribution stability statistics. |

Tracked benchmark summaries under `benchmarks/` record stability and scale
checks. Raw generated dataset folders are kept under `datasets/`, and stale raw
folders should be removed before publishing a refreshed package.

If `--write-seed-datasets` is passed, each seed's full `generate` output is
also written in a `seed_<seed>` subfolder.

## File Relationships

```text
institutions.csv ─┬─ branches.csv
                  ├─ agents.csv
                  ├─ employers.csv
                  └─ accounts.csv (source/sink rows per institution)
members.csv ──────┬─ accounts.csv (member-owned rows)
                  ├─ devices.csv
                  ├─ loans.csv ── guarantors.csv
                  └─ alerts_truth.csv + pattern_labels.csv (ground truth only)
accounts.csv ─────── transactions.csv (both ledger legs)
nodes.csv + graph_edges.csv  ←  projection of all of the above
graph_edges.csv ──── edge_labels.csv (sparse graph truth only)
```

Foreign-key validation enforces these relationships. A broken reference becomes
a validation error and the run exits `1`.
