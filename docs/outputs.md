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
| `devices.csv` | Device identity | Linked to a member. May belong to a `shared_device_group`. |

### Members And Accounts

| File | One row per | Notes |
| --- | --- | --- |
| `members.csv` | Member | Persona, KYC level, risk segment, county, declared income, and institution. |
| `accounts.csv` | Account | Member BOSA/FOSA/share/wallet/loan accounts plus institution source and sink accounts. |

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
| `guarantors.csv` | One row per guarantor pledge linking a guarantor member to a borrower loan. |

`--with-loans` also adds loan disbursement, repayment, interest, penalty, and
recovery transactions.

## With `--with-typologies`

| File | What it is |
| --- | --- |
| `alerts_truth.csv` | Ground-truth labels. This is the only CSV with typology labels. |
| `rule_results.json` | Deterministic rule baseline output with executable rule config, candidates, true positives, false positives, false negatives, candidate IDs, and `near_miss_disclosure`. |

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
| `benchmark_confounder_diagnostics.json` | Temporal and persona/static-attribute concentration diagnostics for ML benchmark interpretation. Temporal concentration uses `max_month_share > 0.40` or `window_span_days < 120`. |
| `feature_documentation.json` | Per-file feature dictionary and split guidance. |
| `dataset_card.md` | Human-readable run summary, intended use, near-miss coverage, metrics, and limitations. |
| `known_limitations.md` | Known shortcomings copied into the dataset for downstream readers. |

## Always Emitted Last

| File | What it is |
| --- | --- |
| `validation_report.json` | The verdict on the run. The top-level `errors` and `warnings` arrays drive the CLI exit code. |
| `manifest.json` | Run metadata, effective config, file list, deterministic `created_at`, and MD5 hashes. |

## From The `benchmark` Command

| File | What it is |
| --- | --- |
| `multi_seed_results.json` | Per-seed validation status, rule metrics, evaluation-validity status, and distribution stability statistics. |

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
                  └─ alerts_truth.csv (ground truth only)
accounts.csv ─────── transactions.csv (both ledger legs)
nodes.csv + graph_edges.csv  ←  projection of all of the above
```

Foreign-key validation enforces these relationships. A broken reference becomes
a validation error and the run exits `1`.
