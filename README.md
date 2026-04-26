# KENYA_SACCO_SIM

Synthetic AML dataset generator for Kenyan SACCO behavior.

The repository implements the frozen v0.1 specification through Milestone 4:
a deterministic SACCO world, reconciled payment and credit ledgers, and two
audit-grade suspicious typologies.

## Current Status

Implemented:

- Milestone 1 world generation: institutions, members, accounts, nodes, and graph edges
- Milestone 2 normal transaction engine with Kenya-like rails, cash/mobile behavior, seasonality, and ledger replay
- Milestone 3 credit system with loans, guarantors, loan accounts, repayments, arrears/default states, and graph links
- Milestone 4 suspicious typologies for `STRUCTURING` and `RAPID_PASS_THROUGH`
- Label output in `alerts_truth.csv` with no label columns leaked into feature files
- Rule reconstruction output in `rule_results.json`
- Near-miss disclosure metrics for unlabeled suspicious-looking behavior
- Validation for schema, foreign keys, balances, graph completeness, distributions, credit, guarantors, labels, clean AML baselines, and ID/reference leakage

Deferred by the frozen v0.1 spec:

- `FAKE_AFFORDABILITY_BEFORE_LOAN`
- Additional suspicious typologies beyond `STRUCTURING` and `RAPID_PASS_THROUGH`
- Device-sharing typologies

## Usage

From the repository root:

```bash
python3 -m kenya_sacco_sim generate --members 1000
```

Generate normal transactions:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-transactions
```

Generate the credit system:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-loans
```

Generate the full Milestone 4 package:

```bash
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --output ./datasets/KENYA_SACCO_SIM_m4_10k
```

If your environment has `python` mapped to Python 3, `python -m kenya_sacco_sim ...` is equivalent.

`--with-typologies` injects the v0.1 suspicious labels into the transaction world.
It does not imply `--with-loans`; pass both flags when you need credit outputs in
the same dataset.

## Outputs

Depending on selected options, the generator writes:

- `members.csv`
- `accounts.csv`
- `nodes.csv`
- `graph_edges.csv`
- `transactions.csv`
- `loans.csv`
- `guarantors.csv`
- `alerts_truth.csv`
- `rule_results.json`
- `validation_report.json`
- `manifest.json`

Default output directory:

```text
datasets/KENYA_SACCO_SIM_v0_1
```

## Validation

Basic code check:

```bash
python3 -m compileall src
```

Representative full-package validation:

```bash
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --output ./datasets/KENYA_SACCO_SIM_m4_10k
```

Latest verified 10,000-member Milestone 4 run:

```text
validation errors:   0
validation warnings: 0
members:             10,000
accounts:            40,969
transactions:        475,712
loans:               2,298
guarantors:          3,231
alerts_truth:        815
```

Milestone 4 audit targets from the latest verified run:

```text
STRUCTURING truth patterns:        50
STRUCTURING candidates:            61
STRUCTURING false positives:       11
RAPID_PASS_THROUGH truth patterns: 50
RAPID_PASS_THROUGH detected:       40
RAPID_PASS_THROUGH false positives: 10
RAPID_PASS_THROUGH false negatives: 10
near-miss transactions:            150
near-miss members:                 40
mirrored references:               0
```

The label validator also checks whether a simple transaction-ID threshold can
recover suspicious transactions. The latest verified 10,000-member run reports:

```text
suspicious txn_id percentile range: 0.3686-0.7855
best threshold precision:           0.0025
best threshold recall:              0.9818
```

This prevents the benchmark from leaking injection phase through late `txn_id`
or mirrored `reference` values.

## Development Discipline

Before committing changes:

- Run the smallest relevant generator command for the touched milestone.
- Run `python3 -m compileall src`.
- Run `git diff --check`.
- Update this README whenever behavior, commands, outputs, milestones, or validation expectations change.
- Do not commit unrelated local files.
