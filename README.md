# KENYA_SACCO_SIM

Synthetic AML dataset generator for Kenyan SACCO behavior.

The repository implements the frozen v0.1 specification through Milestone 5:
a deterministic SACCO world, reconciled payment and credit ledgers, two
audit-grade suspicious typologies, and benchmark release artifacts.

## Current Status

Implemented:

- Milestone 1 world generation: institutions, members, accounts, nodes, and graph edges
- Milestone 2 normal transaction engine with Kenya-like rails, cash/mobile behavior, seasonality, and ledger replay
- Milestone 3 credit system with loans, guarantors, loan accounts, repayments, arrears/default states, and graph links
- Milestone 4 suspicious typologies for `STRUCTURING` and `RAPID_PASS_THROUGH`
- Milestone 5 benchmark artifacts: deterministic splits, baseline results, feature documentation, dataset card, and known limitations
- Normal `CHURCH_ORG` behavior with Sunday collections, M-Pesa/cash inflows, donor receipts, rent/vendor/charity outflows, and validation gates
- Label output in `alerts_truth.csv` with no label columns leaked into feature files
- Rule reconstruction output in `rule_results.json`
- Near-miss disclosure metrics for unlabeled suspicious-looking behavior
- Deterministic `manifest.json` metadata for reproducible seed/config reruns
- Validation for schema, foreign keys, balances, graph completeness, distributions, credit, guarantors, labels, clean AML baselines, benchmark splits, and ID/reference leakage

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

Generate the full Milestone 5 benchmark package:

```bash
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_m5_10k
```

If your environment has `python` mapped to Python 3, `python -m kenya_sacco_sim ...` is equivalent.

`--with-typologies` injects the v0.1 suspicious labels into the transaction world.
It does not imply `--with-loans`; pass both flags when you need credit outputs in
the same dataset.

`--with-benchmark` emits Milestone 5 benchmark artifacts and requires
`--with-typologies`.

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
- `split_manifest.json`
- `baseline_model_results.json`
- `feature_documentation.json`
- `dataset_card.md`
- `known_limitations.md`
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
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_m5_10k
```

Latest verified 10,000-member Milestone 5 run:

```text
validation errors:   0
validation warnings: 0
members:             10,000
accounts:            40,969
transactions:        516,464
loans:               2,298
guarantors:          3,231
alerts_truth:        844
```

Milestone 5 rule-baseline metrics from the latest verified run:

```text
STRUCTURING truth patterns:        50
STRUCTURING candidates:            63
STRUCTURING false positives:       13
STRUCTURING recall:                1.0000
RAPID_PASS_THROUGH truth patterns: 50
RAPID_PASS_THROUGH detected:       40
RAPID_PASS_THROUGH false positives: 11
RAPID_PASS_THROUGH false negatives: 10
RAPID_PASS_THROUGH recall:         0.8000
macro precision:                   0.7890
macro recall:                      0.9000
near-miss transactions:            157
near-miss members:                 42
mirrored references:               0
```

Milestone 5 split and leakage checks:

```text
member split leakage count:         0
pattern split leakage count:        0
unassigned member references:       0
unassigned patterns:                0
split counts, members:              train 6,992 / validation 1,496 / test 1,512
split counts, transactions:         train 361,884 / validation 77,821 / test 76,759
CHURCH_ORG active share:            1.0000
CHURCH_ORG median txns/year:        84.00
```

`feature_documentation.json` points consumers to `split_manifest.json` as the
source of truth for splits and records the relevant member or pattern key per
file. It also exposes explicit file roles so label files are not inferred from
feature-file metadata. `manifest.json` uses deterministic simulation metadata
instead of wall clock time so identical seed/config runs are easier to compare.
Suspicious-member targets are generated and validated with half-up count
rounding plus a discrete count tolerance, avoiding Python banker-rounding edge
cases in small runs.

The label validator also checks whether a simple transaction-ID threshold can
recover suspicious transactions. The latest verified 10,000-member run reports:

```text
suspicious txn_id percentile range: 0.3685-0.7858
best threshold precision:           0.0024
best threshold recall:              0.9825
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
