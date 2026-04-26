# KENYA_SACCO_SIM

Synthetic AML benchmark generator for Kenyan SACCO behavior.

The current implementation is the v1 benchmark branch. It generates a
deterministic Kenyan SACCO financial world with members, accounts, loans,
guarantors, support entities, devices, graph projections, suspicious
typologies, rule baselines, ML baselines, leakage checks, and multi-seed
stability reports.

## Current Status

Implemented:

- World generation for institutions, branches, agents, employers, members,
  devices, accounts, nodes, and graph edges.
- Normal transaction engine with Kenya-like rails, cash/mobile behavior,
  church/org flows, school-fee seasonality, payday clustering, remittances, and
  ledger replay.
- Credit system with loans, guarantors, loan accounts, repayments,
  arrears/default states, and graph links.
- Suspicious typologies:
  - `STRUCTURING`
  - `RAPID_PASS_THROUGH`
  - `FAKE_AFFORDABILITY_BEFORE_LOAN`
  - `DEVICE_SHARING_MULE_NETWORK`
- Ground-truth labels in `alerts_truth.csv` with no label columns leaked into
  feature files.
- Deterministic rule reconstruction in `rule_results.json`.
- Member-level ML baseline using scikit-learn Logistic Regression and Random
  Forest models.
- Rule-proxy leakage ablation in `ml_leakage_ablation.json`.
- Rule-vs-ML comparison in `rule_vs_ml_comparison.json`.
- Benchmark split artifacts, dataset card, feature documentation, and known
  limitations.
- Multi-seed stability harness.
- Validation for schema, foreign keys, balances, distributions, credit,
  guarantors, support entities, devices, labels, typology metrics, benchmark
  validity, split leakage, ID/reference leakage, and device-sharing mule rules.

Current specification:

- `kenya_sacco_sim_v_1_specification.md`

Research source:

- `docs/research/deep-research-report.md`

Latest local output locations:

```text
datasets/KENYA_SACCO_SIM_v1_10k
benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

## Usage

From the repository root:

```bash
python3 -m kenya_sacco_sim generate --members 1000
```

Generate normal transactions and the credit system:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-loans
```

Generate the full benchmark package:

```bash
python3 -m kenya_sacco_sim generate \
  --members 10000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --output ./datasets/KENYA_SACCO_SIM_v1_10k
```

Run the multi-seed stability harness:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 10000 \
  --seeds 42 1337 2026 9001 314159 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

The harness writes `multi_seed_results.json` with per-seed validation status,
rule precision/recall, evaluation-validity status, and distribution stability
statistics. It fails if any seed has validation errors or if typology
precision/recall ranges exceed the stability threshold of `0.10`.

If your environment maps `python` to Python 3, `python -m kenya_sacco_sim ...`
is equivalent.

## CLI Notes

`--with-loans` implies `--with-transactions`.

`--with-typologies` injects suspicious labels into the transaction world. When
combined with `--with-loans`, the active set is:

```text
STRUCTURING
RAPID_PASS_THROUGH
FAKE_AFFORDABILITY_BEFORE_LOAN
DEVICE_SHARING_MULE_NETWORK
```

Sub-1,000-member smoke runs do not request partial device-sharing mule groups.
That typology is either generated in groups of at least three members or left at
zero for the run.

`--with-benchmark` emits benchmark artifacts, including deterministic rule
results, member-level ML baseline results, feature importances, rule-vs-ML
comparison, and leakage-ablation diagnostics. It requires `--with-typologies`.

`--config-dir` defaults to `./config`. Missing config files fall back to built-in
defaults, and CLI arguments override loaded config values.

## Outputs

Depending on selected options, the generator writes:

- `members.csv`
- `accounts.csv`
- `institutions.csv`
- `branches.csv`
- `agents.csv`
- `employers.csv`
- `devices.csv`
- `nodes.csv`
- `graph_edges.csv`
- `transactions.csv`
- `loans.csv`
- `guarantors.csv`
- `alerts_truth.csv`
- `rule_results.json`
- `split_manifest.json`
- `baseline_model_results.json`
- `ml_baseline_results.json`
- `feature_importance.json`
- `ml_leakage_ablation.json`
- `rule_vs_ml_comparison.json`
- `feature_documentation.json`
- `dataset_card.md`
- `known_limitations.md`
- `validation_report.json`
- `manifest.json`

Default generator output directory:

```text
datasets/KENYA_SACCO_SIM_v1
```

For current benchmark work, pass an explicit v1 output path:

```text
datasets/KENYA_SACCO_SIM_v1_10k
```

## Validation

Basic code check:

```bash
python3 -m compileall src tests
```

Automated tests:

```bash
python3 -m unittest discover -s tests
```

Representative full-package validation:

```bash
python3 -m kenya_sacco_sim generate \
  --members 10000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --output ./datasets/KENYA_SACCO_SIM_v1_10k
```

Validation includes:

```text
schema_validation
balance_validation
graph_validation
label_validation
loan_validation
guarantor_validation
credit_distribution_validation
support_entity_validation
device_validation
institution_archetype_metrics
clean_baseline_aml_metrics
distribution_validation
typology_validation
typology_runtime_metrics
fake_affordability_validation
device_sharing_mule_network_validation
benchmark_validation
```

Benchmark validity is explicit in `split_manifest.json` under
`checks.evaluation_validity`. A valid benchmark evaluation requires:

```text
member_count >= 10,000
suspicious_member_count >= 100
typology_member_count >= 30 for each active typology
positive labels per split >= 5 for each active typology
patterns per split >= 5 for each active typology
labeled transactions per typology split >= 10
```

Smaller runs are marked `smoke_only`; full-size runs that miss these thresholds
fail benchmark validation.

## Latest Verified Gate

Latest verified 10,000-member benchmark run:

```text
command: python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_v1_10k
manifest version:    1.0.0-dev
validation errors:   0
validation warnings: 0
members:             10,000
accounts:            41,003
transactions:        511,026
loans:               2,352
guarantors:          3,372
alerts_truth:        796
devices:             10,000
device coverage:     100.00% of digital transactions
shared devices:      343
max members/device:  5
evaluation validity: valid
```

Rule-baseline metrics from that run:

```text
DEVICE_SHARING_MULE_NETWORK precision: 0.9091 / recall: 1.0000
FAKE_AFFORDABILITY precision:          0.2222 / recall: 1.0000
RAPID_PASS_THROUGH precision:          0.6000 / recall: 0.8000
STRUCTURING precision:                 0.7500 / recall: 1.0000
```

Latest multi-seed stability gate:

```text
command: python3 -m kenya_sacco_sim benchmark --members 10000 --seeds 42 1337 2026 9001 314159 --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
validation error free: true
precision/recall variance within threshold: true
evaluation validity: valid for all seeds
digital device coverage mean: 1.0000
shared-device member share mean: 0.0434
cash rail share mean: 0.1939
loan active member mean: 0.2385
arrears share mean: 0.0927
```

Known benchmark behavior:

```text
FAKE_AFFORDABILITY_BEFORE_LOAN intentionally has low rule precision.
Normal borrowers may receive legitimate large pre-loan inflows, so false
positives are expected and make the benchmark less cartoon-clean.
100-member runs are smoke tests only, not valid benchmark evaluations.
```

## Development Discipline

Before committing changes:

- Check the current spec before starting a new milestone.
- Run the smallest relevant generator command for the touched milestone.
- Run `python3 -m compileall src tests`.
- Run `python3 -m unittest discover -s tests` when behavior or validation changes.
- Run `git diff --check`.
- Update the README and current docs whenever behavior, commands, outputs,
  milestones, or validation expectations change.
- Keep only the latest generated output artifacts in visible dataset/benchmark
  folders.
- Do not commit unrelated local files.
