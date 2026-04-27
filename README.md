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
  - `GUARANTOR_FRAUD_RING`
  - `WALLET_FUNNELING`
  - `DORMANT_REACTIVATION_ABUSE`
  - `REMITTANCE_LAYERING`
  - `CHURCH_CHARITY_MISUSE`
- Twelve-persona world model including formal workers, SME owners, micro
  traders, farmers, diaspora-supported members, boda operators, chama groups,
  church/org accounts, and SACCO staff.
- Dormant lifecycle semantics: dormant members start inactive, require
  `KYC_REFRESH` and `ACCOUNT_REACTIVATION` before renewed activity, and are
  validated for dormant-throughput abuse.
- Rich near-miss and negative-control families for active typologies, reported
  in `rule_results.json`, `validation_report.json`, and `dataset_card.md`.
- Ground-truth labels in `alerts_truth.csv` with no label columns leaked into
  feature files.
- Deterministic rule reconstruction in `rule_results.json`.
- Member-level ML baseline using scikit-learn Logistic Regression and Random
  Forest models.
- Rule-proxy leakage ablation in `ml_leakage_ablation.json`.
- Descriptive rule-vs-ML comparison in `rule_vs_ml_comparison.json`.
- Temporal/persona shortcut diagnostics in `benchmark_confounder_diagnostics.json`.
- Benchmark split artifacts, dataset card, feature documentation, and known
  limitations.
- Multi-seed stability harness.
- Validation for schema, foreign keys, balances, distributions, credit,
  guarantors, support entities, devices, labels, typology metrics, benchmark
  validity, split leakage, ID/reference leakage, device-sharing mule rules, and
  wallet-funneling rules.

Current specification:

- `kenya_sacco_sim_v_1_specification.md`

Research source:

- `docs/research/deep-research-report.md`
- `docs/research/current-calibration-notes.md`

Latest local output locations:

```text
datasets/KENYA_SACCO_SIM_v1_100k
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
  --members 100000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --suspicious-ratio 0.015 \
  --output ./datasets/KENYA_SACCO_SIM_v1_100k
```

Run the multi-seed stability harness:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 10000 \
  --seeds 42 1337 2026 9001 314159 \
  --jobs 4 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

The harness writes `multi_seed_results.json` with per-seed validation status,
rule precision/recall, evaluation-validity status, and distribution stability
statistics. It fails if any seed has validation errors or if typology
precision/recall ranges exceed the stability threshold of `0.10`.
Seed runs execute in parallel by default with up to four worker processes. Use
`--jobs 1` for serial debugging or pass a larger `--jobs` value when the host
has enough CPU and memory headroom.

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
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
DORMANT_REACTIVATION_ABUSE
REMITTANCE_LAYERING
CHURCH_CHARITY_MISUSE
```

Sub-1,000-member smoke runs do not request partial device-sharing mule groups.
That typology is either generated in groups of at least three members or left at
zero for the run.

`--with-benchmark` emits benchmark artifacts, including deterministic rule
results, member-level ML baseline results, feature importances, rule-vs-ML
comparison, and leakage-ablation diagnostics. It requires `--with-typologies`.

For larger generated packages, use `--skip-ml-baseline` with `--with-benchmark`
to emit rule, split, leakage, and dataset-card artifacts without training
sklearn models during generation. Run ML later from the generated CSV package:

```bash
python3 -m kenya_sacco_sim ml-baseline \
  --input ./datasets/KENYA_SACCO_SIM_v1_100k
```

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
- `benchmark_confounder_diagnostics.json`
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
datasets/KENYA_SACCO_SIM_v1_100k
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
  --members 100000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --suspicious-ratio 0.015 \
  --output ./datasets/KENYA_SACCO_SIM_v1_100k
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
near_miss_validation
fake_affordability_validation
device_sharing_mule_network_validation
guarantor_fraud_ring_validation
wallet_funneling_validation
dormant_lifecycle metrics
benchmark_validation
```

`benchmark_confounder_diagnostics.json` reports two ML-specific risks that are
not direct label leakage:

```text
temporal concentration of suspicious labels
persona/static-attribute concentration of suspicious labels
```

If either review flag is true, `rule_vs_ml_comparison.json` must be read only
as a descriptive score table, not as evidence that ML outperforms deterministic
rules.

The temporal review flag uses a deliberately conservative month-concentration
threshold:

```text
max_month_share > 0.40
window_span_days < 120
active_month_count < 10
```

This is intended to catch typologies that cluster just below an obvious
single-month majority, have too short a calendar span, or are absent from too
many simulation months.

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

## Current Release Gate

Current target package:

```text
command: python3 -m kenya_sacco_sim generate --members 100000 --with-loans --with-typologies --with-benchmark --skip-ml-baseline --suspicious-ratio 0.015 --output ./datasets/KENYA_SACCO_SIM_v1_100k
manifest version:    1.0.0-dev
validation target:   0 errors / 0 warnings
active typologies:   9
personas:            12
ML baseline:         skipped during generation; run downstream with ml-baseline
transactions:        5,305,344
total CSV rows:      10,196,191
```

Latest multi-seed stability gate:

```text
command: python3 -m kenya_sacco_sim benchmark --members 10000 --seeds 42 1337 2026 9001 314159 --jobs 4 --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
validation error free: true
precision/recall variance within threshold: true
evaluation validity: valid for all seeds
digital device coverage mean: 1.0000
shared-device member share mean: 0.0429
cash rail share mean: 0.1935
loan active member mean: 0.2385
arrears share mean: 0.0927
near-miss member count mean: 213.8
near-miss transaction count mean: 860.2
near-miss guarantee count mean: 18.8
wall clock: 89.0s on this 11-CPU local machine with --jobs 4
first four seeds completed in ~49-50s; final queued seed completed at 88.6s
single 10k package wall clock: 44.4s
```

The benchmark runner now caps parallel workers by both CPU count and an
estimated memory budget. On this local 11-CPU, ~18 GB host, 10k runs use four
workers.

Latest local scale probe:

```text
100k benchmark no ML:  5,305,344 transactions / 10,196,191 total CSV rows / 719.9s
active personas:       12
active typologies:     9
validation result:     0 errors / 0 warnings
ledger replay:         0 mismatches
scale artifact:        ./benchmarks/KENYA_SACCO_SIM_scale_probe_results.json
```

The current package is the release-scale no-ML generation path and preserves
zero validation errors and zero warnings. Full in-generation ML remains
decoupled: use `--skip-ml-baseline` for generation and run the standalone
`ml-baseline` command afterward when model artifacts are needed.

Known benchmark behavior:

```text
FAKE_AFFORDABILITY_BEFORE_LOAN intentionally has low rule precision.
Normal borrowers may receive legitimate large pre-loan inflows, so false
positives are expected and make the benchmark less cartoon-clean.
Near-miss families intentionally create legitimate behavior that can pressure
typology rules without appearing in alerts_truth.csv.
Rule-vs-ML comparison is descriptive. Use ablation and confounder diagnostics
before making ML superiority claims.
ML outperformance on direct rule-proxy features is not treated as benchmark
evidence unless the ablated feature set and multi-seed diagnostics support it.
100-member runs are smoke tests only, not valid benchmark evaluations.
100,000-member generation is validated through the no-ML benchmark path. Full
in-generation ML at that scale remains intentionally decoupled.
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
