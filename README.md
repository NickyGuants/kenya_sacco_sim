# KENYA_SACCO_SIM

Synthetic AML dataset generator for Kenyan SACCO behavior.

The repository now carries the frozen v0.1 release-candidate spec plus the
v0.2 foundation implementation from the deep research blueprint.

## Current Status

Implemented v0.1:

- Milestone 1 world generation: institutions, members, accounts, nodes, and graph edges
- Milestone 2 normal transaction engine with Kenya-like rails, cash/mobile behavior, seasonality, and ledger replay
- Milestone 3 credit system with loans, guarantors, loan accounts, repayments, arrears/default states, and graph links
- Milestone 4 suspicious typologies for `STRUCTURING` and `RAPID_PASS_THROUGH`
- Milestone 5 benchmark artifacts: deterministic splits, baseline results, feature documentation, dataset card, and known limitations
- Normal `CHURCH_ORG` behavior with Sunday collections, M-Pesa/cash inflows, donor receipts, rent/vendor/charity outflows, and validation gates
- Label output in `alerts_truth.csv` with no label columns leaked into feature files
- Standalone deterministic baseline rules in `src/kenya_sacco_sim/benchmark/baseline_rules.py`
- Rule reconstruction output in `rule_results.json`
- Near-miss disclosure metrics for unlabeled suspicious-looking behavior
- Deterministic `manifest.json` metadata for reproducible seed/config reruns
- Validation for schema, foreign keys, balances, graph completeness, distributions, credit, guarantors, labels, clean AML baselines, benchmark splits, and ID/reference leakage; benchmark leakage failures affect validation errors

Implemented v0.2 foundation:

- Engineer-ready v0.2 spec: `kenya_sacco_sim_v_0_2_specification.md`
- v1 backlog: `kenya_sacco_sim_v_1_backlog.md`
- Full research blueprint preserved at `docs/research/deep-research-report.md`
- External YAML config files in `config/` with current defaults and CLI override support
- Support entity exports: `institutions.csv`, `branches.csv`, `agents.csv`, `employers.csv`, `devices.csv`
- Institution archetypes with digital maturity, cash intensity, and guarantor intensity
- Device baseline population, `DEVICE` nodes, `USES_DEVICE` graph edges, and device validation metrics
- New suspicious typology and baseline rule: `FAKE_AFFORDABILITY_BEFORE_LOAN`
- Fake-affordability injection respects configured simulation date windows
- Member-level ML benchmark baseline using scikit-learn Logistic Regression and Random Forest models

Deferred to v1:

- Guarantor fraud rings
- Wallet funneling
- Dormant reactivation abuse
- Remittance layering
- Device-sharing typologies
- Graph neural network benchmark and 100,000+ member scale

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
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_v02_10k
```

Run the v0.2 multi-seed stability harness:

```bash
python3 -m kenya_sacco_sim benchmark --members 10000 --seeds 42 1337 2026 9001 314159 --output ./benchmarks/v02_multi_seed
```

The harness writes `multi_seed_results.json` with per-seed validation status,
baseline precision/recall, and distribution stability statistics. It fails if
any seed has validation errors or if typology precision/recall ranges exceed
the v0.2 stability threshold of `0.10`. Seed lists must be non-empty and unique;
duplicate seeds are rejected so optional per-seed dataset exports cannot
overwrite each other.

If your environment has `python` mapped to Python 3, `python -m kenya_sacco_sim ...` is equivalent.

`--with-typologies` injects suspicious labels into the transaction world. When
combined with `--with-loans`, v0.2 also injects
`FAKE_AFFORDABILITY_BEFORE_LOAN`; without loans, typology generation falls back
to the v0.1 structuring and rapid-pass-through set.

`--with-benchmark` emits benchmark artifacts, including deterministic rule
results and member-level ML baseline results. It requires `--with-typologies`.
The ML baseline uses `scikit-learn`, which is declared in `pyproject.toml`.

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
- `feature_documentation.json`
- `dataset_card.md`
- `known_limitations.md`
- `validation_report.json`
- `manifest.json`

Default output directory:

```text
datasets/KENYA_SACCO_SIM_v0_2
```

## Validation

Basic code check:

```bash
python3 -m compileall src
```

Focused automated tests:

```bash
python3 -m unittest discover -s tests
```

Representative full-package validation:

```bash
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_v02_10k
```

v0.2 validation adds:

```text
support_entity_validation
device_validation
institution_archetype_metrics
fake_affordability_validation
```

Support entity validation rejects institution archetype parameters outside
`0.0-1.0` for `digital_maturity`, `cash_intensity`, and
`loan_guarantor_intensity`.

`device_validation` distinguishes:

```text
digital_transaction_count
device_required_transaction_count
device_required_missing_device_id_count
device_exempt_transaction_count
unresolved_transaction_device_id_count
unresolved_transaction_device_id_distinct_count
devices_used_by_multiple_members_count
max_members_per_device
shared_device_group_missing_count
shared_device_unexplained_member_count
```

Benchmark validation also reports institution split drift:

```text
institution_split_max_share
institution_split_max_institution_id
institution_split_max_split
institution_split_drift_warning
```

Benchmark outputs also include:

```text
baseline_model_results.json   deterministic rule baseline metrics
ml_baseline_results.json      member-level one-vs-rest ML metrics
feature_importance.json       Logistic Regression coefficients and Random Forest importances
```

The ML baseline builds one feature row per member from exported feature files
and uses labels only from `alerts_truth.csv` as targets. Raw identifiers and
label-bearing fields such as `member_id`, `txn_id`, `reference`, `pattern_id`,
`alert_id`, `account_id`, `device_id`, `node_id`, `edge_id`, and typology fields
are excluded from model inputs.

Latest verified v0.1 10,000-member Milestone 5 run:

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
Persona median transaction metrics include an annualized value so shorter
`--months` runs are evaluated against comparable yearly thresholds.

The label validator also checks whether a simple transaction-ID threshold can
recover suspicious transactions. The latest verified 10,000-member run reports:

```text
suspicious txn_id percentile range: 0.3685-0.7858
best threshold precision:           0.0024
best threshold recall:              0.9825
```

This prevents the benchmark from leaking injection phase through late `txn_id`
or mirrored `reference` values.

Latest verified v0.2 10,000-member gate run:

```text
command: python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark --output ./datasets/KENYA_SACCO_SIM_v02_10k_review_fix
validation errors:   0
validation warnings: 0
members:             10,000
accounts:            41,003
transactions:        510,980
loans:               2,352
guarantors:          3,372
alerts_truth:        768
support files:       institutions / branches / agents / employers / devices
devices:             10,000
digital txns:         305,706
device-required txns: 305,706
missing devices:      0
device coverage:      100.00% of digital transactions
shared device share: 3.90% of active digital members
shared devices:      330
max members/device:  2
institution split max share: 71.12%
fake affordability:  precision 0.2409 / recall 0.9706
macro rule baseline: precision 0.6008 / recall 0.9296
```

Latest v0.2 multi-seed stability gate:

```text
command: python3 -m kenya_sacco_sim benchmark --members 10000 --seeds 42 1337 2026 9001 314159 --output ./benchmarks/v02_multi_seed
validation error free: true
precision/recall variance within threshold: true
STRUCTURING precision range: 0.0576
STRUCTURING recall range:    0.0000
RAPID precision range:       0.0241
RAPID recall range:          0.0000
FAKE precision range:        0.0300
FAKE recall range:           0.0882
cash rail share mean:        0.1939
digital device coverage:     1.0000
loan active member mean:     0.2385
arrears share mean:          0.0927
```

Known v0.2 benchmark behavior:

```text
FAKE_AFFORDABILITY_BEFORE_LOAN intentionally has low rule precision.
Normal borrowers may receive legitimate large pre-loan inflows, so false
positives are expected and make the benchmark less cartoon-clean.
100-member runs are smoke tests only, not valid benchmark evaluations.
```

## Development Discipline

Before committing changes:

- Run the smallest relevant generator command for the touched milestone.
- Run `python3 -m compileall src`.
- Run `git diff --check`.
- Update this README whenever behavior, commands, outputs, milestones, or validation expectations change.
- Do not commit unrelated local files.
