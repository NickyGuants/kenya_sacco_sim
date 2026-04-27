# KENYA_SACCO_SIM v1 — Engineer-Ready Implementation Specification

## 1. Purpose

v1 turns KENYA_SACCO_SIM from a validated synthetic dataset generator into a
research-grade AML benchmark platform.

The current v1 foundation is intentionally narrow:

```text
Keep the generator stable.
Keep outputs leakage-safe.
Add graph/device behavioral ambiguity before adding more typologies.
Ship every typology with a rule, near-misses, candidate IDs, split coverage, and validation.
```

The first v1 typology is `DEVICE_SHARING_MULE_NETWORK`.
The current v1 benchmark also includes `GUARANTOR_FRAUD_RING` and
`WALLET_FUNNELING`.

---

## 2. Compatibility Contract

Existing consumers must continue to receive the established CSV and JSON files:

```text
members.csv
accounts.csv
institutions.csv
branches.csv
agents.csv
employers.csv
devices.csv
nodes.csv
graph_edges.csv
transactions.csv
loans.csv
guarantors.csv
alerts_truth.csv
rule_results.json
split_manifest.json
baseline_model_results.json
ml_baseline_results.json
feature_importance.json
ml_leakage_ablation.json
rule_vs_ml_comparison.json
benchmark_confounder_diagnostics.json
feature_documentation.json
dataset_card.md
known_limitations.md
validation_report.json
manifest.json
```

Rules:

```text
1. Feature files must not contain typology labels, pattern IDs, alert IDs, or other truth labels.
2. Label data belongs only in alerts_truth.csv and derived benchmark artifacts.
3. New suspicious behavior must be additive and must not break ledger replay.
4. nodes.csv and graph_edges.csv remain graph projections, not replacements for support files.
5. Every generated benchmark package must be reproducible from seed, config, and CLI flags.
```

---

## 3. Scale And Validity Target

The benchmark-grade run target is:

```yaml
members: 10000
months: 12
institutions: 5
suspicious_member_ratio: 0.01
active_typologies:
  - STRUCTURING
  - RAPID_PASS_THROUGH
  - FAKE_AFFORDABILITY_BEFORE_LOAN
  - DEVICE_SHARING_MULE_NETWORK
  - GUARANTOR_FRAUD_RING
  - WALLET_FUNNELING
```

A benchmark-grade run must satisfy:

```text
validation errors = 0
member_count >= 10,000
suspicious_member_count >= 100
typology_member_count >= 30 for each active typology
positive labels per split >= 5 for each active typology
patterns per split >= 5 for each active typology
labeled transactions per typology split >= 10
```

Smaller runs are smoke tests only.

---

## 4. Suspicious Typologies

### 4.1 STRUCTURING

Purpose:

```text
Detect repeated sub-threshold inbound deposits that form a larger placement pattern.
```

Rule contract:

```text
same member
>=5 inbound deposits under KES 100,000
within 7 days
total counted deposits >= KES 300,000
inbound types:
  FOSA_CASH_DEPOSIT
  BUSINESS_SETTLEMENT_IN
  MPESA_PAYBILL_IN
  PESALINK_IN
```

### 4.2 RAPID_PASS_THROUGH

Purpose:

```text
Detect large inbound value that exits quickly through multiple counterparties.
```

Rule contract:

```text
same account only
inbound within 48 hours >= KES 100,000
outbound value / inbound value >= 0.75
outbound counterparties >= 2
included inbound types:
  PESALINK_IN
  MPESA_PAYBILL_IN
  BUSINESS_SETTLEMENT_IN
included outbound types:
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
excluded:
  loan repayments
  household spend
  BOSA top-ups
  wallet top-ups
```

### 4.3 FAKE_AFFORDABILITY_BEFORE_LOAN

Purpose:

```text
Detect temporary non-salary external inflows that inflate apparent loan affordability before application.
```

Rule contract:

```text
loan application exists
lookback window = 30 days before application
external non-salary credit share >= 0.55
balance growth >= KES 50,000
eligible loan products:
  DEVELOPMENT_LOAN
  SCHOOL_FEES_LOAN
  BIASHARA_LOAN
```

Expected behavior:

```text
Low precision is acceptable and intended.
Normal borrowers may receive legitimate pre-loan remittances, harvest inflows, business receipts, or family support.
```

### 4.4 DEVICE_SHARING_MULE_NETWORK

Purpose:

```text
Detect multiple members coordinating suspicious digital activity through shared devices.
```

Rule contract:

```text
shared digital device
window = 30 days
members per device >= 3
device-attached transactions >= 6
total value >= KES 450,000
outbound value share >= 0.45
digital channels:
  MOBILE_APP
  USSD
  PAYBILL
  TILL
  BANK_TRANSFER
included inbound types:
  PESALINK_IN
  MPESA_PAYBILL_IN
  BUSINESS_SETTLEMENT_IN
included outbound types:
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
  MPESA_WALLET_TOPUP
```

Injection contract:

```text
1. Suspicious device-sharing groups must contain 3 to 5 members.
2. Every suspicious member must keep at least 50% normal transaction share.
3. Typology rows must use real member accounts and real device IDs.
4. devices.csv must explain every multi-member device through shared_device_group.
5. Raw device_id must not be a model input feature.
6. alerts_truth.csv must contain PATTERN, MEMBER, TRANSACTION, and EDGE context where applicable.
7. Suspicious start windows must be randomized across the simulation year so month-of-year is not a stable typology shortcut.
8. Candidate selection should avoid fixed persona-only assignment where the behavior can plausibly occur across broader member segments.
```

Small-run policy:

```text
Sub-1,000-member smoke runs must not request partial device-sharing mule groups.
If the target is nonzero, DEVICE_SHARING_MULE_NETWORK must allocate at least 3 members.
If suspicious_ratio is 0, every typology target must be 0.
```

### 4.5 GUARANTOR_FRAUD_RING

Purpose:

```text
Detect reciprocal or circular guarantor relationships used to manufacture
credit support across a small member group.
```

Rule contract:

```text
directed guarantee graph
active guaranteed loans only
guaranteed products:
  DEVELOPMENT_LOAN
  BIASHARA_LOAN
  ASSET_FINANCE
strongly connected component size >= 3
strongly connected component size <= 6
cycle edges within component >= 3
```

Injection contract:

```text
1. Suspicious rings must contain 3 to 5 members.
2. Ring members must have real active guaranteed loans.
3. Ring guarantees must be written to guarantors.csv and projected as GUARANTEES graph edges.
4. Suspicious labels must include PATTERN, MEMBER, and loan-context TRANSACTION rows.
5. Each suspicious member must keep at least 50% normal transaction share.
6. Normal guarantor concentration limits must remain valid for non-suspicious behavior.
7. Candidate selection must avoid fixed persona-only assignment.
```

### 4.6 WALLET_FUNNELING

Purpose:

```text
Detect many wallet/paybill credits fanning into one member account followed by
quick dispersion to multiple counterparties.
```

Rule contract:

```text
same account only
fan-in window = 7 days
dispersion window after last inbound = 72 hours
inbound count >= 6
inbound counterparties >= 5
inbound value >= KES 350,000
outbound value / inbound value >= 0.55
outbound counterparties >= 2
included inbound types:
  MPESA_PAYBILL_IN
  WALLET_P2P_IN
  BUSINESS_SETTLEMENT_IN
included outbound types:
  MPESA_WALLET_TOPUP
  WALLET_P2P_OUT
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
```

Injection contract:

```text
1. Suspicious wallet-funneling members must use real FOSA accounts.
2. Inbound counterparties must be distinct enough to create fan-in behavior.
3. Outbound dispersion must happen after the fan-in window and use multiple counterparties.
4. Every suspicious member must keep at least 50% normal transaction share.
5. Candidate selection must avoid fixed persona-only assignment.
6. Normal chama, welfare, project, and low-fanout collection near-misses must remain unlabeled.
```

---

## 5. Normal Near-Misses

Every typology must ship with normal-but-suspicious-looking negatives.

Required near-miss families:

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
```

`legitimate_chama_wallet_collection` is intentional false-positive pressure for
`WALLET_FUNNELING`: normal chama, welfare, church, or project collections may
receive many wallet/paybill credits and make quick legitimate vendor or member
payouts. `near_wallet_funnel_low_fanout` remains a negative control that should
miss the executable rule through too few source or destination counterparties.

Near-misses must not appear in `alerts_truth.csv`, but their counts should be
reported in validation or rule artifacts when applicable.

Reporting contract:

```text
rule_results.json.near_miss_disclosure
validation_report.json.near_miss_validation
baseline_model_results.json.near_miss_disclosure
dataset_card.md near-miss section
multi_seed_results.json.near_miss_stability
```

---

## 6. Device Baseline

Digital channels requiring `device_id`:

```text
MOBILE_APP
USSD
PAYBILL
TILL
BANK_TRANSFER
```

Validation metrics:

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
shared_device_member_share
```

Hard rules:

```text
1. device_required_missing_device_id_count must be 0.
2. Every transaction.device_id must resolve to devices.csv.
3. Every transaction.device_id must resolve to a DEVICE node.
4. Any device used by multiple transaction members must have shared_device_group.
5. Multi-member device usage must be explainable by members/devices in the same shared_device_group.
```

---

## 7. Graph Contract

Required graph edge families:

```text
INSTITUTION_HAS_BRANCH
EMPLOYER_BELONGS_TO_INSTITUTION
USES_AGENT
USES_DEVICE
HAS_ACCOUNT
HAS_WALLET
ACCOUNT_BELONGS_TO_INSTITUTION
ACCOUNT_AT_BRANCH
EMPLOYED_BY
GUARANTEES
SOURCE_FUNDS_ACCOUNT
ACCOUNT_PAYS_SINK
```

Graph validation must reject:

```text
missing src_node_id
missing dst_node_id
unresolved support entity references
missing member/account/device nodes
isolated infrastructure caused by missing projection edges
```

---

## 8. Benchmark Artifacts

When `--with-benchmark` is passed, emit:

```text
split_manifest.json
baseline_model_results.json
ml_baseline_results.json
feature_importance.json
ml_leakage_ablation.json
rule_vs_ml_comparison.json
benchmark_confounder_diagnostics.json
feature_documentation.json
dataset_card.md
known_limitations.md
```

The benchmark task is member-level multi-label typology detection.

Rules:

```text
1. Train/validation/test splits are keyed by member_id and pattern_id.
2. No member may appear in more than one split.
3. No pattern may appear in more than one split.
4. Rule results must export executable rule configs and candidate IDs.
5. False-positive and false-negative member IDs must be exported where relevant.
6. ML features must exclude raw identifiers and label-bearing fields.
7. Leakage ablation must remove typology-specific rule-proxy features and rerun ML baselines.
8. Confounder diagnostics must report temporal label concentration and persona/static-attribute label concentration.
9. Rule-vs-ML comparison is descriptive and must not be framed as ML superiority evidence without ablation and confounder support.
10. Multi-seed results must summarize full-feature ML F1, ablated ML F1, ablation F1 drops, and confounder diagnostic flags.
11. Dataset card must summarize near-miss and negative-control coverage.
```

Blocked ML input fields:

```text
member_id
txn_id
reference
pattern_id
alert_id
account_id
device_id
node_id
edge_id
typology
truth_label
label fields
```

Minimum ML feature families:

```text
transaction counts
inbound/outbound counts
total inflow/outflow
inflow/outflow ratio
cash share
M-Pesa share
PesaLink share
device count
shared-device flag
digital device coverage proxy
shared-device transaction share
device peer count
loan count
loan application proximity
pre-loan external credit share
temporal burst counts
rolling 24h/7d aggregates
48h inflow-to-outflow exit ratios
counterparty diversity and concentration
wallet fan-in counterparty count
wallet fan-in value
wallet funnel exit ratio
persona-relative transaction behavior
graph degree
account degree
guarantor out-degree
guarantor in-degree
```

---

## 9. Validation Additions

The validation report must include:

```text
device_sharing_mule_network_validation
guarantor_fraud_ring_validation
wallet_funneling_validation
benchmark_validation
typology_runtime_metrics
label_validation
device_validation
near_miss_validation
```

Hard errors:

```text
1. Ledger replay mismatches.
2. Missing required columns.
3. Strict enum violations.
4. Broken primary keys or foreign keys.
5. Missing required device_id for digital transactions.
6. Multi-member device usage with missing shared_device_group.
7. Suspicious member normal transaction share < 0.50.
8. Benchmark split leakage by member_id or pattern_id.
9. txn_id/reference threshold leakage shortcut exceeds configured tolerance.
10. Full benchmark run fails evaluation-density minimums.
```

Warnings:

```text
1. Distribution drift outside realism bands.
2. Shared-device baseline outside expected range.
3. Institution split drift above threshold.
4. Rule precision for intentionally ambiguous typologies is low but documented.
5. Suspicious labels are concentrated in narrow time windows: `max_month_share > 0.40`, `window_span_days < 120`, or `active_month_count < 10`.
6. Suspicious labels are concentrated by persona/static attributes.
```

---

## 10. Multi-Seed Stability Gate

Required command:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 10000 \
  --seeds 42 1337 2026 9001 314159 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

Acceptance:

```text
all seeds have validation_error_count = 0
all seeds are valid benchmark evaluations
precision/recall range per typology <= 0.10
distribution stability reported for cash, devices, loan activity, arrears, and defaults
```

---

## 11. Latest Verified v1 Gate

Latest verified generated package:

```text
datasets/KENYA_SACCO_SIM_v1_10k
```

Latest verified multi-seed package:

```text
benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

Current accepted metrics:

```text
validation errors: 0
validation warnings: 0
digital device coverage: 100%
max members per device: 3
near-miss families: 12
near-miss members: 211
near-miss transactions: 858
near-miss guarantees: 16
DEVICE_SHARING_MULE_NETWORK precision: 1.0000
DEVICE_SHARING_MULE_NETWORK recall: 1.0000
GUARANTOR_FRAUD_RING precision: 1.0000
GUARANTOR_FRAUD_RING recall: 1.0000
WALLET_FUNNELING precision: 0.5833
WALLET_FUNNELING recall: 0.9333
FAKE_AFFORDABILITY precision: 0.1974
RAPID_PASS_THROUGH precision: 0.4615
STRUCTURING precision: 0.3571
evaluation validity: valid
multi-seed precision/recall variance: within threshold
single 10k package wall clock: 52.3s
five-seed 10k benchmark wall clock: 106.6s with --jobs 4
```

The benchmark runner caps parallel seed workers by CPU count and estimated
memory budget. The current local gate uses four workers for 10k runs. 100k
full benchmark probes are capped to two workers on the local development
machine; the latest two-seed 100k probe was stopped after ten minutes with no
completed seed. 100k remains an experimental scaling target, not supported
release scale.

---

## 12. Next Implementation Slice

Latest implementation slice:

```text
WALLET_FUNNELING
```

Why:

```text
MPESA, paybill, and counterparty structure already exist.
The behavior is Kenyan-specific and SACCO-relevant.
It extends rapid-pass-through into multi-counterparty wallet fan-in behavior.
```

Acceptance requirements for the next typology:

```text
1. Add executable rule config.
2. Add injector with normal blending.
3. Add normal near-misses.
4. Export candidate IDs, false positives, and false negatives.
5. Add validation section.
6. Add ML features only if they are non-leaky.
7. Update README, docs, dataset card, known limitations, and this spec in the same implementation slice.
```

Next candidate after this slice:

```text
CHURCH_CHARITY_MISUSE
```
