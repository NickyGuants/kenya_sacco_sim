# KENYA_SACCO_SIM v0.2 — Engineer-Ready Implementation Specification

## 1. Purpose

Extend the frozen v0.1 generator into a stronger Kenyan SACCO AML simulator without breaking existing v0.1 consumers.

v0.2 is a foundation release. It translates the deep research blueprint into implementation-ready contracts for external configuration, support entity exports, institution archetypes, device population, richer validation, and one SACCO-specific suspicious typology.

v0.2 must remain boring, reconciled, and auditable.

---

## 2. Compatibility Contract

v0.1 output files remain valid and continue to be emitted:

```text
members.csv
accounts.csv
loans.csv
guarantors.csv
transactions.csv
nodes.csv
graph_edges.csv
alerts_truth.csv
rule_results.json
split_manifest.json
baseline_model_results.json
feature_documentation.json
dataset_card.md
known_limitations.md
manifest.json
validation_report.json
```

v0.2 adds support files. These are additive and must not replace `nodes.csv` or `graph_edges.csv`:

```text
institutions.csv
branches.csv
agents.csv
employers.csv
devices.csv
```

The graph files remain projections over entities. Support files are the authoritative tabular metadata for infrastructure entities.

---

## 3. v0.2 Scale Target

```yaml
members: 10000
months: 12
institutions: 5
suspicious_member_ratio: 0.01
seeded_reproducibility: true
```

The release gate is a clean 10,000-member run with zero validation errors.

---

## 4. External Configuration

v0.2 introduces YAML configuration while preserving current CLI defaults.

Required config files:

```text
config/world.yaml
config/personas.yaml
config/products.yaml
config/institutions.yaml
config/patterns.yaml
config/typologies.yaml
config/calendar.yaml
config/validation.yaml
```

Rules:

```text
1. Missing config files must fall back to built-in defaults.
2. The default config must reproduce current v0.1 behaviour for equivalent seed and CLI arguments.
3. CLI arguments override config values for member count, institution count, months, seed, suspicious ratio, difficulty, and output path.
4. The manifest must record config source and loaded config filenames.
```

Implementation target:

```bash
python3 -m kenya_sacco_sim generate --members 10000 --with-loans --with-typologies --with-benchmark
```

must continue to work without requiring config arguments.

---

## 5. Institution Archetypes

Each institution must have an archetype. Archetypes influence persona assignment, channel mix, cash intensity, digital maturity, loan style, and normal cadence.

Allowed archetypes:

```text
TEACHER_PUBLIC_SECTOR
UNIFORMED_SERVICES
UTILITY_PRIVATE_SECTOR
COMMUNITY_CHURCH
FARMER_COOPERATIVE
SME_BIASHARA
DIASPORA_FACING
```

Minimum institution fields:

```text
institution_id:string
name:string
archetype:string
county:string
urban_rural:string
digital_maturity:decimal
cash_intensity:decimal
loan_guarantor_intensity:decimal
created_at:datetime
```

Validation:

```text
1. Every institution has an allowed archetype.
2. digital_maturity, cash_intensity, and loan_guarantor_intensity are between 0 and 1.
3. validation_report.json includes institution_archetype_metrics.
```

---

## 6. Support Entity Schemas

### 6.1 institutions.csv

One row per SACCO institution.

```text
institution_id:string
name:string
archetype:string
county:string
urban_rural:string
digital_maturity:decimal
cash_intensity:decimal
loan_guarantor_intensity:decimal
created_at:datetime
```

### 6.2 branches.csv

One row per branch.

```text
branch_id:string
institution_id:string
county:string
urban_rural:string
branch_type:string
opening_date:date
created_at:datetime
```

Allowed `branch_type`:

```text
HQ
BRANCH
AGENT_DESK
```

### 6.3 agents.csv

One row per cash or wallet agent used by the simulator.

```text
agent_id:string
institution_id:string
branch_id:string
provider:string
county:string
urban_rural:string
location_type:string
active_from:date
active_to:date|null
created_at:datetime
```

### 6.4 employers.csv

One row per payroll employer.

```text
employer_id:string
institution_id:string
employer_type:string
sector:string
public_private:string
county:string
urban_rural:string
payroll_frequency:string
checkoff_supported:boolean
created_at:datetime
```

### 6.5 devices.csv

One row per device identity used by digital channels.

```text
device_id:string
member_id:string
institution_id:string
first_seen:date
last_seen:date
os_family:string
app_user_flag:boolean
shared_device_group:string|null
created_at:datetime
```

Rules:

```text
1. device_id must resolve to DEVICE in nodes.csv.
2. member_id must resolve to members.csv.
3. Normal shared-device groups are allowed at low baseline rates.
4. Shared devices must not imply suspicious labels.
```

---

## 7. Device Layer

Digital transactions should populate `device_id` when the channel is:

```text
MOBILE_APP
USSD
PAYBILL
TILL
BANK_TRANSFER
```

Validation:

```text
1. device_id coverage for digital transactions must be greater than 0.
2. If device_id is present in transactions.csv, it must resolve to devices.csv and nodes.csv.
3. Every DEVICE node must have at least one USES_DEVICE edge.
4. shared_device_member_share must be reported.
```

Device sharing is baseline only in v0.2. Device-sharing typologies are deferred to v1.

---

## 8. Normal Rail Enrichment

v0.2 keeps v0.1 transaction schemas but makes rail semantics more explicit.

Normal behaviour should distinguish:

```text
1. wallet/paybill/till behaviour
2. agent and branch cash conversion
3. remittance-linked inflows
4. EFT/RTGS-style large institutional or supplier flows
5. government/tax/service-payment counterparties
```

Only the subset needed for v0.2 validation must be implemented. Do not destabilize the reconciled ledger to chase full rail realism.

---

## 9. Suspicious Typologies

v0.2 includes v0.1 typologies plus one new typology:

```text
STRUCTURING
RAPID_PASS_THROUGH
FAKE_AFFORDABILITY_BEFORE_LOAN
```

### 9.1 FAKE_AFFORDABILITY_BEFORE_LOAN

Meaning:

```text
Temporary non-salary external credits are used to inflate apparent affordability shortly before a loan application.
```

Candidate personas:

```text
SME_OWNER
DIASPORA_SUPPORTED
COUNTY_WORKER
SALARIED_TEACHER
```

Pattern:

```yaml
FAKE_AFFORDABILITY_BEFORE_LOAN:
  lookback_days: 30
  min_external_credit_share: 0.55
  min_balance_growth_kes: 50000
  credit_count: [2, 5]
  credit_amount_kes: [25000, 150000]
  eligible_loan_products:
    - DEVELOPMENT_LOAN
    - SCHOOL_FEES_LOAN
    - BIASHARA_LOAN
```

Insertion contract:

```text
1. Select an otherwise normal borrower with a real loan application.
2. Insert temporary external credits inside 30 days before application_date.
3. Credits must be non-salary and use normal-looking rails such as REMITTANCE, MPESA, PESALINK, or CASH_BRANCH.
4. Do not create a suspicious-only account.
5. Do not put typology labels in transactions.csv, accounts.csv, members.csv, loans.csv, or graph_edges.csv.
```

Labeling:

```text
Each suspicious pre-application credit gets an alerts_truth row.
The associated loan account/member gets pattern context through alerts_truth.
One PATTERN_SUMMARY row links the full pattern.
```

Baseline rule:

```python
if loan_application_within_30d \
   and external_non_salary_credits_share_30d >= 0.55 \
   and balance_growth_30d >= 50000:
    alert("FAKE_AFFORDABILITY_BEFORE_LOAN")
```

---

## 10. Validation Additions

Add these sections to `validation_report.json`:

```text
support_entity_validation
device_validation
institution_archetype_metrics
fake_affordability_validation
```

Hard errors:

```text
1. Support entity primary keys are duplicated.
2. Support entity foreign keys do not resolve.
3. transaction.device_id is populated but missing from devices.csv or nodes.csv.
4. DEVICE node has no USES_DEVICE edge.
5. FAKE_AFFORDABILITY_BEFORE_LOAN labels reference missing loans, members, accounts, or transactions.
6. Benchmark leakage checks fail.
```

Warnings:

```text
1. Digital device coverage is unexpectedly low.
2. Shared-device baseline is unexpectedly high.
3. Institution archetype distribution is too concentrated.
4. v0.2 rail enrichment metrics are weak but not invalid.
```

---

## 11. Benchmark Artifacts

Milestone 5 artifacts continue to be emitted when `--with-benchmark` is passed.

v0.2 additions:

```text
1. split_manifest.json includes support file split guidance.
2. feature_documentation.json documents support files and label roles.
3. baseline_model_results.json includes FAKE_AFFORDABILITY_BEFORE_LOAN rule results.
4. dataset_card.md documents v0.2 support files and known device limitations.
5. known_limitations.md clearly defers v1 typologies.
```

Leakage checks must cover:

```text
member_id
pattern_id
txn_id
reference
support entity identifiers
loan application timing
device_id
```

---

## 12. Implementation Milestones

### Milestone 6 — Specs and Config Foundation

Deliver:

```text
kenya_sacco_sim_v_0_2_specification.md
kenya_sacco_sim_v_1_backlog.md
docs/research/deep-research-report.md
config/*.yaml
YAML config loader
README update
```

Acceptance:

```text
Existing v0.1 CLI commands still work.
Config defaults load without changing current default behaviour.
Tests cover config loading and fallback defaults.
```

### Milestone 7 — Support Entity Exports

Deliver:

```text
institutions.csv
branches.csv
agents.csv
employers.csv
devices.csv
support entity validation
```

Acceptance:

```text
All support entity IDs resolve.
Nodes and graph edges remain valid.
Existing v0.1 files are still emitted.
```

### Milestone 8 — Institution Archetypes and Device Baseline

Deliver:

```text
institution archetype assignment
device population
USES_DEVICE edges
device coverage metrics
```

Acceptance:

```text
Digital transaction device coverage > 0.
Shared-device baseline is reported.
Institution archetype metrics are reported.
```

### Milestone 9 — Fake Affordability Typology

Deliver:

```text
FAKE_AFFORDABILITY_BEFORE_LOAN injection
alerts_truth coverage
baseline rule reconstruction
rule_results.json metrics
```

Acceptance:

```text
Ledger and loan balances reconcile.
No label leakage.
Rule results include TP/FP/FN and candidate IDs.
Suspicious members retain >=50% normal transaction share.
```

### Milestone 10 — v0.2 Benchmark Hardening

Deliver:

```text
updated split_manifest.json
updated baseline_model_results.json
updated feature_documentation.json
updated dataset_card.md
updated known_limitations.md
```

Acceptance:

```text
10,000-member v0.2 benchmark run has zero validation errors.
Baseline rules are reproducible from exported configs.
Leakage checks pass.
```

---

## 13. Deferred to v1

Do not implement these in v0.2 unless the v0.2 release gate is already clean:

```text
1. Guarantor fraud rings
2. Dormant-account reactivation abuse
3. Remittance layering typology
4. Wallet funneling typology
5. Device-sharing typologies
6. Graph neural network benchmark
7. 100,000+ member scale
8. Multi-difficulty benchmark suite
9. Full pattern_labels.csv and edge_labels.csv
10. Institution-specific calibration packs
```

---

## 14. Final Principle

```text
v0.2 should deepen the world before multiplying typologies.

First make infrastructure, devices, and support entities observable.
Then add fake affordability because it uses the SACCO credit engine.
Do not advance to v1 typologies until v0.2 benchmark leakage checks pass.
```
