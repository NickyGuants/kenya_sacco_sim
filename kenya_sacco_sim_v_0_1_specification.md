# KENYA_SACCO_SIM v0.1 — Engineer-Ready Implementation Specification

## 1. Purpose

Build a synthetic AML dataset generator for Kenyan SACCOs.

The simulator must generate realistic normal SACCO/member financial behaviour, then inject controlled suspicious behaviours with complete ground-truth labels.

This v0.1 spec is intentionally narrow. It is not the final research simulator. It is the first buildable version.

---

## 2. MVP Target

### v0.1 must generate

```text
members.csv
accounts.csv
loans.csv
guarantors.csv
transactions.csv
nodes.csv
graph_edges.csv
alerts_truth.csv
manifest.json
validation_report.json
```

v0.1 development benchmark may also generate:

```text
rule_results.json
```

`rule_results.json` is not required for the base dataset release, but it is required for Milestone 5.

### v0.1 scale target

```yaml
members: 10000
months: 12
institutions: 5
suspicious_member_ratio: 0.01
seeded_reproducibility: true
```

### v0.1 is optimized for

```text
correctness > realism > scale
```

The first release should run cleanly at 10,000 members before scaling to 100,000+.

---

## 3. Intended Consumers

KENYA_SACCO_SIM v0.1 should support four consumers:

```text
1. AML rule-testing
2. Transaction monitoring benchmark data
3. Graph AML research
4. Demo data for Kenyan SACCO AML products
```

The first priority is **rule-testing**, not ML training.

ML and graph benchmarks are supported structurally through nodes, edges, and labels, but v0.1 success is measured by whether deterministic AML rules can be tested against known typologies.

---

## 4. Design Principles

### 4.1 Pattern-first generation

Every transaction must come from a named behaviour pattern.

Bad:

```python
emit_random_transaction(member)
```

Good:

```python
insert_pattern(
    pattern="SALARY_CHECKOFF_WALLET_SPEND",
    actor=member,
    calendar_window=payday_window
)
```

### 4.2 Graph-first world

The simulator must first create a graph of actors, accounts, wallets, employers, SACCOs, branches, agents, devices, sources, and sinks.

Transactions are emitted over this graph.

### 4.3 Complete ground truth

Suspicious behaviour must be labeled at transaction, member, account, pattern, and graph-edge level where possible.

### 4.4 No obvious label leakage

Generated benchmark files must not contain fields that trivially reveal suspicious status unless the file is explicitly a label file.

For example, `transactions.csv` must not contain:

```text
is_synthetic_suspicious
injected_rule_id
typology
alert_id
```

Those belong in `alerts_truth.csv`.

---

## 5. High-Level Architecture

```text
KENYA_SACCO_SIM
  ├── config loader
  ├── world generator
  │   ├── institutions
  │   ├── members
  │   ├── employers
  │   ├── branches
  │   ├── agents
  │   ├── wallets
  │   ├── devices
  │   ├── sources
  │   └── sinks
  │
  ├── graph generator
  │   ├── nodes
  │   ├── edges
  │   └── graph constraints
  │
  ├── normal pattern engine
  │   ├── salary/checkoff pattern
  │   ├── wallet spend pattern
  │   ├── SME cash cycle pattern
  │   ├── farmer seasonal pattern
  │   └── loan repayment pattern
  │
  ├── SACCO product engine
  │   ├── BOSA deposits
  │   ├── FOSA savings/current accounts
  │   ├── share capital
  │   ├── loans
  │   └── guarantors
  │
  ├── suspicious pattern engine
  │   ├── structuring
  │   └── rapid pass-through
  │
  ├── balance engine
  │   ├── account balances
  │   ├── source-funded inflows
  │   ├── credit-funded inflows
  │   └── insufficient-funds prevention
  │
  ├── label engine
  │   ├── alerts_truth.csv
  │   └── hidden internal labels
  │
  ├── validator
  │   ├── schema validation
  │   ├── balance validation
  │   ├── graph validation
  │   ├── label validation
  │   └── distribution validation
  │
  └── exporter
```

---

## 6. Mandatory Kenyan SACCO/Payment Assumptions

These are mandatory in v0.1.

```text
1. Each formal salaried member has at least one FOSA account.
2. Each normal SACCO member has a BOSA deposit account.
3. Share capital is distinct from deposits.
4. Loan accounts are distinct from FOSA and BOSA accounts.
5. Payroll checkoff can split salary into:
   - loan repayment
   - BOSA deposit contribution
   - net salary to FOSA
6. M-Pesa/wallet behaviour exists for most members.
7. Guarantor-backed loans exist for development/emergency loans.
8. School-fee seasonality exists in January, May, and August.
9. December has increased spend/remittance/cashout activity.
10. Rural members use more cash/agent transactions than urban members.
```

These are configurable in later versions, but required for v0.1.

---

## 7. Config Files

```text
config/
  world.yaml
  personas.yaml
  products.yaml
  patterns.yaml
  typologies.yaml
  calendar.yaml
  validation.yaml
```

### 7.1 world.yaml

```yaml
world:
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  currency: "KES"
  seed: 42

  institutions:
    count: 5

  members:
    count: 10000

  suspicious_member_ratio: 0.01
  difficulty: medium
```

### 7.2 minimal config schemas

`personas.yaml` must define persona shares, income ranges, wallet adoption, rural probability, and annual loan probability.

```yaml
personas:
  SALARIED_TEACHER:
    share: 0.22
    monthly_income_kes: [45000, 78000, 120000]
    wallet_adoption_probability: 0.95
    rural_probability: 0.35
    loan_probability_annual: 0.35
  COUNTY_WORKER:
    share: 0.13
    monthly_income_kes: [35000, 65000, 95000]
    wallet_adoption_probability: 0.95
    rural_probability: 0.30
    loan_probability_annual: 0.30
  SME_OWNER:
    share: 0.18
    monthly_income_kes: [30000, 120000, 300000]
    wallet_adoption_probability: 0.98
    rural_probability: 0.25
    loan_probability_annual: 0.25
  FARMER_SEASONAL:
    share: 0.17
    monthly_income_kes: [10000, 35000, 150000]
    wallet_adoption_probability: 0.80
    rural_probability: 0.85
    loan_probability_annual: 0.20
  DIASPORA_SUPPORTED:
    share: 0.10
    monthly_income_kes: [15000, 50000, 180000]
    wallet_adoption_probability: 0.95
    rural_probability: 0.45
    loan_probability_annual: 0.15
  BODA_BODA_OPERATOR:
    share: 0.15
    monthly_income_kes: [20000, 45000, 80000]
    wallet_adoption_probability: 0.98
    rural_probability: 0.40
    loan_probability_annual: 0.22
  CHURCH_ORG:
    share: 0.05
    monthly_income_kes: [30000, 150000, 600000]
    wallet_adoption_probability: 0.90
    rural_probability: 0.50
    loan_probability_annual: 0.10
```

`products.yaml` must define account semantics and loan product rules.

```yaml
products:
  accounts:
    BOSA_DEPOSIT:
      withdrawable: false
      loan_eligibility_base: true
    FOSA_SAVINGS:
      withdrawable: true
      transactional: true
    FOSA_CURRENT:
      withdrawable: true
      transactional: true
    SHARE_CAPITAL:
      withdrawable: false
      ownership_equity: true
    MPESA_WALLET:
      withdrawable: true
      external_wallet: true
    LOAN_ACCOUNT:
      balance_semantics: positive_outstanding_principal

  loans:
    DEVELOPMENT_LOAN:
      deposit_multiplier: 3.0
      requires_guarantors: true
      tenor_months: [12, 36]
    SCHOOL_FEES_LOAN:
      deposit_multiplier: 2.0
      requires_guarantors: false
      tenor_months: [3, 12]
    EMERGENCY_LOAN:
      deposit_multiplier: 1.5
      requires_guarantors: false
      tenor_months: [1, 12]
    BIASHARA_LOAN:
      deposit_multiplier: 2.5
      requires_guarantors: true
      tenor_months: [6, 24]
    ASSET_FINANCE:
      deposit_multiplier: 3.0
      requires_guarantors: true
      tenor_months: [12, 48]
    SALARY_ADVANCE:
      deposit_multiplier: 1.0
      requires_guarantors: false
      tenor_months: [1, 3]
```

`patterns.yaml` must define which normal and suspicious patterns are enabled.

```yaml
patterns:
  normal:
    SALARY_CHECKOFF_WALLET_SPEND:
      enabled: true
      monthly_probability: 1.0
    SME_DAILY_RECEIPTS_MONDAY_DEPOSIT:
      enabled: true
      monthly_probability: 0.95
    FARMER_SEASONAL_INCOME:
      enabled: true
      harvest_months: [3, 4, 8, 9, 12]
    DIASPORA_SUPPORT_HOUSEHOLD:
      enabled: true
      monthly_probability: 0.65
    LOAN_LIFECYCLE_NORMAL:
      enabled: true
  suspicious:
    STRUCTURING:
      enabled: true
    RAPID_PASS_THROUGH:
      enabled: true
```

`typologies.yaml` must define suspicious-pattern parameters.

```yaml
typologies:
  STRUCTURING:
    candidate_personas: [SME_OWNER, BODA_BODA_OPERATOR, DIASPORA_SUPPORTED]
    deposit_count_7d: [5, 12]
    amount_each_kes: [70000, 99000]
    window_days: [2, 7]
    rails: [CASH_BRANCH, CASH_AGENT, MPESA]

  RAPID_PASS_THROUGH:
    candidate_personas: [DIASPORA_SUPPORTED, SME_OWNER, CHURCH_ORG]
    inflow_amount_kes: [100000, 750000]
    exit_ratio: [0.75, 0.98]
    exit_delay_hours: [1, 48]
    outflow_count: [2, 8]
```

`calendar.yaml` must define recurring timing effects.

```yaml
calendar:
  payday_days: [24, 25, 26, 27, 28, 29, 30, 31]
  school_fee_months: [1, 5, 8]
  harvest_months: [3, 4, 8, 9, 12]
  december_spend_multiplier: 1.5
  weekend_wallet_multiplier: 1.3
  monday_sme_deposit_multiplier: 1.4
```

`validation.yaml` must define validation thresholds.

```yaml
validation:
  suspicious_ratio_tolerance: 0.002
  allow_negative_balances_for_customer_accounts: false
  allow_negative_balances_for_source_accounts: true
  max_missing_foreign_key_count: 0
  require_pattern_summary_for_suspicious_patterns: true
  forbid_label_leakage: true
```

---

## 8. Output Schemas

## 7.8 Global conventions

### Canonical transaction sign convention

```text
amount_kes is always positive.
Direction is determined only by account_id_dr and account_id_cr.
Do not use negative transaction amounts.
```

### Timezone standard

```text
All timestamps use Africa/Nairobi (EAT, UTC+3).
Exports should use ISO-8601 format with timezone offset where supported.
```

### Reproducibility seed hierarchy

```text
global_seed
institution_seed = hash(global_seed, institution_id)
member_seed = hash(global_seed, member_id)
pattern_seed = hash(global_seed, pattern_id)
```

Use deterministic child seeds so parallel generation reproduces identical outputs.

### Deterministic ID formats

```text
institution_id = INST0001
member_id      = MEM0000001
account_id     = ACC00000001
loan_id        = LOAN000001
guarantee_id   = GUA000001
txn_id         = TXN000000000001
node_id        = NODE00000001
edge_id        = EDGE00000001
alert_id       = ALT00000001
pattern_id     = PAT00000001
```

---

## 8.1 members.csv

One row per SACCO member or organization.

```text
member_id:string
institution_id:string
member_type:string
persona_type:string
county:string
urban_rural:string  # recommended values: URBAN, PERI_URBAN, RURAL
gender:string  # recommended values: MALE, FEMALE, OTHER, UNKNOWN
age:int
occupation:string
employer_id:string|null
join_date:date
kyc_level:string
risk_segment:string
phone_hash:string
id_hash:string|null
declared_monthly_income_kes:decimal
income_stability_score:decimal
dormant_flag:boolean
created_at:datetime
```

### Allowed member_type

```text
INDIVIDUAL
ORGANIZATION
```

### Allowed kyc_level

```text
MINIMAL
STANDARD
ENHANCED
SIMPLIFIED
PENDING_REFRESH
RESTRICTED
```

### Allowed risk_segment

```text
LOW
MEDIUM
HIGH
PEP
SANCTIONS_RELATED
WATCHLIST
UNDER_REVIEW
```

### Allowed persona_type

```text
SALARIED_TEACHER
COUNTY_WORKER
SME_OWNER
FARMER_SEASONAL
DIASPORA_SUPPORTED
BODA_BODA_OPERATOR
CHURCH_ORG
```

`SHELL_MEMBER` is reserved for later versions. In v0.1 suspicious behaviour is injected into plausible ordinary personas to avoid cartoon-criminal leakage.

---

## 8.2 accounts.csv

One row per account, including SACCO, wallet, loan, share capital, source, and sink accounts.

```text
account_id:string
member_id:string|null
institution_id:string|null
account_owner_type:string
account_type:string
product_code:string
open_date:date
status:string
linked_wallet_id:string|null
branch_id:string|null
currency:string  # v0.1 allowed values: KES (default). Future versions may support multi-currency.
opening_balance_kes:decimal
current_balance_kes:decimal
external_account_label:string|null
```

### account_owner_type

```text
MEMBER
INSTITUTION
SOURCE
SINK
```

### account_status

(Validator note: applies to `accounts.status` field.)

```text
ACTIVE
DORMANT
FROZEN
CLOSED
PENDING_OPEN
RESTRICTED
```

### product_code guidance

`product_code` is required for every account row.

Recommended values by account type:

```text
BOSA_DEPOSIT   -> BOSA_STANDARD
FOSA_SAVINGS   -> FOSA_SAVINGS_STANDARD
FOSA_CURRENT   -> FOSA_CURRENT_STANDARD
SHARE_CAPITAL  -> SHARE_CAPITAL_STANDARD
LOAN_ACCOUNT   -> use matching loan product code (DEVELOPMENT_LOAN, SCHOOL_FEES_LOAN, etc.)
MPESA_WALLET   -> MPESA_WALLET
AIRTEL_WALLET  -> AIRTEL_WALLET
SOURCE_ACCOUNT -> EXTERNAL_SOURCE
SINK_ACCOUNT   -> EXTERNAL_SINK
```

### account_type

```text
BOSA_DEPOSIT
FOSA_SAVINGS
FOSA_CURRENT
SHARE_CAPITAL
LOAN_ACCOUNT
MPESA_WALLET
AIRTEL_WALLET
SOURCE_ACCOUNT
SINK_ACCOUNT
```

### Loan account balance semantics

`LOAN_ACCOUNT.current_balance_kes` is always **positive outstanding principal**.

```text
Loan disbursement increases LOAN_ACCOUNT.current_balance_kes.
Loan repayment decreases LOAN_ACCOUNT.current_balance_kes.
A fully repaid loan has LOAN_ACCOUNT.current_balance_kes = 0.
```

This avoids negative-liability confusion in v0.1. The simulator may later introduce full accounting signs, but v0.1 exports loan balances as outstanding principal.

### Source and sink account semantics

`SOURCE_ACCOUNT` and `SINK_ACCOUNT` are external balancing accounts used to preserve double-entry structure.

```text
SOURCE_ACCOUNT balances may go negative.
SINK_ACCOUNT balances may grow positive.
Neither is treated as a customer/member balance.
They are excluded from customer balance distribution validation.
```

Public source/sink account IDs and labels must be generic. Do not expose names like `ILLICIT_CASH_SOURCE` or `CYBER_FRAUD_SOURCE` in exported public feature files.

### Required account setup

For most individual members:

```text
1 BOSA_DEPOSIT
1 FOSA_SAVINGS or FOSA_CURRENT
1 SHARE_CAPITAL
0 or 1 MPESA_WALLET
0 or more LOAN_ACCOUNT
```

For church/org members:

```text
1 FOSA_CURRENT
1 BOSA_DEPOSIT optional
1 MPESA_WALLET optional
```

---

## 8.3 loans.csv

One row per loan.

```text
loan_id:string
member_id:string
institution_id:string
loan_account_id:string
product_code:string
application_date:date
approval_date:date|null
disbursement_date:date|null
principal_kes:decimal
tenor_months:int
interest_rate_annual:decimal
repayment_mode:string
disbursement_channel:string
purpose_code:string
deposit_balance_at_application_kes:decimal
loan_to_deposit_multiple:decimal
performing_status:string
arrears_days:int
restructure_flag:boolean
default_flag:boolean
```

### product_code

```text
DEVELOPMENT_LOAN
SCHOOL_FEES_LOAN
EMERGENCY_LOAN
BIASHARA_LOAN
ASSET_FINANCE
SALARY_ADVANCE
```

### repayment_mode

```text
PAYROLL_CHECKOFF
MANUAL_FOSA_TRANSFER
MPESA_PAYBILL
CASH_BRANCH
```

### disbursement_channel

```text
FOSA_ACCOUNT
MPESA_WALLET
BANK_TRANSFER
CASH_BRANCH
```

### purpose_code

```text
SCHOOL_FEES
BUSINESS_WORKING_CAPITAL
ASSET_PURCHASE
MEDICAL_EMERGENCY
HOUSEHOLD_NEED
AGRICULTURE_INPUTS
DEVELOPMENT_PROJECT
OTHER
```

### performing_status

```text
CURRENT
IN_ARREARS
RESTRUCTURED
DEFAULTED
CLOSED
WRITTEN_OFF
```

---

## 8.4 guarantors.csv

One row per guarantor relationship.

```text
guarantee_id:string
loan_id:string
borrower_member_id:string
guarantor_member_id:string
guarantee_amount_kes:decimal
guarantee_pct:decimal
pledge_date:date
release_date:date|null
guarantor_deposit_balance_at_pledge_kes:decimal
relationship_type:string
guarantor_capacity_remaining_kes:decimal
```

### relationship_type

```text
COWORKER
FAMILY
FRIEND
SACCO_MEMBER
CHURCH_MEMBER
BUSINESS_ASSOCIATE
UNKNOWN
```

---

## 8.5 transactions.csv

One row per ledger event.

Important: no suspicious labels in this file.

```text
txn_id:string
timestamp:datetime
institution_id:string|null
account_id_dr:string|null
account_id_cr:string|null
member_id_primary:string|null
txn_type:string
rail:string
channel:string
provider:string|null  # recommended values: MPESA, AIRTEL_MONEY, SACCO_CORE, INTERNAL_SYSTEM, BANK_PARTNER
counterparty_type:string
counterparty_id_hash:string|null
amount_kes:decimal
fee_kes:decimal
currency:string
narrative:string
reference:string
branch_id:string|null
agent_id:string|null
device_id:string|null
geo_bucket:string|null  # recommended values: county/subcounty/location cluster code
batch_id:string|null
balance_after_dr_kes:decimal|null
balance_after_cr_kes:decimal|null
is_reversal:boolean
```

### txn_type

```text
SALARY_IN
CHECKOFF_DEPOSIT
CHECKOFF_LOAN_RECOVERY
SHARE_CAPITAL_TOPUP
BOSA_DEP_TOPUP
FOSA_CASH_DEPOSIT
FOSA_CASH_WITHDRAWAL
MPESA_PAYBILL_IN
MPESA_WALLET_TOPUP
MPESA_CASHOUT
PESALINK_IN
PESALINK_OUT
WALLET_P2P_IN
WALLET_P2P_OUT
LOAN_DISBURSEMENT
LOAN_REPAYMENT
INTEREST_ACCRUAL
PENALTY_POST
DIVIDEND_POST
ACCOUNT_REACTIVATION
KYC_REFRESH
SYSTEM_CORRECTION
CHURCH_COLLECTION_IN
BUSINESS_SETTLEMENT_IN
SUPPLIER_PAYMENT_OUT
SCHOOL_FEES_PAYMENT_OUT
HOUSEHOLD_SPEND_OUT
REVERSAL
```

### rail

```text
SACCO_INTERNAL
MPESA
AIRTEL_MONEY
PESALINK
EFT
RTGS
CASH_BRANCH
CASH_AGENT
PAYROLL_CHECKOFF
REMITTANCE
```

### counterparty_type

```text
MEMBER
CUSTOMER
MERCHANT
EMPLOYER
GOVERNMENT
BANK
WALLET_USER
AGENT
CHURCH
SACCO
EXTERNAL_UNKNOWN
SOURCE
SINK
```

### channel

```text
BRANCH
AGENT
MOBILE_APP
USSD
PAYBILL
TILL
BANK_TRANSFER
PAYROLL_FILE
SYSTEM
```

### Non-financial transaction events

`KYC_REFRESH` and `ACCOUNT_REACTIVATION` are allowed in `transactions.csv` as zero-amount operational ledger events.

Rules:

```text
amount_kes = 0
fee_kes = 0
account_id_dr may be null
account_id_cr may be the affected member account
balance_after_dr_kes = null when account_id_dr is null
balance_after_cr_kes must equal the unchanged account balance
rail = SACCO_INTERNAL
channel = SYSTEM
```

These events are included because some AML typologies depend on dormant-account reactivation or KYC-refresh timing, even if v0.1 does not inject dormant reactivation as a suspicious typology.

`SYSTEM_CORRECTION` is a zero or non-zero administrative event used only by the balance engine to correct generated inconsistencies during development. It should be disabled in released benchmark datasets unless explicitly documented in `manifest.json`.

---

## 8.6 nodes.csv

One row per graph node.

```text
node_id:string
node_type:string
entity_id:string|null
institution_id:string|null
county:string|null
urban_rural:string|null
created_at:datetime
```

### node_type

```text
INSTITUTION
MEMBER
ACCOUNT
WALLET
EMPLOYER
BRANCH
AGENT
DEVICE
SOURCE
SINK
```

---

## 8.7 graph_edges.csv

One row per graph edge.

```text
edge_id:string
src_node_id:string
dst_node_id:string
edge_type:string
start_date:date
end_date:date|null
weight:decimal
metadata_json:string
```

### edge_type

```text
HAS_ACCOUNT
HAS_WALLET
EMPLOYED_BY
USES_DEVICE
USES_AGENT
GUARANTEES
RELATED_TO
INTRODUCED_BY
ATTENDS_CHURCH
OWNS_BUSINESS
TRANSACTS_WITH
SHARES_PHONE
SHARES_DEVICE
INSTITUTION_HAS_BRANCH
ACCOUNT_AT_BRANCH
ACCOUNT_BELONGS_TO_INSTITUTION
EMPLOYER_BELONGS_TO_INSTITUTION
SOURCE_FUNDS_ACCOUNT
ACCOUNT_PAYS_SINK
```

No suspicious labels in this file.

### Foreign key resolution rule

v0.1 does not export dedicated `institutions.csv`, `branches.csv`, `employers.csv`, `agents.csv`, or `devices.csv`.

Therefore:

```text
institution_id, branch_id, employer_id, agent_id, and device_id must resolve to nodes.entity_id in nodes.csv.
```

Required node mappings:

```text
institution_id -> node_type = INSTITUTION
branch_id -> node_type = BRANCH
employer_id -> node_type = EMPLOYER
agent_id -> node_type = AGENT
device_id -> node_type = DEVICE
account_id -> node_type = ACCOUNT or WALLET
member_id -> node_type = MEMBER
```

If a future version adds dedicated entity CSVs, the foreign-key rule may be tightened.

---

## 8.8 alerts_truth.csv

This is the only v0.1 file that explicitly reveals suspicious ground truth.

```text
alert_id:string
pattern_id:string
typology:string
entity_type:string
entity_id:string
member_id:string|null
account_id:string|null
txn_id:string|null
edge_id:string|null
start_timestamp:datetime
end_timestamp:datetime
severity:string
truth_label:boolean
stage:string
explanation_code:string
```

### typology

```text
STRUCTURING
RAPID_PASS_THROUGH
```

### entity_type

```text
MEMBER
ACCOUNT
TRANSACTION
EDGE
PATTERN
```

### severity

```text
LOW
MEDIUM
HIGH
CRITICAL
```

### stage

```text
PLACEMENT
LAYERING
INTEGRATION
PATTERN_SUMMARY
```

### explanation_code

```text
STRUCTURED_SUB_THRESHOLD_DEPOSITS
RAPID_IN_OUT_MOVEMENT
HIGH_EXIT_RATIO
MULTIPLE_OUTBOUND_COUNTERPARTIES
SUSPICIOUS_PATTERN_SUMMARY
```

---

## 9. Balance Invariants

These must always hold.

### 9.1 Double-entry invariant

Every transaction must have at least one debit account or one credit account.

Preferred:

```text
account_id_dr != null AND account_id_cr != null
```

Allowed exceptions:

```text
SOURCE_ACCOUNT -> member account
member account -> SINK_ACCOUNT
zero-amount operational event with one affected account
```

Source and sink accounts must be represented in `accounts.csv` in v0.1. They preserve double-entry structure while representing external money entering or leaving the observable SACCO/wallet world.

---

### 9.2 No unfunded outflows

An account cannot debit more than its available balance unless the transaction is explicitly credit-funded.

Allowed credit-funded or balance-adjusting events:

```text
LOAN_DISBURSEMENT
REVERSAL
SYSTEM_CORRECTION
```

`SYSTEM_CORRECTION` must appear in the `txn_type` enum if used. Released benchmark datasets should have zero `SYSTEM_CORRECTION` rows unless the manifest explicitly says otherwise.

---

### 9.3 Loan disbursement invariant

Every loan disbursement must create:

```text
1. A loans.csv row
2. A LOAN_ACCOUNT in accounts.csv
3. A LOAN_DISBURSEMENT transaction
4. Zero or more guarantors.csv rows depending on product
```

---

### 9.4 Loan repayment invariant

Every loan repayment must reduce loan outstanding balance.

The simulator may not export outstanding balance in v0.1, but internal validation must verify it.

---

### 9.5 Share capital invariant

Share capital is not withdrawable like FOSA savings.

No normal `FOSA_CASH_WITHDRAWAL` or `MPESA_CASHOUT` may debit a `SHARE_CAPITAL` account.

---

### 9.6 BOSA/FOSA distinction

```text
BOSA_DEPOSIT = long-term member deposit / loan eligibility base
FOSA_* = transactional savings/current account
SHARE_CAPITAL = ownership stake
LOAN_ACCOUNT = credit product ledger
MPESA_WALLET = external wallet node/account
```

---

## 10. Pattern Contract

Every pattern implementation must return:

```python
@dataclass
class PatternResult:
    pattern_id: str
    pattern_name: str
    actor_member_ids: list[str]
    account_ids: list[str]
    txn_ids: list[str]
    edge_ids: list[str]
    start_ts: datetime
    end_ts: datetime
    is_suspicious: bool
    typology: str | None
```

Every pattern must implement:

```python
class Pattern(Protocol):
    name: str
    pattern_type: Literal["NORMAL", "SUSPICIOUS"]

    def select_candidates(self, graph, state, rng) -> list[Candidate]:
        ...

    def can_insert(self, candidate, state) -> bool:
        ...

    def insert(self, candidate, state, calendar, rng) -> PatternResult:
        ...

    def validate(self, result, state) -> list[ValidationError]:
        ...
```

---

## 11. v0.1 Normal Patterns

Implement exactly five normal patterns first.

---

## 11.1 SALARY_CHECKOFF_WALLET_SPEND

Applies to:

```text
SALARIED_TEACHER
COUNTY_WORKER
```

Sequence:

```text
1. Employer source pays gross salary.
2. Payroll checkoff sends BOSA contribution.
3. Payroll checkoff sends loan repayment if loan exists.
4. Net salary lands in FOSA.
5. Member transfers part of FOSA balance to M-Pesa wallet.
6. Wallet funds household spend, rent, school fees, cashout.
```

Required graph edges:

```text
MEMBER -EMPLOYED_BY-> EMPLOYER
MEMBER -HAS_ACCOUNT-> FOSA
MEMBER -HAS_ACCOUNT-> BOSA
MEMBER -HAS_WALLET-> MPESA_WALLET
```

---

## 11.2 SME_DAILY_RECEIPTS_MONDAY_DEPOSIT

Applies to:

```text
SME_OWNER
```

Sequence:

```text
1. Daily customer wallet/cash receipts accumulate.
2. Monday cash deposit into FOSA/business account.
3. Supplier payments go out via wallet/PesaLink/cash.
4. Occasional BOSA top-up.
```

---

## 11.3 FARMER_SEASONAL_INCOME

Applies to:

```text
FARMER_SEASONAL
```

Sequence:

```text
1. Harvest-month lump income.
2. BOSA top-up after harvest.
3. Farm-input purchases before planting.
4. Low-activity off-season.
```

---

## 11.4 DIASPORA_SUPPORT_HOUSEHOLD

Applies to:

```text
DIASPORA_SUPPORTED
```

Sequence:

```text
1. Remittance inflow.
2. Partial wallet cashout.
3. Household spend.
4. Occasional school-fee or rent payment.
```

---

## 11.5 LOAN_LIFECYCLE_NORMAL

Applies to:

```text
SALARIED_TEACHER
COUNTY_WORKER
SME_OWNER
FARMER_SEASONAL
BODA_BODA_OPERATOR
```

Sequence:

```text
1. Loan application.
2. Eligibility check against deposits/income.
3. Guarantor selection where required.
4. Disbursement to FOSA.
5. Monthly repayment.
6. Occasional arrears for selected members.
```

---

## 12. v0.1 Suspicious Patterns

Implement exactly two suspicious patterns first.

---

## 12.1 STRUCTURING

Meaning:

```text
Multiple deposits below an internal reporting/risk threshold within a short time window.
```

Pattern:

```yaml
STRUCTURING:
  candidate_personas:
    - SME_OWNER
    - BODA_BODA_OPERATOR
    - DIASPORA_SUPPORTED
  deposit_count_7d: [5, 12]
  amount_each_kes: [70000, 99000]
  window_days: [2, 7]
  rails:
    - CASH_BRANCH
    - CASH_AGENT
    - MPESA
```

Insertion logic:

```python
for i in range(n_deposits):
    emit_credit(
        source=internal_source("ILLICIT_CASH_SOURCE"),
        target=member_fosa,
        txn_type="FOSA_CASH_DEPOSIT" or "MPESA_PAYBILL_IN",
        amount=random_between(70000, 99000),
        timestamp=random_time_inside_window()
    )
```

Labeling:

```text
Each suspicious deposit gets an alerts_truth row.
One PATTERN_SUMMARY row links the full pattern.
```

Do not put typology labels in `transactions.csv`.

The internal source name must not be exported directly. The public debit account should be a generic `SOURCE_ACCOUNT` with a non-revealing ID such as `EXT_SRC_041`.

---

## 12.2 RAPID_PASS_THROUGH

Meaning:

```text
Large inflow enters an account and most value exits quickly to unrelated counterparties.
```

Pattern:

```yaml
RAPID_PASS_THROUGH:
  candidate_personas:
    - DIASPORA_SUPPORTED
    - SME_OWNER
    - CHURCH_ORG
  inflow_amount_kes: [100000, 750000]
  exit_ratio: [0.75, 0.98]
  exit_delay_hours: [1, 48]
  outflow_count: [2, 8]
```

Insertion logic:

```python
inflow = emit_credit(
    source=internal_source("PROCUREMENT_CORRUPTION_SOURCE") or internal_source("CYBER_FRAUD_SOURCE"),
    target=member_fosa,
    txn_type="PESALINK_IN" or "MPESA_PAYBILL_IN",
    amount=random_between(100000, 750000)
)

for sink in selected_sinks:
    emit_debit(
        source=member_fosa,
        target=sink,
        txn_type="PESALINK_OUT" or "MPESA_CASHOUT",
        amount=allocated_exit_amount,
        timestamp=inflow.timestamp + random_hours(1, 48)
    )
```

Labeling:

```text
Inflow = PLACEMENT
Outflows = LAYERING
Pattern summary = PATTERN_SUMMARY
```

The internal source name must not be exported directly. The public debit account should be a generic `SOURCE_ACCOUNT` with a non-revealing ID such as `EXT_SRC_087`.

---

## 13. Anti-Label-Leakage Rules

Do not create obvious suspicious-only artefacts in public feature files.

### Forbidden in transactions.csv

```text
is_suspicious
typology
pattern_id
alert_id
source_is_illicit
synthetic_flag
```

### Forbidden in members.csv

```text
criminal_flag
shell_flag
suspicious_member
injected_typology
```

### Forbidden in accounts.csv

```text
mule_account_flag
laundering_account_flag
```

### Allowed only in label files

```text
typology
truth_label
pattern_id
stage
severity
```

### Suspicious behaviour must overlap normal behaviour

For medium difficulty:

```text
1. Suspicious amounts overlap high-end SME/church/farmer behaviour.
2. Suspicious members still have normal transactions.
3. Suspicious accounts are not newly created by default.
4. Suspicious counterparties should not all be unique one-off obvious nodes.
```

---

## 14. Validation Report

The generator must output `validation_report.json`.

Required sections:

```json
{
  "schema_validation": {},
  "row_counts": {},
  "balance_validation": {},
  "graph_validation": {},
  "label_validation": {},
  "distribution_validation": {},
  "typology_validation": {},
  "warnings": []
}
```

---

## 14.1 Schema validation

Check:

```text
1. All required files exist.
2. All required columns exist.
3. Primary keys are unique.
4. Foreign keys resolve.
5. Enum values are valid.
6. Transaction timestamps are within simulation window.
7. Historical member join_date and account open_date may predate the simulation window, but may not be after the simulation end date.
```

---

## 14.2 Balance validation

Check:

```text
1. No customer/member account has negative balance unless explicitly allowed. `SOURCE_ACCOUNT` may go negative and `SINK_ACCOUNT` may grow positive.
2. balance_after_dr_kes and balance_after_cr_kes match ledger state.
3. Sum of account movements reconciles with exported current balances.
4. Loan disbursements match loan principal.
5. Loan repayments do not exceed outstanding balance.
```

---

## 14.3 Graph validation

Check:

```text
1. Every member has at least one account.
2. Every account belongs to exactly one member, source, sink, or institution.
3. Every transaction account appears in nodes.csv.
4. Every guarantor edge has matching guarantors.csv row.
5. Suspicious patterns form connected subgraphs.
```

---

## 14.4 Label validation

Check:

```text
1. Every suspicious transaction has alerts_truth coverage.
2. Every alerts_truth txn_id exists in transactions.csv.
3. Every suspicious pattern has one PATTERN_SUMMARY label row.
4. Normal patterns do not require PATTERN_SUMMARY rows in v0.1.
5. No typology labels appear in non-label files.
6. Suspicious prevalence matches config tolerance.
```

---

## 14.5 Distribution validation

Check at minimum:

```text
txns_per_member_per_month
salary_day_concentration >= 0.70 within payday window
wallet_to_cash_ratio
rural_cash_share_vs_urban
loan_utilization_rate
members_with_active_loans
school_fee_month_outflow_spike
december_spend_spike
suspicious_member_ratio
structuring_pattern_count
rapid_pass_through_pattern_count
```

---

## 15. Baseline AML Rules for v0.1

These rules are used to test the generated dataset.

### 15.1 Structured deposits

```python
if deposit_count_7d >= 5 \
   and max_single_deposit_7d < 100000 \
   and total_deposit_7d >= 300000:
    alert("STRUCTURING")
```

### 15.2 Rapid pass-through

```python
if inbound_value_48h >= 100000 \
   and outbound_value_48h / inbound_value_48h >= 0.75 \
   and retained_balance_ratio <= 0.25 \
   and outbound_counterparty_count >= 2:
    alert("RAPID_PASS_THROUGH")
```

---

## 16. CLI Target

```bash
python -m kenya_sacco_sim generate \
  --members 10000 \
  --months 12 \
  --institutions 5 \
  --suspicious-ratio 0.01 \
  --difficulty medium \
  --seed 42 \
  --output ./datasets/KENYA_SACCO_SIM_v0_1
```

Expected output:

```text
members.csv
accounts.csv
loans.csv
guarantors.csv
transactions.csv
nodes.csv
graph_edges.csv
alerts_truth.csv
manifest.json
validation_report.json
```

---

## 17. Manifest

`manifest.json` must include:

```json
{
  "dataset_name": "KENYA_SACCO_SIM",
  "version": "0.1.0",
  "seed": 42,
  "start_date": "2024-01-01",
  "end_date": "2024-12-31",
  "members": 10000,
  "institutions": 5,
  "suspicious_ratio": 0.01,
  "difficulty": "medium",
  "files": [],
  "created_at": "ISO-8601 timestamp"
}
```

---

## 18. First Implementation Milestones

### Milestone 1 — Static world generation

Deliver:

```text
members.csv
accounts.csv
nodes.csv
graph_edges.csv
```

Acceptance:

```text
All schemas valid.
Every member has required accounts.
Graph has valid member-account edges.
```

---

### Milestone 2 — Normal transaction generation

Deliver:

```text
transactions.csv
validation_report.json
```

Acceptance:

```text
No negative balances.
Payday clustering exists.
Wallet/cash/channel mix is plausible.
```

---

### Milestone 3 — Loans and guarantors

Deliver:

```text
loans.csv
guarantors.csv
loan-related transactions
```

Acceptance:

```text
Loan disbursements reconcile to loans.csv.
Guarantor links reconcile to graph_edges.csv.
Loan repayments reduce outstanding balances.
```

---

### Milestone 4 — Suspicious pattern injection

Deliver:

```text
STRUCTURING
RAPID_PASS_THROUGH
alerts_truth.csv
```

Acceptance:

```text
Each injected pattern is recoverable from transactions.
Each suspicious txn has label coverage.
No label leakage in feature files.
```

---

### Milestone 5 — Baseline rule benchmark

Deliver:

```text
baseline_rules.py
rule_results.json
```

Acceptance:

```text
Rules catch a meaningful share of injected typologies.
False positives exist but are not absurdly high.
Results are reproducible with same seed.
```

---

## 19. Deferred to v0.2

Do not implement these in v0.1 unless the core build is already stable:

```text
1. Guarantor fraud rings
2. Wallet funneling
3. Church/charity misuse
4. Dormant reactivation
5. Fake affordability before loans
6. Remittance layering
7. Graph neural network benchmark
8. 100,000+ member scale
9. Multi-difficulty benchmark suite
10. Full pattern_labels.csv and edge_labels.csv
```

---

## 20. Final Principle

```text
v0.1 should be boring, correct, and testable.

Do not build a beautiful simulator that cannot reconcile balances.
Do not build a graph benchmark before the CSV ledger is trustworthy.
Do not inject 12 typologies before 2 typologies are validated.

Build the smallest Kenya-specific SACCO AML simulator that proves the architecture works.
```
