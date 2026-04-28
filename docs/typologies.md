# Typologies

A typology is a recognisable suspicious financial pattern. The generator
injects typologies as behavioral overlays on otherwise normal members. For each
typology there is:

```text
injector
deterministic rule baseline
truth labels in alerts_truth.csv
candidate IDs and rule metrics in rule_results.json
validation checks
normal near-misses where applicable
```

Typologies are emitted only when `--with-typologies` is passed. Credit-linked
typologies also require `--with-loans`.

Executable rule parameters live in `src/kenya_sacco_sim/core/rules.py`.
Injection parameters live in `config/typologies.yaml`.

## STRUCTURING

A member breaks one large placement event into many smaller inbound deposits
under the KES 100,000 reporting threshold.

Injector behavior:

- Can use any persona with a suitable member account, so persona alone is not a
  stable label shortcut.
- Posts 5 to 12 inbound deposits.
- Spreads them across 2 to 7 days.
- Uses cash, M-Pesa, and bank-like inbound rails.
- Randomizes pattern start windows across the simulation year.
- Mixes in near-miss normal behavior so the rule is not perfectly clean.

Rule contract:

```text
same member
>=5 inbound deposits under KES 100,000
within a 7-day window
total counted deposits >= KES 300,000
inbound types:
  FOSA_CASH_DEPOSIT
  BUSINESS_SETTLEMENT_IN
  MPESA_PAYBILL_IN
  PESALINK_IN
```

## RAPID_PASS_THROUGH

Large inbound value lands and most of it exits quickly through multiple
counterparties.

Injector behavior:

- Can use any persona with a suitable member account.
- Plants inbound value between KES 100,000 and KES 750,000.
- Exits 75% to 98% of value within 1 to 48 hours.
- Uses 2 to 8 outbound transfers.
- Randomizes pattern start windows across the simulation year.
- Leaves false positives and false negatives in the rule baseline.

Rule contract:

```text
same account only
inbound value within 48 hours >= KES 100,000
outbound value / inbound value >= 0.75
outbound counterparties >= 2
inbound types:
  PESALINK_IN
  MPESA_PAYBILL_IN
  BUSINESS_SETTLEMENT_IN
outbound types:
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
excluded:
  LOAN_REPAYMENT
  CHECKOFF_LOAN_RECOVERY
  HOUSEHOLD_SPEND_OUT
  BOSA_DEP_TOPUP
  MPESA_WALLET_TOPUP
```

## FAKE_AFFORDABILITY_BEFORE_LOAN

A borrower receives temporary non-salary external inflows before a loan
application, making affordability look stronger than the member's stable income
would support.

This typology only fires when `--with-loans` is on.

Injector behavior:

- Picks real loan applicants.
- Posts 2 to 5 external credits in the 30 days before application.
- Uses normal-looking sources such as cash, PesaLink, M-Pesa paybill, and
  business settlements.
- Keeps the member blended with normal activity.

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

This typology is intentionally noisy. Normal borrowers can receive legitimate
large pre-loan inflows, so low rule precision is expected.

## DEVICE_SHARING_MULE_NETWORK

Several members coordinate suspicious digital movement through one shared
device. This typology uses the existing device baseline and graph projection.

Injector behavior:

- Forms suspicious groups of 3 to 5 members.
- Reuses a real shared `device_id` across the group.
- Creates inbound and outbound digital activity in a 30-day window.
- Randomizes group start windows across the simulation year.
- Keeps every suspicious member above the normal-activity blending threshold.
- Leaves normal shared-device usage in the dataset as near-miss behavior.

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
inbound types:
  PESALINK_IN
  MPESA_PAYBILL_IN
  BUSINESS_SETTLEMENT_IN
outbound types:
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
  MPESA_WALLET_TOPUP
```

Normal shared-device groups are allowed. Suspicion requires the shared-device
graph pattern plus value, transaction-count, member-count, and outbound-share
thresholds.

## GUARANTOR_FRAUD_RING

Several members create reciprocal or circular guarantee relationships so that
loan approvals appear supported by independent guarantors when the support is
actually coordinated.

Injector behavior:

- Forms rings of 3 to 5 members with real active guaranteed loans.
- Adds reciprocal/circular rows to `guarantors.csv`.
- Relies on the existing `GUARANTEES` graph projection instead of adding label
  columns to feature files.
- Labels member and loan-context transaction rows while keeping the members
  blended with normal activity.

Rule contract:

```text
directed guarantee graph
active guaranteed loans only
strongly connected component size 3 to 6
cycle edges within component >= 3
products:
  DEVELOPMENT_LOAN
  BIASHARA_LOAN
  ASSET_FINANCE
```

## WALLET_FUNNELING

Many wallet or paybill credits from distinct counterparties fan into one member
account, then the value disperses quickly to several wallet, PesaLink, or
supplier counterparties.

Injector behavior:

- Can use any persona with a suitable FOSA account.
- Posts 6 to 10 inbound wallet/paybill/business-settlement credits in a 7-day
  fan-in window.
- Uses distinct counterparty hashes so the pattern is fan-in rather than one
  repeated payer.
- Disperses 58% to 82% of value within 72 hours after the last inbound credit.
- Keeps the member blended with normal activity.
- Leaves chama/project collection and low-fanout near-misses unlabeled.

Rule contract:

```text
same account only
fan-in window = 7 days
dispersion window = 72 hours
inbound count >= 6
inbound counterparties >= 5
inbound value >= KES 350,000
outbound value / inbound value >= 0.55
outbound counterparties >= 2
inbound types:
  MPESA_PAYBILL_IN
  WALLET_P2P_IN
  BUSINESS_SETTLEMENT_IN
outbound types:
  MPESA_WALLET_TOPUP
  WALLET_P2P_OUT
  PESALINK_OUT
  SUPPLIER_PAYMENT_OUT
```

## DORMANT_REACTIVATION_ABUSE

A member account that was dormant at simulation start is reactivated and then
used for large first-credit velocity and rapid fanout.

Injector behavior:

- Selects members with `dormant_flag=True`.
- Emits `KYC_REFRESH` and `ACCOUNT_REACTIVATION` before suspicious movement.
- Posts a large first credit, then disperses most of it within seven days.
- Adds later low-volume normal activity so the member is not only a typology
  carrier.
- Leaves legitimate low-velocity dormant reactivations unlabeled.

Benchmark caveat: this typology is dormant-lifecycle-scoped by design. Do not
use raw `dormant_flag` as an ML feature when claiming behavioral detection
lift; hold it out or evaluate inside the dormant-member subset.

Rule contract:

```text
KYC_REFRESH and ACCOUNT_REACTIVATION observed
first credit >= KES 120,000
window = 7 days
outbound value / first credit >= 0.70
outflow count >= 2
outbound counterparties >= 2
```

## REMITTANCE_LAYERING

Remittance funds arrive and are rapidly redistributed through wallet, bank,
cash, or supplier channels.

Injector behavior:

- Uses remittance-like inbound rail and PesaLink-style bank transfer.
- Fans value out within a 72-hour window.
- Keeps legitimate family/remittance distribution near-misses unlabeled.

Rule contract:

```text
inbound rail = REMITTANCE
inbound txn type = PESALINK_IN
inbound amount >= KES 140,000
window = 72 hours
outbound value / inbound value >= 0.68
outflow count >= 3
outbound counterparties >= 3
```

## CHURCH_CHARITY_MISUSE

A church/org or chama account receives an abnormal donor inflow and disperses
funds outside its normal collection and project-payment rhythm.

Injector behavior:

- Uses `CHURCH_ORG` and `CHAMA_GROUP` members.
- Posts abnormal donor inflows and vendor/individual dispersals.
- Keeps legitimate church project and charity disbursement near-misses
  unlabeled.

Benchmark caveat: this typology is organization-scoped by design. Hold out or
stratify by `persona_type` and `member_type` before making ML lift claims.

Rule contract:

```text
candidate personas:
  CHURCH_ORG
  CHAMA_GROUP
inbound amount >= KES 180,000
window = 96 hours
outbound value / inbound value >= 0.55
outflow count >= 3
outbound counterparties >= 3
```

## How Labels Appear

Every injected typology produces rows in `alerts_truth.csv`:

- one `PATTERN` row summarising the pattern
- one or more `MEMBER`, `ACCOUNT`, `TRANSACTION`, or `EDGE` rows for
  participating entities

The `pattern_id` links these rows together.

`pattern_labels.csv` is the one-row-per-`pattern_id` label table for unique
case counts, split joins, and benchmark summaries. `edge_labels.csv` is the
sparse graph label table for typologies with graph-backed edge context, such as
guarantor rings.

No feature file contains `typology`, `pattern_id`, `alert_id`, or truth-label
columns. Validation enforces this.

`alerts_truth.csv`, `pattern_labels.csv`, and `edge_labels.csv` are positive
injected truth only. They do not include `truth_label=False` rows for
historical false positives; deterministic rule false positives are candidates
outside the injected truth set. Use `pattern_labels.csv` or aggregate by
`pattern_id` for unique suspicious cases because the same case can have member,
account, transaction, and edge context rows.

## Current Typology Discipline

Every new typology must ship with:

```text
executable rule config
candidate IDs
false-positive IDs
false-negative IDs
normal near-misses
split coverage
leakage checks
dataset-card and docs updates
```

## Near-Miss Families

Near-misses are legitimate, unlabeled transactions that resemble suspicious
behavior enough to pressure rules and ML features. They must not appear in
`alerts_truth.csv`.

Current families:

```text
legitimate_structuring_like
  Target: STRUCTURING
  Effect: false_positive_pressure

incomplete_structuring
  Target: STRUCTURING
  Effect: negative_control

legitimate_sme_liquidity_sweep
  Target: RAPID_PASS_THROUGH
  Effect: false_positive_pressure

near_rapid_low_exit
  Target: RAPID_PASS_THROUGH
  Effect: negative_control

church_family_bulk_payments
  Target: STRUCTURING and RAPID_PASS_THROUGH
  Effect: negative_control

legitimate_preloan_affordability_candidate
  Target: FAKE_AFFORDABILITY_BEFORE_LOAN
  Effect: false_positive_pressure

near_affordability_low_growth
  Target: FAKE_AFFORDABILITY_BEFORE_LOAN
  Effect: negative_control

normal_shared_device_low_value
  Target: DEVICE_SHARING_MULE_NETWORK
  Effect: negative_control

legitimate_two_member_reciprocal_guarantee
  Target: GUARANTOR_FRAUD_RING
  Effect: negative_control

trusted_guarantor_star
  Target: GUARANTOR_FRAUD_RING
  Effect: negative_control

legitimate_chama_wallet_collection
  Target: WALLET_FUNNELING
  Effect: false_positive_pressure

near_wallet_funnel_low_fanout
  Target: WALLET_FUNNELING
  Effect: negative_control

legitimate_dormant_reactivation_low_velocity
  Target: DORMANT_REACTIVATION_ABUSE
  Effect: negative_control

legitimate_remittance_family_distribution
  Target: REMITTANCE_LAYERING
  Effect: false_positive_pressure

legitimate_church_project_disbursement
  Target: CHURCH_CHARITY_MISUSE
  Effect: false_positive_pressure
```

The generator reports these through:

```text
rule_results.json.near_miss_disclosure
validation_report.json.near_miss_validation
dataset_card.md
multi_seed_results.json.near_miss_stability
```
