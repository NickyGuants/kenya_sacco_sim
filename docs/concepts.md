# Concepts

You can use the generator without reading this page. You cannot
interpret the data without it.

## What a SACCO is

A SACCO — Savings and Credit Co-operative — is a member-owned financial
institution. In Kenya they are large, regulated, and fill a gap between
informal savings groups and full commercial banks. A typical Kenyan
SACCO has two sides:

- **BOSA** (Back Office Savings Activity): the original deposit and
  loan business. BOSA deposits are not withdrawable on demand. They are
  the collateral against which the SACCO lends.
- **FOSA** (Front Office Savings Activity): bank-like accounts you can
  pay in and out of, often with a card or a wallet link.

Members get loans backed partly by their own BOSA deposits and partly by
**guarantors** — other members who pledge their deposits to cover the
loan if the borrower defaults. The simulator models all of this:
share capital, BOSA deposits, FOSA savings and current accounts, loan
accounts, guarantor pledges, and arrears.

## How money moves in Kenya

Cash is still important but most everyday flows are digital and run over
a small set of rails. The simulator names them explicitly:

- **MPESA** and **AIRTEL_MONEY** — mobile money wallets. Used for
  person-to-person transfers, paybills, and tills.
- **PESALINK** — fast bank-to-bank rail.
- **EFT** and **RTGS** — slower bank rails for larger amounts.
- **CASH_BRANCH** and **CASH_AGENT** — physical cash, either over the
  counter at a branch or through an authorised agent.
- **PAYROLL_CHECKOFF** — the employer deducts SACCO contributions and
  loan repayments from salary before paying the member.
- **REMITTANCE** — money from abroad.

Each transaction in the dataset carries a `rail` and a `channel`.
Together they tell you how the money actually moved.

## Personas

Every member is one of seven personas. The persona drives income,
savings cadence, loan appetite, wallet adoption, and where they live:

```text
SALARIED_TEACHER
COUNTY_WORKER
SME_OWNER
FARMER_SEASONAL
DIASPORA_SUPPORTED
BODA_BODA_OPERATOR
CHURCH_ORG
```

`CHURCH_ORG` is the only `ORGANIZATION` member type — Sunday
collections, donor receipts, vendor and charity payments. Everyone else
is an `INDIVIDUAL`.

## Institution archetypes

Each SACCO institution in the simulated world has an archetype. The
archetype shifts the persona mix, how digital the institution is, how
much cash flows through it, and how heavily it leans on guarantors:

```text
TEACHER_PUBLIC_SECTOR
UNIFORMED_SERVICES
UTILITY_PRIVATE_SECTOR
COMMUNITY_CHURCH
FARMER_COOPERATIVE
SME_BIASHARA
DIASPORA_FACING
```

A `FARMER_COOPERATIVE` will look very different from a `DIASPORA_FACING`
SACCO in cash share, device coverage, and seasonal rhythms.

## What AML means here

Anti-Money Laundering is the discipline of finding accounts whose
behavior suggests an attempt to disguise the origin or destination of
funds. Real AML programs combine know-your-customer data, fixed
threshold rules, statistical models, and human investigators.

The simulator does the financial-data half of that loop. It generates
mostly normal activity and quietly injects a small fraction of members
into one of a few **typologies** — recognisable money-laundering
patterns:

- `STRUCTURING` — many small deposits sized to stay under a reporting
  threshold.
- `RAPID_PASS_THROUGH` — money lands and leaves again within hours.
- `FAKE_AFFORDABILITY_BEFORE_LOAN` — borrowed money is shuttled in
  before a loan application to fake creditworthiness.
- `DEVICE_SHARING_MULE_NETWORK` — several members coordinate from one
  shared device.
- `GUARANTOR_FRAUD_RING` — members manufacture credit support through
  circular or reciprocal guarantor links.

See [Typologies](typologies.md) for what each looks like in the data
and what rule the simulator's baseline detector applies.

## Labels and leakage

The labels live in `alerts_truth.csv`. They mark the suspicious patterns
the generator injected. **No other file** carries label columns. The
generator validates this — it will fail the run if a label-bearing field
sneaks into a feature file. This is the discipline that lets you train
a model on the feature files and trust its score on the test split.

There is also a "near-miss" concept. Some normal members happen to do
things that look suspicious without being injected typologies. The
validation report tracks them so you know how messy the negative class
is.

## Determinism

The generator is fully seeded. Every random draw flows from a single
seed in `WorldConfig`. Identical seed plus identical config plus
identical CLI flags produce identical CSVs and identical MD5 hashes in
`manifest.json`. This is non-negotiable: it is what makes the dataset a
benchmark instead of an experiment.
