"""One-shot script: inject 'assumptions & decisions' markdown into the
getting-started notebook so the tutorial reflects every decision recorded in
kenya_sacco_sim_v_0_1_specification.md, kenya_sacco_sim_v_0_2_specification.md,
and deep-research-report.md.

Idempotent: re-running replaces the injected cells in place rather than
duplicating them. Each injected cell is tagged with a sentinel HTML comment so
this script can find and remove prior injections before re-inserting. The
script also rewrites the dataset directory reference in the intro markdown
and the ``DATASET_DIR`` code cell so the notebook tracks the latest exported
benchmark.
"""
from __future__ import annotations

import json
from pathlib import Path

NB = Path(__file__).resolve().parent / "01_getting_started.ipynb"
SENTINEL = "<!-- KSS_ASSUMPTIONS -->"

# Update this when the canonical demo dataset is regenerated.
LATEST_DATASET_DIR = "KENYA_SACCO_SIM_v02_10k_review_fix"
LEGACY_DATASET_DIRS = ("KENYA_SACCO_SIM_m5_10k",)


def md(src: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [SENTINEL + "\n", *[ln + "\n" for ln in src.splitlines()]],
    }


def retarget_dataset_dir(cells: list[dict]) -> int:
    """Replace any reference to a known legacy dataset directory with the
    latest one. Returns the number of substitutions performed."""
    n = 0
    for cell in cells:
        new_src = []
        for line in cell["source"]:
            for legacy in LEGACY_DATASET_DIRS:
                if legacy in line:
                    line = line.replace(legacy, LATEST_DATASET_DIR)
                    n += 1
            new_src.append(line)
        cell["source"] = new_src
    return n


# Full-source replacements for specific cells. Each entry is
# ``(cell_type, marker, applied_signature, new_source)``:
#   * ``marker`` uniquely identifies the *original* cell to replace.
#   * ``applied_signature`` is a substring guaranteed to appear in
#     ``new_source`` but never in the original; it lets the patcher
#     recognise an already-patched cell and stay idempotent.
CELL_PATCHES: list[tuple[str, str, str, str]] = [
    (
        "markdown",
        "## 0. Prerequisites",
        "pip install -r requirements-notebooks.txt",
        """## 0. Prerequisites

The generator itself has zero third-party dependencies. This tutorial uses
pandas, scikit-learn, matplotlib, and networkx for analysis only. Install
them once from the repo root:

```bash
pip install -r requirements-notebooks.txt
```

The next cell verifies the environment and aborts with a clear message if
any dependency is missing.""",
    ),
    (
        "code",
        "%pip install",
        "Preflight: confirm the notebook deps",
        """# Preflight: confirm the notebook deps are importable.
import importlib.util

_required = ('pandas', 'sklearn', 'matplotlib', 'networkx')
_missing = [m for m in _required if importlib.util.find_spec(m) is None]
if _missing:
    raise SystemExit(
        f'Missing notebook deps: {_missing}. From the repo root run:\\n'
        '    pip install -r requirements-notebooks.txt'
    )
print('All notebook dependencies present.')""",
    ),
    (
        "code",
        "alerts['typology'].value_counts().plot.bar",
        "counts = alerts['typology'].value_counts()",
        """counts = alerts['typology'].value_counts()
fig, ax = plt.subplots()
ax.bar(counts.index, counts.values, color='#1f77b4')
ax.set_title('Suspicious patterns by typology')
ax.set_ylabel('count')
plt.tight_layout()""",
    ),
]


def apply_cell_patches(cells: list[dict]) -> int:
    """Replace whole-cell sources matched by ``CELL_PATCHES``. Idempotent:
    a cell whose source already contains ``applied_signature`` is left
    alone."""
    n = 0
    for cell_type, marker, applied_signature, new_source in CELL_PATCHES:
        if any(
            c["cell_type"] == cell_type and applied_signature in "".join(c["source"])
            for c in cells
        ):
            continue
        matches = [
            c for c in cells
            if c["cell_type"] == cell_type and marker in "".join(c["source"])
        ]
        if len(matches) != 1:
            raise SystemExit(
                f"CELL_PATCHES marker {marker!r} matched {len(matches)} {cell_type} cell(s); expected exactly 1"
            )
        matches[0]["source"] = [ln + "\n" for ln in new_source.splitlines()]
        n += 1
    return n


# Insertions are anchored by the *opening line* of the section markdown
# they should be placed near. Default placement is *after* the anchor (so the
# injected cell sits between a section header and its first code cell). Pass
# ``where="before"`` to insert immediately ahead of the anchor instead.
INSERTIONS: list[tuple[str, str, str]] = [
    # Top of notebook: global design assumptions
    (
        "after",
        "# KENYA_SACCO_SIM — Getting Started",
        """## Design assumptions and decisions

The dataset is generated under a frozen **v0.1 specification**, extended by
**v0.2** which adds support entities, institution archetypes, a device
layer, one new typology, and a multi-seed stability harness. The full source
documents are `kenya_sacco_sim_v_0_1_specification.md`,
`kenya_sacco_sim_v_0_2_specification.md`, and `deep-research-report.md`.
Everything in this notebook follows from those three documents.

> **correctness > realism > scale.**
>
> A small, ledger-reconciled, well-labelled dataset is more useful than a
> large unreconciled one. v0.1 froze that contract at 10k members; v0.2
> deepens the world without changing existing file schemas.

### Architectural decisions (carried from v0.1)

- **Pattern-first generation.** No transaction is emitted at random. Every
  ledger event comes from a named behavioural pattern (e.g.
  `SALARY_CHECKOFF_WALLET_SPEND`, `SME_DAILY_RECEIPTS_MONDAY_DEPOSIT`,
  `STRUCTURING`). This is what makes alerts explainable.
- **Graph-first world.** The simulator first materialises a typed graph of
  institutions, members, accounts, wallets, employers, branches, agents,
  devices, sources, and sinks. Transactions are emitted *over* that graph,
  so `nodes.csv` / `graph_edges.csv` are first-class outputs.
- **Deterministic seed hierarchy.** All randomness derives from a single
  `WorldConfig.seed`. Child seeds are computed as
  `institution_seed = hash(global_seed, institution_id)`,
  `member_seed = hash(global_seed, member_id)`,
  `pattern_seed = hash(global_seed, pattern_id)`, so parallel generation
  reproduces bit-identical outputs.
- **Canonical conventions.** `amount_kes` is **always positive**; direction
  is determined only by `account_id_dr` / `account_id_cr`. Timestamps are
  Africa/Nairobi (EAT, UTC+3), ISO-8601. Currency is KES.
- **Compatibility contract (v0.2 §2).** Every v0.1 file is still emitted,
  unchanged in schema. v0.2 only *adds* support files
  (`institutions.csv`, `branches.csv`, `agents.csv`, `employers.csv`,
  `devices.csv`) and four new sections inside `validation_report.json`.

### Mandatory Kenyan SACCO / payment assumptions (v0.1 spec §6)

1. Every formal salaried member has at least one FOSA account.
2. Every normal SACCO member has a BOSA deposit account.
3. Share capital is distinct from deposits.
4. Loan accounts are distinct from FOSA and BOSA accounts.
5. Payroll check-off can split salary into loan repayment, BOSA deposit
   contribution, and net salary to FOSA.
6. M-Pesa / wallet behaviour exists for most members.
7. Guarantor-backed loans exist for development and emergency loans.
8. School-fee seasonality exists in January, May, and August.
9. December has elevated spend / remittance / cash-out activity.
10. Rural members use more cash / agent transactions than urban members.

### Why this is "mobile-money-first"

Kenya is, structurally, a wallet-dominated retail-finance environment. The
research report anchors the simulator to:

- ~82.4M registered mobile-money users and ~36.9M 30-day actives (CBK, Dec 2024).
- 82.3% of adults using mobile money, 52.6% using it **daily**; daily bank
  use only 4.8% (FinAccess 2024).
- 6.84M SASRA-regulated SACCO members, KES 758.6B gross loans, KES 682.2B
  deposits across 357 SACCOs in 2023; 53 SACCOs hold 73% of assets.
- Diaspora remittances of USD 4.945B in 2024, ~51% from the US corridor.

That is why every persona has a wallet propensity, why `MPESA` and
`PESALINK` are first-class rails, and why v0.2 institutions carry an
explicit `digital_maturity` and `cash_intensity` rather than a uniform
prior.

### v0.2 scope additions

- **5 v0.1 normal patterns** are still the only normal-behaviour
  generators: salary check-off + wallet spend, SME daily receipts + Monday
  deposit, farmer seasonal income, diaspora support, loan lifecycle.
- **3 suspicious typologies** are now injected: `STRUCTURING` and
  `RAPID_PASS_THROUGH` (v0.1) plus `FAKE_AFFORDABILITY_BEFORE_LOAN` (v0.2).
- **Configuration is externalised.** Eight YAML files under `config/` drive
  the run (`world.yaml`, `personas.yaml`, `products.yaml`,
  `institutions.yaml`, `patterns.yaml`, `typologies.yaml`, `calendar.yaml`,
  `validation.yaml`); the manifest records which were loaded.
- **Institution archetypes** assign every SACCO one of
  `TEACHER_PUBLIC_SECTOR`, `UNIFORMED_SERVICES`, `UTILITY_PRIVATE_SECTOR`,
  `COMMUNITY_CHURCH`, `FARMER_COOPERATIVE`, `SME_BIASHARA`, or
  `DIASPORA_FACING`, which biases persona mix, channel mix, and loan style.
- **Device layer baseline.** Digital channels (`MOBILE_APP`, `USSD`,
  `PAYBILL`, `TILL`, `BANK_TRANSFER`) populate `device_id`; shared-device
  groups exist at low rates as a baseline (typology-free in v0.2).
- **Multi-seed stability harness.** `python3 -m kenya_sacco_sim benchmark`
  emits `multi_seed_results.json` with per-seed metrics and a stability
  report; the v0.2 release gate is per-typology precision/recall range
  ≤ 0.10 across configured seeds.
- **Deferred to v1**: guarantor rings, dormant reactivation, remittance
  layering, wallet funnels, device-sharing typologies, GNN benchmarks,
  100k+ scale.""",
    ),
    # Members section: persona priors and income anchors
    (
        "after",
        "## 2. Members",
        """**Decisions encoded here.** Persona shares, monthly-income tertiles, wallet
adoption probability, rural probability, and annual loan probability are all
fixed in the spec (`personas.yaml`, §7.2) and implemented in
`core.config.PERSONA_CONFIG`. The income tertiles are calibrated so the
formal-salaried cohorts centre near the KNBS 2024 public-sector mean of
**KSh 933.1k / year (≈ KSh 77.8k / month)**:

| Persona | Share | Monthly income tertiles (KES) | Wallet | Rural | Loan p/yr |
|---|---:|---|---:|---:|---:|
| `SALARIED_TEACHER` | 22% | 45k / 78k / 120k | 0.95 | 0.35 | 0.35 |
| `SME_OWNER` | 18% | 30k / 120k / 300k | 0.98 | 0.25 | 0.25 |
| `FARMER_SEASONAL` | 17% | 10k / 35k / 150k | 0.80 | 0.85 | 0.20 |
| `BODA_BODA_OPERATOR` | 15% | 20k / 45k / 80k | 0.98 | 0.40 | 0.22 |
| `COUNTY_WORKER` | 13% | 35k / 65k / 95k | 0.95 | 0.30 | 0.30 |
| `DIASPORA_SUPPORTED` | 10% | 15k / 50k / 180k | 0.95 | 0.45 | 0.15 |
| `CHURCH_ORG` | 5% | 30k / 150k / 600k | 0.90 | 0.50 | 0.10 |

`SHELL_MEMBER` is **deliberately absent**. v0.1 injects suspicious behaviour
into plausible ordinary personas (typically `SME_OWNER`, `BODA_BODA_OPERATOR`,
`DIASPORA_SUPPORTED`, `CHURCH_ORG`) so detectors cannot trivially win by
learning a "criminal persona" feature.""",
    ),
    # Accounts section: BOSA/FOSA, source/sink, loan semantics, v0.2 support entities
    (
        "after",
        "## 3. Accounts",
        """**Decisions encoded here.**

- **BOSA vs FOSA vs share capital vs loan account.** These are *distinct*
  account types with different semantics:
  - `BOSA_DEPOSIT` — long-term, non-withdrawable, drives loan eligibility.
  - `FOSA_SAVINGS` / `FOSA_CURRENT` — transactional, withdrawable.
  - `SHARE_CAPITAL` — ownership equity, non-withdrawable; never debited by a
    `FOSA_CASH_WITHDRAWAL` or `MPESA_CASHOUT`.
  - `LOAN_ACCOUNT` — `current_balance_kes` is **positive outstanding
    principal**. Disbursement increases it; repayment decreases it; a fully
    repaid loan reads zero. This avoids negative-liability sign confusion.
- **Why `SOURCE_ACCOUNT` and `SINK_ACCOUNT` exist.** External money
  (employer payroll, remittances, cash entering a branch, supplier
  payments leaving the system) has to debit or credit *something* to keep
  the ledger double-entry. Source / sink accounts absorb those flows.
  They may carry negative or unbounded positive balances and are
  **excluded from member-balance validation**. Public IDs are generic
  (e.g. `EXT_SRC_041`); the spec forbids leaking labels like
  `ILLICIT_CASH_SOURCE` into exported files.
- **Required account setup** for an individual member: 1 `BOSA_DEPOSIT`,
  1 `FOSA_SAVINGS` or `FOSA_CURRENT`, 1 `SHARE_CAPITAL`, optionally 1
  `MPESA_WALLET`, plus zero or more `LOAN_ACCOUNT`s.

The SASRA distinction between deposit-taking SACCOs (177, FOSA-capable) and
specified non-deposit-taking SACCOs (178, BOSA-only) is the reason both
account types exist as first-class concepts even though the v0.1 institution
mix simulates DT-style SACCOs.

### v0.2 support entities (new files, additive)

`accounts.csv` no longer stands alone. v0.2 promotes the infrastructure
that members and accounts attach to into authoritative tabular files
alongside `nodes.csv` / `graph_edges.csv`:

| File | Role | Key columns (excerpt) |
|---|---|---|
| `institutions.csv` | One row per SACCO. | `institution_id`, `archetype`, `county`, `urban_rural`, `digital_maturity`, `cash_intensity`, `loan_guarantor_intensity` |
| `branches.csv` | Physical branches a member or `CASH_BRANCH` transaction can resolve to. | `branch_id`, `institution_id`, `county`, `urban_rural` |
| `agents.csv` | M-Pesa / SACCO agent endpoints used by `CASH_AGENT` and `MPESA_*` rails. | `agent_id`, `institution_id`, `county`, `agent_type` |
| `employers.csv` | Payroll counterparties used by `PAYROLL_CHECKOFF`. | `employer_id`, `sector`, `payroll_day`, `county` |
| `devices.csv` | Mobile / web devices recorded for digital channels. | `device_id`, `device_type`, `os_family`, `is_shared` |

Foreign keys must resolve: every `institution_id` referenced by a member,
branch, agent, or employer; every `device_id` referenced by a transaction;
every agent/branch attached to a cash transaction. The validator's
`support_entity_validation` block aborts export on any unresolved FK or
duplicate primary key. `nodes.csv` / `graph_edges.csv` continue to be
emitted as graph projections over the same entities.""",
    ),
    # Transactions section: rails, calendar, rural/urban
    (
        "after",
        "## 4. Transactions",
        """**Decisions encoded here.**

- **Rails are explicit, not collapsed into "transfer".** The taxonomy
  separates `SACCO_INTERNAL`, `MPESA`, `AIRTEL_MONEY`, `PESALINK`, `EFT`,
  `RTGS`, `CASH_BRANCH`, `CASH_AGENT`, `PAYROLL_CHECKOFF`, `REMITTANCE`.
  This split is required because they have very different AML profiles —
  PesaLink is real-time and useful for rapid pass-through; RTGS / KEPSS is
  for treasury (~KSh 5M average payment); cash via agent allows
  third-party deposits the original depositor cannot be identified for.
- **Calendar effects (`calendar.yaml`).**
  - `payday_days = [24..31]` — formal salary clusters at month-end and the
    distribution validator requires ≥70% of salary events fall in this
    window.
  - `school_fee_months = [1, 5, 8]` — household outflow spike for fees.
  - `harvest_months = [3, 4, 8, 9, 12]` — `FARMER_SEASONAL` income lumps.
  - `december_spend_multiplier = 1.5` — holiday remittance / spend bump.
  - `weekend_wallet_multiplier = 1.3`, `monday_sme_deposit_multiplier = 1.4`.
- **Rural / urban channel mix.** `urban_rural` on each member biases
  channel selection: rural members are ≥1.2x more likely to use
  `CASH_BRANCH` / `CASH_AGENT`; urban members lean wallet / app / USSD.
- **No label leakage in `transactions.csv`.** The spec forbids any of
  `is_suspicious`, `typology`, `pattern_id`, `alert_id`,
  `source_is_illicit`, `synthetic_flag` in this file. Labels live
  exclusively in `alerts_truth.csv`.
- **Operational zero-amount events** (`KYC_REFRESH`,
  `ACCOUNT_REACTIVATION`) are emitted as ledger rows because future
  typologies depend on dormant-reactivation timing. `SYSTEM_CORRECTION`
  is dev-only and absent from released benchmarks unless the manifest
  declares otherwise.""",
    ),
    # Loans section: deposit multiples, guarantor rules
    (
        "after",
        "## 5. Loans and guarantors",
        """**Decisions encoded here.**

- **Six loan products** (`products.yaml`), each with a deposit multiplier
  and guarantor requirement: `DEVELOPMENT_LOAN` (3.0x, guarantors
  required), `BIASHARA_LOAN` (2.5x, required), `ASSET_FINANCE` (3.0x,
  required), `SCHOOL_FEES_LOAN` (2.0x, optional), `EMERGENCY_LOAN` (1.5x,
  optional), `SALARY_ADVANCE` (1.0x, none).
- **Deposit-multiple rule.** Approved principal cannot exceed
  `deposit_multiplier × member's BOSA balance at application`. The
  research report anchors this to the Kenya Police SACCO FAQ ("borrow up
  to three times savings"); the simulator also enforces the Kenyan
  two-thirds net-pay rule on payroll-attachable income.
- **Guarantor mechanics.** Guarantors must be active SACCO members with
  sufficient deposits to pledge; each guarantor's
  `guarantor_capacity_remaining_kes` is decremented at pledge and restored
  at release. Relationship type (`COWORKER`, `FAMILY`, `CHURCH_MEMBER`,
  …) is captured to support future guarantor-ring detection.
- **Repayment routes.** `PAYROLL_CHECKOFF` (the dominant mode for salaried
  members), `MANUAL_FOSA_TRANSFER`, `MPESA_PAYBILL`, or `CASH_BRANCH`.
  Each repayment posts a `LOAN_REPAYMENT` ledger row that decreases
  outstanding principal — the balance engine refuses to repay more than
  is owed.""",
    ),
    # Labels section: anti-leakage discipline, overlay rule, v0.2 typology
    (
        "after",
        "## 6. The labels — `alerts_truth.csv`",
        """**Decisions encoded here.**

- **Three typologies in v0.2.** The two v0.1 typologies are unchanged;
  v0.2 adds one SACCO-credit-engine typology (spec §9.1):

  - `STRUCTURING` — 5–12 sub-threshold deposits (KES 70k–99k each) over
    a 2–7 day window into a member's FOSA via `CASH_BRANCH`,
    `CASH_AGENT`, or `MPESA`. Candidate personas: `SME_OWNER`,
    `BODA_BODA_OPERATOR`, `DIASPORA_SUPPORTED`.
  - `RAPID_PASS_THROUGH` — single inflow of KES 100k–750k followed
    within 1–48h by 2–8 outflows draining 75–98% of the value to
    unrelated counterparties. Candidate personas: `DIASPORA_SUPPORTED`,
    `SME_OWNER`, `CHURCH_ORG`.
  - `FAKE_AFFORDABILITY_BEFORE_LOAN` *(v0.2 only)* — within the 30-day
    lookback before a loan application, 2–5 temporary non-salary
    external credits (KES 25k–150k each, on `REMITTANCE`, `MPESA`,
    `PESALINK`, or `CASH_BRANCH`) push the apparent affordability
    upward: external-credit share ≥ 0.55 of inflows and balance growth
    ≥ KES 50k. Candidate personas: `SME_OWNER`, `DIASPORA_SUPPORTED`,
    `COUNTY_WORKER`, `SALARIED_TEACHER`. Eligible products:
    `DEVELOPMENT_LOAN`, `SCHOOL_FEES_LOAN`, `BIASHARA_LOAN`. The
    deterministic baseline rule for this typology is **expected to have
    low precision** — legitimate large remittances ahead of a loan are
    common — so false positives are an intended ambiguity, not a
    validation failure (spec §9.1).

- **Anti-leakage discipline (spec §13).** Label columns are forbidden in
  feature files. The validator (`validation.forbid_label_leakage = true`)
  fails the run if any of these appear outside `alerts_truth.csv`:

  | File | Forbidden columns |
  |---|---|
  | `transactions.csv` | `is_suspicious`, `typology`, `pattern_id`, `alert_id`, `source_is_illicit`, `synthetic_flag` |
  | `members.csv` | `criminal_flag`, `shell_flag`, `suspicious_member`, `injected_typology` |
  | `accounts.csv` | `mule_account_flag`, `laundering_account_flag` |
  | `loans.csv` | typology / pattern markers of any kind |

- **Behavioural overlay, not cartoon criminals.** Suspicious members
  also produce normal traffic; suspicious amounts overlap legitimate
  high-end SME / church / farmer behaviour; suspicious accounts are
  generally not freshly created; suspicious counterparties are not all
  unique one-off nodes. The v0.2 fake-affordability typology specifically
  requires that suspicious members retain ≥ 50% normal transaction
  share (spec §12 Milestone 9 acceptance gate).

- **Stage labels** distinguish `PLACEMENT`, `LAYERING`, `INTEGRATION`,
  and `PATTERN_SUMMARY` — the last is one row per injected pattern,
  required by the validator to exist for every suspicious pattern. For
  `FAKE_AFFORDABILITY_BEFORE_LOAN`, every suspicious pre-application
  credit gets its own `alerts_truth` row and the loan account / member
  receives pattern context through additional rows.

- **Suspicious-member ratio target = 1%** (`world.suspicious_member_ratio
  = 0.01`), with a tolerance of ±0.2% enforced by distribution
  validation.""",
    ),
    # Splits section: leakage checks
    (
        "after",
        "## 7. Official splits",
        """**Decisions encoded here.**

- **Member-level partition.** Every member is assigned to exactly one of
  `train` / `val` / `test`. All transactions, accounts, loans, and
  guarantor edges for a member follow that member's split — there is no
  edge that crosses splits for the same actor.
- **Pattern-level disjointness.** Every suspicious pattern (and therefore
  every `pattern_id` in `alerts_truth.csv`) belongs to exactly one split.
  This blocks the most subtle leakage path, where two halves of the same
  injected typology end up on opposite sides of the train / test
  boundary.
- **Anti-leakage gates** are recomputed on every run and recorded in
  `validation_report.json` under `label_validation` and
  `distribution_validation`. A failed gate aborts export.""",
    ),
    # Validation gates: new section before "Where to go next"
    (
        "before",
        "## 11. Where to go next",
        """## 10b. Validation gates every release must pass

The generator refuses to write a dataset that fails any of these checks
(implemented in `validation/`, summarised in `validation_report.json`).
The v0.1 gates listed first are the foundation; the v0.2 gates below
them (spec §10) are additive and equally hard.

**v0.1 foundation gates**

- **Schema** — required files exist, columns present, primary keys
  unique, foreign keys resolve, enums valid, timestamps inside the
  simulation window.
- **Balance** — no member account goes negative; `balance_after_*` fields
  match the running ledger; loan disbursements equal loan principal;
  loan repayments never exceed outstanding balance. `SOURCE` /
  `SINK` accounts are explicitly excluded from this check.
- **Graph** — every member has at least one account; every account
  belongs to exactly one member, source, sink, or institution; every
  account / member referenced by a transaction appears in `nodes.csv`;
  every guarantor edge has a matching `guarantors.csv` row; every
  injected suspicious pattern forms a connected subgraph.
- **Label** — every suspicious transaction has `alerts_truth` coverage;
  every `alerts_truth.txn_id` exists in `transactions.csv`; every
  suspicious pattern has exactly one `PATTERN_SUMMARY` row; suspicious
  prevalence matches `world.suspicious_member_ratio` ± tolerance.
- **Distribution** — salary-day concentration ≥ 0.70 within the payday
  window; school-fee-month outflow spike present; December spend spike
  present; rural cash share > urban cash share; loan utilisation,
  per-member transaction counts, and structuring / pass-through counts
  all within configured bands.
- **Leakage** — no forbidden label column appears in any feature file;
  member and pattern IDs are disjoint across splits.

**v0.2 additions** (new sections inside `validation_report.json`)

- **`support_entity_validation`** — primary keys in `institutions.csv`,
  `branches.csv`, `agents.csv`, `employers.csv`, `devices.csv` are
  unique; every foreign key referenced from members, transactions, or
  graph edges resolves to an existing support row. Hard error on any
  duplicate or unresolved FK.
- **`device_validation`** — every `transactions.device_id` appears in
  `devices.csv` *and* `nodes.csv`; every `DEVICE` node has at least one
  `USES_DEVICE` edge; digital-channel device coverage is reported and
  warned on if unexpectedly low; the shared-device baseline is reported
  and warned on if unexpectedly high.
- **`institution_archetype_metrics`** — per-archetype member counts,
  channel mix, loan style, and split distribution. Warns if the
  archetype distribution is too concentrated.
- **`fake_affordability_validation`** — every
  `FAKE_AFFORDABILITY_BEFORE_LOAN` label resolves to existing loans,
  members, accounts, and transactions; suspicious members retain ≥ 50%
  normal transaction share; baseline rule TP / FP / FN counts are
  reported.
- **Benchmark / split** — `institution_split_max_share` is reported and
  warns if > 0.80 (so no single SACCO dominates a split); the multi-seed
  harness records per-typology precision/recall ranges and the v0.2
  release gate is **range ≤ 0.10 across configured seeds**.

Open `datasets/KENYA_SACCO_SIM_v02_10k_review_fix/validation_report.json`
to see every gate's pass / fail status for the run this notebook reads.

""",
    ),
]


def first_line(cell: dict) -> str:
    src = cell["source"]
    if not src:
        return ""
    return (src[0] if isinstance(src, list) else src.splitlines()[0]).strip()


def is_injected(cell: dict) -> bool:
    return cell["cell_type"] == "markdown" and any(
        SENTINEL in (ln if isinstance(ln, str) else "") for ln in cell["source"]
    )


def main() -> None:
    nb = json.loads(NB.read_text())
    cells = [c for c in nb["cells"] if not is_injected(c)]

    retargeted = retarget_dataset_dir(cells)
    if retargeted:
        print(f"Retargeted {retargeted} dataset-path reference(s) -> {LATEST_DATASET_DIR}")

    patched = apply_cell_patches(cells)
    if patched:
        print(f"Applied {patched} cell patch(es)")

    anchors = {anchor: (where, new_md) for where, anchor, new_md in INSERTIONS}
    out: list[dict] = []
    for cell in cells:
        if cell["cell_type"] == "markdown":
            head = first_line(cell)
            for anchor, (where, new_md) in list(anchors.items()):
                if head.startswith(anchor) and where == "before":
                    out.append(md(new_md))
                    anchors.pop(anchor)
                    break
        out.append(cell)
        if cell["cell_type"] != "markdown":
            continue
        head = first_line(cell)
        for anchor, (where, new_md) in list(anchors.items()):
            if head.startswith(anchor) and where == "after":
                out.append(md(new_md))
                anchors.pop(anchor)
                break

    if anchors:
        missing = ", ".join(repr(a) for a in anchors)
        raise SystemExit(f"Anchors not found in notebook: {missing}")

    nb["cells"] = out
    NB.write_text(json.dumps(nb, indent=1))
    print(f"Wrote {NB} with {len(out)} cells (added {len(INSERTIONS)} assumption blocks)")


if __name__ == "__main__":
    main()
