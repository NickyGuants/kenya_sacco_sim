# Configuration

The generator is configured by YAML files in `./config/`. Every file is
optional. If a file is missing, the loader falls back to built-in defaults. CLI
flags always override loaded config.

Precedence:

```text
CLI flag > YAML file > built-in default
```

## Files

| File | What it controls |
| --- | --- |
| `world.yaml` | Top-level scale: members, institutions, months, seed, suspicious ratio, difficulty, date range, and currency. |
| `personas.yaml` | Population shares, income bands, wallet adoption, rural probability, and annual loan probability by persona. |
| `institutions.yaml` | Institution archetypes and digital/cash/guarantor intensity knobs. |
| `products.yaml` | Account and loan product catalogue. |
| `patterns.yaml` | Normal-pattern defaults and currently configured suspicious-pattern switches. |
| `typologies.yaml` | Injection parameters for suspicious typologies. |
| `calendar.yaml` | Payday, school-fee, harvest, December, weekend, and SME calendar effects. |
| `validation.yaml` | Validation tolerances and policies. |

## `world.yaml`

Shipping default:

```yaml
world:
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  currency: "KES"
  seed: 42
  months: 12
  institutions:
    count: 5
  members:
    count: 10000
  suspicious_member_ratio: 0.01
  difficulty: "medium"
```

`seed`, `members`, `institutions`, `months`, `suspicious_ratio`, and
`difficulty` are overridable from the CLI. `start_date`, `end_date`, and
`currency` are loaded from config/defaults.

## `personas.yaml`

Each persona block defines population share and behavior priors:

```yaml
personas:
  SALARIED_TEACHER:
    share: 0.22
    monthly_income_kes: [45000, 78000, 120000]
    wallet_adoption_probability: 0.95
    rural_probability: 0.35
    loan_probability_annual: 0.35
```

Persona shares should sum to `1.0`. `monthly_income_kes` is a triangular
distribution `[low, mode, high]`.

## `institutions.yaml`

Defines allowed institution archetypes and three numeric intensity knobs:

```text
digital_maturity
cash_intensity
loan_guarantor_intensity
```

Validation rejects values outside `0.0` to `1.0`.

## `patterns.yaml`

This file records normal pattern switches and current suspicious pattern
switches. The suspicious injection contract is still governed by the CLI
`--with-typologies`, the target-count logic, and `config/typologies.yaml`.

Do not assume changing this file alone is a complete benchmark-control
interface unless the corresponding generator and rule code has been wired for
that switch.

## `typologies.yaml`

Injection parameters for:

```text
STRUCTURING
RAPID_PASS_THROUGH
FAKE_AFFORDABILITY_BEFORE_LOAN
DEVICE_SHARING_MULE_NETWORK
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
```

Executable detection-rule parameters live in
`src/kenya_sacco_sim/core/rules.py`.

## `calendar.yaml`

Defaults reflect Kenyan financial rhythms:

```yaml
calendar:
  payday_days: [24, 25, 26, 27, 28, 29, 30, 31]
  school_fee_months: [1, 5, 8]
  harvest_months: [3, 4, 8, 9, 12]
  december_spend_multiplier: 1.5
  weekend_wallet_multiplier: 1.3
  monday_sme_deposit_multiplier: 1.4
```

## `validation.yaml`

Validation defaults are intentionally strict:

```yaml
validation:
  suspicious_ratio_tolerance: 0.002
  allow_negative_balances_for_customer_accounts: false
  allow_negative_balances_for_source_accounts: true
  max_missing_foreign_key_count: 0
  require_pattern_summary_for_suspicious_patterns: true
  forbid_label_leakage: true
```

Loosening these should be treated as a last resort. If a run only passes after
weakening validation, fix the upstream generator first.

## YAML Loading

The loader prefers `PyYAML`. If it is unavailable, a small built-in parser can
handle the simple mapping/list/scalar subset used by the shipping config files.
Install project dependencies for normal use.

## Manifest Recording

Each run writes `manifest.json` with:

- `config_dir`
- `loaded_config_files`
- effective `seed`, `members`, `institutions`, `suspicious_ratio`, and
  `difficulty`
- file list
- MD5 hashes

To reproduce a run, reuse the same config directory and CLI flags.
