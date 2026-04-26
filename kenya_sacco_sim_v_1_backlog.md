# KENYA_SACCO_SIM v1 Backlog

This backlog preserves research-blueprint ideas that are intentionally outside the v0.2 foundation release.

v1 should only start after v0.2 produces clean 10,000-member benchmark runs with support files, device coverage, institution archetypes, `FAKE_AFFORDABILITY_BEFORE_LOAN`, and multi-seed stability evidence.

## v0.2 Completion Gate Before v1

```text
multi_seed_results.json generated
all configured seeds have validation_error_count = 0
typology precision/recall range <= 0.10 across accepted seeds
cash/device/credit stability statistics reported
dataset card documents expected ambiguity and benchmark limits
```

Do not add v1 typologies until this gate is clean.

## v1 Typology Backlog

```text
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
CHURCH_CHARITY_MISUSE
DORMANT_REACTIVATION_ABUSE
REMITTANCE_LAYERING
DEVICE_SHARING_MULE_NETWORK
PAYROLL_PROXY_ABUSE
PROCUREMENT_CORRUPTION_PARKING
TILL_PAYBILL_SHELL_ACTIVITY
```

## v1 Benchmark Backlog

```text
Graph neural network benchmark
100,000+ member scale
Multi-difficulty benchmark suite
pattern_labels.csv
edge_labels.csv
trained baseline model reports
notebook-based benchmark walkthrough
institution-specific calibration packs
```

## v1 Data Model Backlog

```text
dedicated remittance corridors
government/tax/service payment tables
merchant/till/paybill registry
full device session table
account lifecycle events beyond transaction rows
formal CTR/SAR report artifacts
crop/county-specific farmer calendars
institution concentration calibration
```

## v1 Validation Backlog

```text
guarantor-cycle detection
dormancy reactivation anomaly baselines
remittance corridor fan-out baselines
device-sharing false-positive baselines
CTR threshold coverage
institution concentration targets
train/validation/test leakage checks for new label tables
```

## v1 Principle

v1 should expand behavioural ambiguity, not just add suspicious rows. Every new typology must ship with normal near-misses, rule reconstruction, candidate IDs, leakage checks, and validation metrics.
