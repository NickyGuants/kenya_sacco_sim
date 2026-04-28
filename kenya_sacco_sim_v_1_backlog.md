# KENYA_SACCO_SIM v1 Backlog

This backlog captures future v1 work that is not part of the current
implemented typology set.

The current v1 contract lives in:

```text
kenya_sacco_sim_v_1_specification.md
```

## Active Slice

```text
none
```

The latest completed slice refreshes the v1 100k benchmark with 12 personas,
correct dormant semantics, three additional typologies, static-confounder
documentation, organization-age null handling, observed device `last_seen`, and
a refreshed 30k five-seed stability gate for the current nine-typology mix.

Completed v1 slices:

```text
DEVICE_SHARING_MULE_NETWORK
NEGATIVE_CONTROLS_AND_NEAR_MISSES
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
CHURCH_CHARITY_MISUSE
DORMANT_REACTIVATION_ABUSE
REMITTANCE_LAYERING
100K_RELEASE_SCALE_REFRESH
STATIC_CONFOUNDER_DOCUMENTATION
BENCHMARK_STABILITY_REFRESH
```

## Next Typology Candidates

Priority order:

```text
PAYROLL_PROXY_ABUSE
PROCUREMENT_CORRUPTION_PARKING
TILL_PAYBILL_SHELL_ACTIVITY
```

## Next Recommended Slice

```text
RELEASE_NOTEBOOK_AND_LABEL_TABLES
```

Rationale:

```text
the current nine-typology 100k package is generated, raw-audited, and covered
by a 30k five-seed stability gate
the next release risk is consumer confusion around alert granularity and
benchmark usage
pattern_labels.csv, edge_labels.csv, release notes, and a notebook walkthrough
would make the package easier to consume without adding new suspicious behavior
```

## Benchmark Backlog

```text
pattern_labels.csv
edge_labels.csv
notebook-based benchmark walkthrough
release notes for the v1 100k package
Multi-difficulty benchmark suite
Graph neural network benchmark
100,000+ member scale
institution-specific calibration packs
```

## Data Model Backlog

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

## Validation Backlog

```text
guarantor concentration false-positive baselines
dormancy reactivation anomaly baselines
remittance corridor fan-out baselines
church/charity misuse false-positive baselines
device-sharing false-positive baselines
CTR threshold coverage
institution concentration targets
train/validation/test leakage checks for new label tables
```

## v1 Principle

v1 should expand behavioral ambiguity, not just add suspicious rows. Every new
typology must ship with normal near-misses, rule reconstruction, candidate IDs,
leakage checks, validation metrics, and documentation updates.
