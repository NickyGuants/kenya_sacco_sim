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
documentation, organization-age null handling, and observed device `last_seen`.

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
BENCHMARK_STABILITY_AND_RELEASE_HYGIENE
```

Rationale:

```text
the current nine-typology 100k package is generated and raw-audited
the next risk is benchmark drift, stale summaries, and untested multi-seed
behavior after the latest persona/dormancy/typology refresh
new typologies should wait until current nine-typology stability is refreshed
and release notes/notebook guidance are coherent
```

## Benchmark Backlog

```text
Refresh five-seed stability for the current nine-typology mix
Multi-seed ML and ablation diagnostics from exported packages
Temporal/persona/static-confounder stress test report
Multi-difficulty benchmark suite
pattern_labels.csv
edge_labels.csv
notebook-based benchmark walkthrough
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
