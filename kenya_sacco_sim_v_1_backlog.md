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

The latest completed slice adds `WALLET_FUNNELING`, a multi-counterparty
wallet/paybill fan-in typology with normal chama/project collection near-misses.

Completed v1 slices:

```text
DEVICE_SHARING_MULE_NETWORK
NEGATIVE_CONTROLS_AND_NEAR_MISSES
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
```

## Next Typology Candidates

Priority order:

```text
CHURCH_CHARITY_MISUSE
DORMANT_REACTIVATION_ABUSE
REMITTANCE_LAYERING
PAYROLL_PROXY_ABUSE
PROCUREMENT_CORRUPTION_PARKING
TILL_PAYBILL_SHELL_ACTIVITY
```

## Next Recommended Slice

```text
CHURCH_CHARITY_MISUSE
```

Rationale:

```text
church/org normal behavior, paybill collections, supplier outflows, and
bulk-family/church near-miss patterns already exist
it extends the blueprint's charity/church misuse risk without adding new tables
it should reuse the current near-miss, confounder, and split diagnostics
```

## Benchmark Backlog

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
device-sharing false-positive baselines
CTR threshold coverage
institution concentration targets
train/validation/test leakage checks for new label tables
```

## v1 Principle

v1 should expand behavioral ambiguity, not just add suspicious rows. Every new
typology must ship with normal near-misses, rule reconstruction, candidate IDs,
leakage checks, validation metrics, and documentation updates.
