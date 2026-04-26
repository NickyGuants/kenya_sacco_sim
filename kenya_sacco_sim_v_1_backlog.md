# KENYA_SACCO_SIM v1 Backlog

This backlog captures future v1 work that is not part of the current
`DEVICE_SHARING_MULE_NETWORK` slice.

The current v1 contract lives in:

```text
kenya_sacco_sim_v_1_specification.md
```

## Active Slice

```text
DEVICE_SHARING_MULE_NETWORK
```

This slice uses the existing device layer, support entities, graph projection,
rule baseline, ML baseline, and validation framework. It must keep raw
`device_id` out of model features, label only in `alerts_truth.csv`, export
candidate IDs, include near-misses, and pass the multi-seed stability gate.

## Next Typology Candidates

Priority order:

```text
GUARANTOR_FRAUD_RING
WALLET_FUNNELING
CHURCH_CHARITY_MISUSE
DORMANT_REACTIVATION_ABUSE
REMITTANCE_LAYERING
PAYROLL_PROXY_ABUSE
PROCUREMENT_CORRUPTION_PARKING
TILL_PAYBILL_SHELL_ACTIVITY
```

## Next Recommended Slice

```text
GUARANTOR_FRAUD_RING
```

Rationale:

```text
credit and guarantor graph already exists
the behavior is SACCO-specific
it exercises graph motifs and default contagion
it is harder than another pure transaction-flow typology
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
guarantor-cycle detection
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
