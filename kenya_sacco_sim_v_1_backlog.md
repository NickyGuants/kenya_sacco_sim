# KENYA_SACCO_SIM v1 Backlog

This backlog captures future v1 work that is not part of the current
`GUARANTOR_FRAUD_RING` slice.

The current v1 contract lives in:

```text
kenya_sacco_sim_v_1_specification.md
```

## Active Slice

```text
GUARANTOR_FRAUD_RING
```

This slice adds a graph-credit typology using reciprocal or circular guarantor
relationships. It must update guarantors, graph projections, executable rules,
near-misses, validation, benchmark artifacts, and docs together.

Completed v1 slices:

```text
DEVICE_SHARING_MULE_NETWORK
NEGATIVE_CONTROLS_AND_NEAR_MISSES
```

## Next Typology Candidates

Priority order:

```text
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
WALLET_FUNNELING
```

Rationale:

```text
MPESA, paybill, and counterparty structure already exist
it extends rapid-pass-through into multi-member funnel behavior
it can use current near-miss and confounder diagnostics
it should be deferred until the guarantor-ring slice passes the 10k and multi-seed gates
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
