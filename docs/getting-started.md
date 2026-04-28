# Getting Started

This page takes you from a fresh clone to a verified synthetic AML benchmark
package on disk.

## Requirements

- Python 3.11 or newer.
- Roughly 2 GB of free disk space for a 100,000-member no-ML benchmark run.
- The Python packages declared in `pyproject.toml`, including `PyYAML` and
  `scikit-learn`.

The generator does not need a network connection at runtime and does not talk
to a database. Everything it produces is written to local files.

## Install

From the repository root:

```bash
python3 -m pip install -e .
```

Editable install is the recommended path while the project is under active
development.

## First Smoke Run

Generate a small dataset to confirm the pipeline is wired up:

```bash
python3 -m kenya_sacco_sim generate --members 1000
```

This writes support entities, members, accounts, devices, nodes, graph edges,
`validation_report.json`, and `manifest.json` to the default output directory.
It does not produce transactions or loans.

Add transactions and the credit system:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-loans
```

`--with-loans` implies `--with-transactions` because loan disbursements and
repayments are ledger events.

## Full Benchmark Run

This is the current release-scale benchmark run:

```bash
python3 -m kenya_sacco_sim generate \
  --members 100000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --suspicious-ratio 0.015 \
  --output ./datasets/KENYA_SACCO_SIM_v1_100k
```

When it finishes, inspect these first:

- `validation_report.json` - must report zero errors.
- `dataset_card.md` - human-readable summary of counts, metrics, and limits.
- `manifest.json` - seed, config, files, and MD5 hashes.
- `rule_results.json` - executable rule results, candidate IDs, and near-miss
  disclosure.
- `ml_leakage_ablation.json` - ML rule-proxy ablation diagnostics.
- `benchmark_confounder_diagnostics.json` - temporal/persona shortcut diagnostics.

If `errors` is nonzero, the run still writes files, but the dataset should not
be used as a benchmark. Read the top-level `errors` array in
`validation_report.json`.

## Multi-Seed Stability Check

For a stronger guarantee that the benchmark is not seed-fragile:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 30000 \
  --seeds 42 1337 2026 9001 314159 \
  --jobs 4 \
  --suspicious-ratio 0.015 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

This writes `multi_seed_results.json`. The command exits nonzero if any seed
has validation errors or if typology precision/recall ranges drift by more than
`0.10` across seeds. Seeds must be unique. Seed runs execute in parallel by
default; use `--jobs 1` when debugging a single serial path.

## Larger Generated Packages

For larger generated packages, keep ML training out of the generation loop:

```bash
python3 -m kenya_sacco_sim generate \
  --members 50000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --output ./datasets/KENYA_SACCO_SIM_v1_50k
```

Then run ML artifacts later:

```bash
python3 -m kenya_sacco_sim ml-baseline \
  --input ./datasets/KENYA_SACCO_SIM_v1_50k
```

The current generated package is a richer 100,000-member no-ML package with
12 personas, nine active typologies, corrected dormant semantics, 5,305,344
transactions, and 10,196,191 total CSV rows.

## Sanity Tests

Unit tests:

```bash
python3 -m unittest discover -s tests
```

Compile check:

```bash
python3 -m compileall src tests
```

Both are part of the development checklist in the top-level README.

## What To Do Next

- Read [Concepts](concepts.md) to understand the domain.
- Read [Outputs](outputs.md) to understand the files.
- Read [Typologies](typologies.md) before interpreting labels or rule metrics.
