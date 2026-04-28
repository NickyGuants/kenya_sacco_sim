# CLI Reference

Invoke the package as a Python module:

```bash
python3 -m kenya_sacco_sim <command> [options]
```

Commands:

```text
generate
benchmark
```

## `generate`

Produces one synthetic dataset and writes it to disk.

```bash
python3 -m kenya_sacco_sim generate [options]
```

### Options

| Flag | Type | Default | What it does |
| --- | --- | --- | --- |
| `--members` | int | from `world.yaml` (10000) | Number of members in the world. |
| `--institutions` | int | from `world.yaml` (5) | Number of SACCO institutions. |
| `--months` | int | from `world.yaml` (12) | Length of the simulated period. |
| `--seed` | int | from `world.yaml` (42) | Master random seed. Same seed plus same config means same output. |
| `--suspicious-ratio` | float | from `world.yaml` (0.01) | Fraction of members assigned suspicious typology labels. |
| `--difficulty` | string | from `world.yaml` (`medium`) | Difficulty knob recorded in `manifest.json`. |
| `--config-dir` | path | `./config` | Folder to load YAML config from. Missing files fall back to defaults. |
| `--output` | path | `./datasets/KENYA_SACCO_SIM_v1` | Where to write CSVs and JSON artifacts. |
| `--with-transactions` | flag | off | Emit `transactions.csv` and run balance validation. |
| `--with-loans` | flag | off | Emit `loans.csv`, `guarantors.csv`, and loan-lifecycle transactions. Implies `--with-transactions`. |
| `--with-typologies` | flag | off | Inject suspicious typologies and unlabeled near-miss families; emit `alerts_truth.csv` plus `rule_results.json`. Combine with `--with-loans` to enable credit-linked typologies such as fake-affordability and guarantor rings; wallet funneling uses the normal wallet/paybill transaction layer. |
| `--with-benchmark` | flag | off | Emit the benchmark package: splits, rule baseline, ML baseline, feature docs, dataset card, known limitations, descriptive comparison, leakage ablation, and confounder diagnostics. Requires `--with-typologies`. |
| `--skip-ml-baseline` | flag | off | With `--with-benchmark`, skip sklearn ML training and emit explicit skipped ML artifacts. Use this for large generated packages, then run `ml-baseline` later if needed. |

CLI flags always win over `world.yaml`. Anything not passed on the command line
takes its value from the loaded config, which in turn falls back to a built-in
default if the YAML file is missing.

### Exit Code

`generate` exits `0` if the validation report has zero errors and `1`
otherwise. Output files are written either way so failures can be inspected.

### Output Summary

On completion, the command prints a JSON summary:

```json
{"output": "./datasets/KENYA_SACCO_SIM_v1", "errors": 0, "warnings": 0}
```

The complete file set is described in [Outputs](outputs.md).

### Examples

A 1,000-member smoke run with no transactions:

```bash
python3 -m kenya_sacco_sim generate --members 1000
```

A 1,000-member run with normal transactions and loans:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-loans
```

A 100,000-member release-scale benchmark package:

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

A larger package with benchmark diagnostics but no in-generation ML training:

```bash
python3 -m kenya_sacco_sim generate \
  --members 50000 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --output ./datasets/KENYA_SACCO_SIM_v1_50k
```

A run pinned to a different seed:

```bash
python3 -m kenya_sacco_sim generate \
  --members 100000 \
  --seed 1337 \
  --with-loans \
  --with-typologies \
  --with-benchmark \
  --skip-ml-baseline \
  --suspicious-ratio 0.015 \
  --output ./datasets/KENYA_SACCO_SIM_v1_100k_seed1337
```

## `benchmark`

Runs the full generator across multiple seeds and reports stability.

```bash
python3 -m kenya_sacco_sim benchmark --seeds 42 1337 2026 [options]
```

### Options

| Flag | Type | Default | What it does |
| --- | --- | --- | --- |
| `--seeds` | int+ | required | One or more unique seeds. |
| `--members` | int | from `world.yaml` | Member count for each seed run. |
| `--institutions` | int | from `world.yaml` | Institution count for each seed run. |
| `--months` | int | from `world.yaml` | Months simulated for each seed run. |
| `--suspicious-ratio` | float | from `world.yaml` | Suspicious member ratio. |
| `--difficulty` | string | from `world.yaml` | Difficulty knob. |
| `--config-dir` | path | `./config` | Same semantics as `generate`. |
| `--output` | path | `./benchmarks/v1_multi_seed` | Where the multi-seed report is written. |
| `--write-seed-datasets` | flag | off | Also write each seed's full generated package under the output directory. |
| `--jobs` | int | auto | Parallel seed workers. Auto is capped by seed count, CPU count, and an estimated memory budget. Explicit values are still memory-capped. Use `1` for serial execution. |
| `--skip-ml-baseline` | flag | off | Skip per-seed sklearn ML artifacts while still checking generation, validation, rule, split, and stability metrics. |
| `--quiet` | flag | off | Suppress benchmark progress logs on stderr. |

There is no `--seed` flag; the loop owns the seed choice. There are also no
`--with-loans`, `--with-typologies`, or `--with-benchmark` flags because the
benchmark loop always runs the full pipeline.

Each seed is independent, so the benchmark runner executes seeds in parallel
worker processes by default. The final JSON summary is still printed on stdout;
progress logs go to stderr.

For the current multi-seed benchmark on the local 11-CPU, ~18 GB development
machine, auto/`--jobs 4` runs four seed workers. The release-hygiene gate uses
30,000 members, five seeds, `--suspicious-ratio 0.015`, and full ML/ablation
artifacts; it completes in about 8m32s locally. For larger generated packages,
use `--skip-ml-baseline` to keep ML training out of the generation loop.

### Output Summary

```json
{
  "output": "./benchmarks/KENYA_SACCO_SIM_v1_multi_seed",
  "seeds": [42, 1337, 2026, 9001, 314159],
  "validation_error_free": true,
  "precision_recall_variance_within_threshold": true
}
```

The detailed per-seed metrics live in `multi_seed_results.json`.

### Exit Code

`benchmark` exits `0` only if every seed produced a clean validation report and
every typology stayed within the precision/recall stability threshold of
`0.10` across seeds. Otherwise it exits `1`.

### Failure Modes

- Duplicate seeds are rejected before any work is done.
- An empty `--seeds` list is rejected.
- A seed with validation errors fails the gate even if precision/recall look stable.

## `ml-baseline`

Runs the ML benchmark layer from an existing generated dataset directory.

```bash
python3 -m kenya_sacco_sim ml-baseline --input ./datasets/KENYA_SACCO_SIM_v1_100k
```

This command reads the exported CSVs plus `rule_results.json`, rebuilds split,
rule, ML, ablation, comparison, feature-documentation, dataset-card, and known
limitations artifacts, and writes them to `--output` or back into `--input` when
no output directory is provided. It is intended for large packages generated
with `--skip-ml-baseline`.
