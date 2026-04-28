# KENYA_SACCO_SIM Documentation

KENYA_SACCO_SIM is a command-line generator for synthetic Kenyan SACCO AML
benchmark data. It writes members, accounts, institutions, branches, agents,
employers, devices, loans, guarantors, transactions, graph projections,
ground-truth labels, baseline results, ML diagnostics, and validation reports.

The generator is deterministic. Given the same seed, config, and CLI flags, it
produces the same CSVs and JSON artifacts. Every run writes a
`validation_report.json`; a benchmark dataset is usable only when that report
has zero errors.

## Who This Is For

- Data scientists who need a labelled AML benchmark without real customer data.
- Engineers wiring the generator into model training or benchmark pipelines.
- Compliance and risk teams reviewing what the synthetic world contains.
- Researchers testing AML rules, graph features, and member-level typology
  detection.

## Read In This Order

1. [Getting Started](getting-started.md) - install, generate, and validate a run.
2. [Concepts](concepts.md) - SACCO, AML, labels, leakage, and personas.
3. [CLI Reference](cli-reference.md) - subcommands and flags.
4. [Outputs](outputs.md) - emitted files and relationships.
5. [Configuration](configuration.md) - YAML defaults and CLI overrides.
6. [Typologies](typologies.md) - suspicious patterns and rule baselines.
7. [Validation](validation.md) - validation report and benchmark gates.
8. [Architecture](architecture.md) - code layout and generation order.
9. [v1 100k Release Notes](release-notes/v1-100k-benchmark.md) - current package commands, gates, and caveats.

## What Is In This Folder

```text
docs/
  index.md
  getting-started.md
  concepts.md
  cli-reference.md
  outputs.md
  configuration.md
  typologies.md
  validation.md
  architecture.md
  release-notes/
    v1-100k-benchmark.md
  research/
    deep-research-report.md
    current-calibration-notes.md
```

The current implementation contract is the root-level
`kenya_sacco_sim_v_1_specification.md`.

## Quick Orientation

- Package: `kenya_sacco_sim`
- CLI entrypoint: `python3 -m kenya_sacco_sim ...`
- Commands: `generate`, `benchmark`
- Default config directory: `./config`
- Current benchmark output path: `./datasets/KENYA_SACCO_SIM_v1_100k`
- Current multi-seed output path: `./benchmarks/KENYA_SACCO_SIM_v1_multi_seed`
- Release-scale benchmark target: 100,000 members over 12 months, generated
  with `--skip-ml-baseline`.
