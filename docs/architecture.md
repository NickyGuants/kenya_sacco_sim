# Architecture

The codebase is intentionally small. The CLI builds a deterministic in-memory
world, validates it, and writes CSV/JSON/Markdown artifacts.

## Source Layout

```text
src/kenya_sacco_sim/
  __main__.py
  cli.py
  core/
    config.py
    enums.py
    id_factory.py
    models.py
    rules.py
  generators/
    institutions.py
    members.py
    devices.py
    accounts.py
    loans.py
    repayment_schedule.py
    guarantors.py
    transactions.py
    typologies.py
    nodes.py
    edges.py
  benchmark/
    artifacts.py
    baseline_rules.py
    ml_baseline.py
    multi_seed.py
  validation/
    schema.py
    foreign_keys.py
    balances.py
    distribution.py
    loans.py
    loan_validator.py
    labels.py
    support_entities.py
    clean_baseline.py
    report.py
  export/
    csv.py
```

## Generation Pipeline

The order in `cli.generate` is fixed:

```text
load_world_config
    |
    v
generate_institution_world      institutions, branches, agents, employers
    |
    v
generate_members                personas, KYC, risk, declared income
    |
    v
generate_devices                member devices and baseline shared groups
    |
    v
generate_accounts               member accounts plus source/sink accounts
    |
    v
generate_loans_and_guarantors   only with --with-loans
    |
    v
generate_transactions           normal ledger and loan lifecycle
    |
    v
inject_typologies               only with --with-typologies
    |
    v
generate_nodes / generate_edges graph projection
    |
    v
build_pattern_labels / build_edge_labels
    |
    v
build_benchmark_artifacts       only with --with-benchmark
    |
    v
build_validation_report         always
    |
    v
write artifacts                 CSV, JSON, Markdown, manifest
```

Most steps return new row collections. Typology injection is the deliberate
exception: it appends suspicious transactions and labels, may update shared
device grouping for device-based typologies, may add guarantor rows for
graph-credit typologies, and then the graph/validation phases consume the
finalized world.

## Determinism Model

There is one master `seed` in `WorldConfig`. Each generator derives a local
`random.Random` from `config.seed + offset`.

This keeps generator streams stable:

- Adding a new generator does not perturb existing generator seeds.
- Re-running with the same seed/config/CLI flags produces identical artifacts.
- Different member counts produce different outputs because generators draw
  different numbers of random values.

The same model applies to typology injection.

## Validation Phase

Validation does not write files and does not repair data. It reads the
finalized in-memory rows, raises `ValidationFinding` instances, and assembles
`validation_report.json`.

The CLI exit code is derived from the top-level `errors` array.

## Benchmark Layer

The benchmark layer is downstream of generation. It produces:

- stratified member and pattern splits
- deterministic rule metrics
- member-level ML baselines
- near-miss and negative-control coverage
- feature importances
- rule-proxy leakage ablation
- rule-vs-ML comparison
- temporal/persona confounder diagnostics
- dataset card and limitations

For large generated packages, the ML portion can be decoupled from generation.
`generate --with-benchmark --skip-ml-baseline` still emits split, rule,
confounder, feature-documentation, dataset-card, and skipped ML placeholder
artifacts. `ml-baseline --input <dataset_dir>` can then run the sklearn models
later from the exported CSVs. This keeps large-scale generation from being
blocked by model fitting.

The `benchmark` command wraps the full generation pipeline in a multi-seed loop,
executes independent seeds in parallel worker processes, and writes
`multi_seed_results.json`, including near-miss stability, full-feature ML
stability, ablated ML stability, ablation F1-drop stability, and per-seed
confounder flags. Use `--jobs 1` to force serial execution when debugging a
single seed path.

## Adding A New Typology

The required sequence:

1. Check `kenya_sacco_sim_v_1_specification.md`.
2. Add executable rule parameters in `core/rules.py`.
3. Add injector behavior in `generators/typologies.py`.
4. Add or extend deterministic detection in `benchmark/baseline_rules.py`.
5. Add validation metrics in `validation/report.py` or a focused validator.
6. Add ML features only if they are non-leaky.
7. Add tests for target counts, rule reconstruction, and validation failures.
8. Update README, docs, dataset card/limitations generation, and the spec.

Every typology needs normal near-misses and candidate IDs.

## Adding A New Export

The required sequence:

1. Define columns and enums.
2. Generate rows with deterministic IDs.
3. Add schema and foreign-key validation.
4. Add graph projection when relevant.
5. Add artifact documentation.
6. Add tests.
