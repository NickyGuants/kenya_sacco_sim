# KENYA_SACCO_SIM

Synthetic AML dataset generator for Kenyan SACCO behavior.

## Current status

The repository implements the frozen v0.1 specification through the first clean-world transaction slice:

- Static world generation for institutions, members, accounts, nodes, and graph edges
- Optional normal-pattern transaction generation with `--with-transactions`
- Schema, foreign-key, balance, and distribution validation
- CSV/JSON export with manifest and validation report

## Usage

From the repository root:

```bash
python3 -m kenya_sacco_sim generate --members 1000 --with-transactions
```

If your environment has `python` mapped to Python 3, the equivalent command is:

```bash
python -m kenya_sacco_sim generate --members 1000 --with-transactions
```

Outputs are written to `datasets/KENYA_SACCO_SIM_v0_1` by default.

## Validation

```bash
python3 -m compileall kenya_sacco_sim src
python3 -m kenya_sacco_sim generate --members 1000 --with-transactions
```

The current calibrated 1,000-member run produces zero validation errors and zero warnings.
