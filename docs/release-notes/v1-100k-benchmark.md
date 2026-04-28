# v1 100k Benchmark Release Notes

Status: internal release-scale benchmark package.

Current generated package:

```text
datasets/KENYA_SACCO_SIM_v1_100k
```

Generation command:

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

Raw package summary:

```text
members:        100,000
transactions:   5,305,344
alerts_truth:      11,749
loans:             20,404
guarantors:        29,287
devices:          100,000
total CSV rows: 10,196,191
validation:     0 errors / 0 warnings
ledger replay:  0 mismatches
```

Current stability gate:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 30000 \
  --seeds 42 1337 2026 9001 314159 \
  --jobs 4 \
  --suspicious-ratio 0.015 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

Stability result:

```text
validation error free: true
precision/recall variance within threshold: true
threshold: 0.10
confounder diagnostics: all clear across 5 seeds
digital device coverage mean: 1.0000
cash rail share mean: 0.1859
loan active member ratio mean: 0.2055
arrears share mean: 0.0941
default share mean: 0.0214
near-miss member count mean: 653.0
near-miss transaction count mean: 2734.8
```

Consumer guidance:

- `alerts_truth.csv` is positive injected truth only.
- Count unique suspicious cases by `pattern_id`, not raw alert rows.
- Hold out or stratify static confounders before ML lift claims: `persona_type`, `member_type`, `dormant_flag`, `age`, and `devices.last_seen`.
- `SOURCE_ACCOUNT` and `SINK_ACCOUNT` rows are ledger plumbing and should be filtered from customer-account risk aggregation.
- `--skip-ml-baseline` is intentional for 100k generation. Run `python3 -m kenya_sacco_sim ml-baseline --input ./datasets/KENYA_SACCO_SIM_v1_100k` downstream when model artifacts are needed.
