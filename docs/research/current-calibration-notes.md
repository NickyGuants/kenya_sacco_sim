# Current Calibration Notes

These notes document the external grounding used for the current v1 100k
refresh. They do not replace `deep-research-report.md`; they record the
implementation-facing calibration choices made after reviewing the current
official sources.

## Sources Reviewed

- SASRA SACCO supervision reports page: lists the 2024 SACCO supervision report
  and prior annual reports.
- SASRA 2023 SACCO Supervision Annual Report page: reports 357 regulated
  SACCOs, 6.84 million members, KES 682.19 billion deposits, and
  KES 758.57 billion gross loans for 2023.
- CBK Bank Supervision Annual Report 2024: reports remittance inflows rising to
  KES 440 billion in 2024 and notes digital channels as the most popular
  transfer mode for remittances.
- CBK Financial Stability Report 2024: reports DTS NPLs at 6.15% and notes
  delayed employer remittances as a SACCO credit-risk concern.
- FinAccess 2024: describes the shift in SACCO service delivery toward agency,
  internet, and mobile technologies while branch usage remains strong,
  especially in rural areas.

## Calibration Choices

- Expand personas from 7 to 12 to better represent public-sector payroll,
  private-sector payroll, uniformed services, micro-trade, chama/group, church
  organization, and SACCO staff behavior.
- Keep mobile and paybill-heavy channels prominent, but preserve branch and
  cash behavior for rural and lower-digital-maturity segments.
- Treat remittances as a material source of legitimate inflow and as an AML
  layering surface.
- Keep credit behavior central: SACCO loan/guarantor behavior remains a core
  differentiator from a generic payments simulator.
- Correct dormant semantics: `dormant_flag=True` means inactive at simulation
  start, not merely low-activity. Reactivated dormant accounts require
  `KYC_REFRESH` and `ACCOUNT_REACTIVATION` before renewed activity.
- Add church/charity misuse and chama-related near-misses because organization
  accounts now have normal active behavior and should not be ceremonial.

## Current 100k Release Gate

The current generated package is:

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

The package preserves zero validation errors and zero warnings at 100,000
members, with 5,305,344 transactions and 10,196,191 total CSV rows in the
current generated artifact.

The current five-seed stability gate is:

```bash
python3 -m kenya_sacco_sim benchmark \
  --members 30000 \
  --seeds 42 1337 2026 9001 314159 \
  --jobs 4 \
  --suspicious-ratio 0.015 \
  --output ./benchmarks/KENYA_SACCO_SIM_v1_multi_seed
```

It passes with zero validation errors across all seeds, typology
precision/recall ranges within the `0.10` stability threshold, and no temporal
or persona/static-confounder review flags.

Recent audit fixes:

- Organization age is blank/missing rather than `0`.
- `devices.last_seen` is derived from observed device transaction usage.
- Static fields such as `persona_type`, `member_type`, `dormant_flag`, `age`,
  and `devices.last_seen` are documented as holdout/stratification fields for
  ML lift claims.
