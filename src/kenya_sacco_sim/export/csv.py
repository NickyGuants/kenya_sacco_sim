from __future__ import annotations

import csv
import json
from pathlib import Path


def write_csvs(output_dir: Path, rows_by_file: dict[str, list[dict[str, object]]]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, rows in rows_by_file.items():
        path = output_dir / filename
        if not rows:
            path.write_text("", encoding="utf-8")
            continue
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
