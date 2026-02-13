#!/usr/bin/env python3
"""Merge multi-worker about-page CSV part files into one deduplicated CSV.

Default behavior deduplicates by EIN, keeping the first seen row by sorted
part filename order.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--parts_glob",
        default="processed_data/org_matching/candidate_about_pages_parts/candidate_about_pages.part*.csv",
        help="Glob pattern for part CSV files.",
    )
    p.add_argument(
        "--out_file",
        default="processed_data/org_matching/candidate_about_pages.csv",
        help="Merged output CSV path.",
    )
    p.add_argument(
        "--dedupe_key",
        default="ein",
        help="Column used for deduplication. Empty values are not deduped.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    part_files = sorted(glob.glob(args.parts_glob))

    if not part_files:
        raise SystemExit(f"No part files matched: {args.parts_glob}")

    all_rows: List[Dict[str, str]] = []
    fieldnames: List[str] | None = None

    for path in part_files:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and fieldnames is None:
                fieldnames = list(reader.fieldnames)
            for row in reader:
                all_rows.append(row)

    if fieldnames is None:
        raise SystemExit("Part files found but no CSV headers were readable")

    key = args.dedupe_key
    seen = set()
    dedup: List[Dict[str, str]] = []

    for row in all_rows:
        k = (row.get(key) or "").strip()
        if k:
            if k in seen:
                continue
            seen.add(k)
        dedup.append(row)

    out_dir = os.path.dirname(args.out_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(args.out_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(dedup)

    print(f"Merged {len(part_files)} files -> {args.out_file}")
    print(f"Rows (raw): {len(all_rows)}")
    print(f"Rows (dedup by {key}): {len(dedup)}")


if __name__ == "__main__":
    main()
