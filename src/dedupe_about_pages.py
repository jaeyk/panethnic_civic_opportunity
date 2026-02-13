#!/usr/bin/env python3
"""Create a backup and deduplicate about-page rows by EIN.

Default behavior:
- backup: processed_data/org_matching/candidate_about_pages_original_backup.csv
- unique output: processed_data/org_matching/candidate_about_pages_unique.csv
- overwrite input with unique rows (can be disabled)
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
from typing import Dict, List


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input_file",
        default="processed_data/org_matching/candidate_about_pages.csv",
    )
    p.add_argument(
        "--backup_file",
        default="processed_data/org_matching/candidate_about_pages_original_backup.csv",
    )
    p.add_argument(
        "--output_file",
        default="processed_data/org_matching/candidate_about_pages_unique.csv",
    )
    p.add_argument("--dedupe_key", default="ein")
    p.add_argument(
        "--overwrite_input",
        default="true",
        help="true/false; when true, writes deduped rows back to input_file",
    )
    return p.parse_args()


def as_bool(x: str) -> bool:
    return str(x).strip().lower() in {"1", "true", "t", "yes", "y"}


def main() -> None:
    args = parse_args()
    overwrite_input = as_bool(args.overwrite_input)

    if not os.path.exists(args.input_file):
        raise SystemExit(f"Input file not found: {args.input_file}")

    os.makedirs(os.path.dirname(args.backup_file) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.output_file) or ".", exist_ok=True)

    shutil.copyfile(args.input_file, args.backup_file)

    rows: List[Dict[str, str]] = []
    with open(args.input_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise SystemExit("CSV has no header")
        for row in reader:
            rows.append(row)

    seen = set()
    unique_rows: List[Dict[str, str]] = []
    key = args.dedupe_key

    for row in rows:
        kval = (row.get(key) or "").strip()
        if kval:
            if kval in seen:
                continue
            seen.add(kval)
        unique_rows.append(row)

    with open(args.output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(unique_rows)

    if overwrite_input:
        with open(args.input_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            writer.writerows(unique_rows)

    removed = len(rows) - len(unique_rows)
    print(f"Backup: {args.backup_file}")
    print(f"Rows (raw): {len(rows)}")
    print(f"Rows (unique by {key}): {len(unique_rows)}")
    print(f"Duplicates removed: {removed}")
    print(f"Unique file: {args.output_file}")
    print(f"Input overwritten: {'yes' if overwrite_input else 'no'}")


if __name__ == "__main__":
    main()
