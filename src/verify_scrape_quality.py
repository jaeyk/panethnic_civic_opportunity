#!/usr/bin/env python3
"""Audit scrape quality and coverage for ethnic-named candidates (step 03 output).

Reports how many ethnic-named candidates have no text, thin text, or usable text,
broken out by inferred panethnic group. Candidates with no or thin text are written
to a review file because they are silently excluded from step 05 reclassification
and may represent a systematic bias if they are older, smaller, or less digitally present.

Text quality classification:
  no_text    : empty or error-marker text (scrape_error, is broken, etc.)
  very_short : < 100 chars or < 2 qualifying sentences (likely a stub or scrape failure)
  short      : 100–399 chars or 2–4 qualifying sentences (borderline; may be informative)
  usable     : >= 400 chars or >= 5 qualifying sentences

Outputs:
  processed_data/verification/scrape_quality_summary.csv
  processed_data/verification/scrape_quality_thin_pages.csv
"""
from __future__ import annotations

import argparse
import csv
import os
import re
from collections import defaultdict
from typing import Dict, List

BAD_SCRAPE_RE = re.compile(
    r"^(?:scrape_error:|does not have about page|is broken|is flat|php error)",
    re.IGNORECASE,
)
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|\n+")

USABLE_CHARS = 400
SHORT_CHARS = 100
USABLE_SENTS = 5
SHORT_SENTS = 2


def classify_text(text: str) -> str:
    t = (text or "").strip()
    if not t or BAD_SCRAPE_RE.search(t):
        return "no_text"
    chars = len(t)
    sents = len([s for s in SENTENCE_SPLIT.split(t) if len(s.strip()) >= 15])
    if chars >= USABLE_CHARS or sents >= USABLE_SENTS:
        return "usable"
    if chars >= SHORT_CHARS or sents >= SHORT_SENTS:
        return "short"
    return "very_short"


def norm_ein(x: str) -> str:
    digits = "".join(c for c in str(x or "") if c.isdigit())
    return digits.zfill(9) if digits else ""


def load_group_map(candidates_path: str) -> Dict[str, str]:
    """Return {ein: panethnic_group} from the candidates file."""
    out: Dict[str, str] = {}
    if not candidates_path or not os.path.exists(candidates_path):
        return out
    with open(candidates_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ein = norm_ein(row.get("ein", ""))
            group = (
                row.get("panethnic_group")
                or row.get("inferred_group")
                or "uncertain"
            ).strip().lower()
            if ein:
                out[ein] = group
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--about_input",
        default="processed_data/org_matching/candidate_about_pages.csv",
    )
    p.add_argument(
        "--candidates_input",
        default="processed_data/org_matching/similar_org_candidates.csv",
    )
    p.add_argument("--out_dir", default="processed_data/verification")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    group_map = load_group_map(args.candidates_input)

    QUALITY_LEVELS = ["no_text", "very_short", "short", "usable"]
    counts: Dict[tuple, int] = defaultdict(int)
    thin_rows: List[Dict] = []

    with open(args.about_input, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if (row.get("candidate_type") or "").strip() != "ethnic_named":
                continue
            ein = norm_ein(row.get("ein", ""))
            text = (row.get("about_page_text") or "").strip()
            group = group_map.get(ein, "uncertain")
            quality = classify_text(text)
            counts[(group, quality)] += 1

            if quality != "usable":
                thin_rows.append({
                    "ein": ein,
                    "irs_name_raw": row.get("irs_name_raw", ""),
                    "group": group,
                    "text_quality": quality,
                    "char_count": len(text),
                    "url": (row.get("preferred_link") or row.get("selected_about_url") or "").strip(),
                    "scrape_status": row.get("scrape_status", ""),
                    "error_message": row.get("error_message", ""),
                    "text_preview": text[:200],
                })

    groups = sorted({g for g, _ in counts})

    summary_rows = []
    for grp in groups:
        total = sum(counts[(grp, q)] for q in QUALITY_LEVELS)
        if total == 0:
            continue
        row_out: Dict = {"group": grp, "n_total": total}
        for q in QUALITY_LEVELS:
            n = counts[(grp, q)]
            row_out[f"n_{q}"] = n
            row_out[f"pct_{q}"] = round(100 * n / total, 1) if total > 0 else 0.0
        summary_rows.append(row_out)

    total_all = sum(counts.values())
    if total_all > 0:
        row_out = {"group": "ALL", "n_total": total_all}
        for q in QUALITY_LEVELS:
            n = sum(counts[(g, q)] for g in groups)
            row_out[f"n_{q}"] = n
            row_out[f"pct_{q}"] = round(100 * n / total_all, 1)
        summary_rows.append(row_out)

    summary_path = os.path.join(args.out_dir, "scrape_quality_summary.csv")
    thin_path = os.path.join(args.out_dir, "scrape_quality_thin_pages.csv")

    summary_fields = ["group", "n_total"] + [
        f"{pfx}_{q}" for q in QUALITY_LEVELS for pfx in ("n", "pct")
    ]
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=summary_fields)
        w.writeheader()
        w.writerows(summary_rows)

    thin_fields = [
        "ein", "irs_name_raw", "group", "text_quality", "char_count",
        "url", "scrape_status", "error_message", "text_preview",
    ]
    with open(thin_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=thin_fields)
        w.writeheader()
        w.writerows(thin_rows)

    print(f"Scrape quality summary  → {summary_path}")
    print(f"Thin-page candidates    → {thin_path}  ({len(thin_rows):,} rows)")
    print()
    hdr = f"{'group':12s}  {'total':>6}  {'no_text':>9} {'very_short':>11} {'short':>7} {'usable':>8}"
    print(hdr)
    print("-" * len(hdr))
    for row in summary_rows:
        print(
            f"{row['group']:12s}  {row['n_total']:>6}  "
            f"{row['n_no_text']:>5} ({row['pct_no_text']:>4.1f}%)  "
            f"{row['n_very_short']:>5} ({row['pct_very_short']:>4.1f}%)  "
            f"{row['n_short']:>5} ({row['pct_short']:>4.1f}%)  "
            f"{row['n_usable']:>5} ({row['pct_usable']:>4.1f}%)"
        )


if __name__ == "__main__":
    main()
