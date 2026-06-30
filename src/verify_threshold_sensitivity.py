#!/usr/bin/env python3
"""Threshold sensitivity analysis for panethnic constituency reclassification (step 05).

Reads the pre-computed sentence evidence file (panethnic_constituency_sentence_evidence.csv)
and applies a grid of (min_score, min_similarity) combinations to show how reclassification
counts change. Does NOT re-embed any text — uses scores already written by step 05 and
runs in seconds.

For each threshold combination an org is counted as reclassified when:
  - its best-scoring sentence for a given group passes both min_score and min_similarity
  - that sentence has cue > 0 and term_hits > 0 (same logic as step 05 pick_reclass)

The "reference" combination matching the current default run is marked in the output.

Outputs:
  processed_data/verification/threshold_sensitivity.csv
"""
from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from itertools import product
from typing import Dict, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--evidence_input",
        default="processed_data/org_matching/panethnic_constituency_sentence_evidence.csv",
        help="Sentence evidence file written by step 05.",
    )
    p.add_argument("--out_dir", default="processed_data/verification")
    p.add_argument(
        "--score_thresholds",
        default="0.35,0.40,0.45,0.50",
        help="Comma-separated min_score values to test.",
    )
    p.add_argument(
        "--similarity_thresholds",
        default="0.35,0.40,0.45",
        help="Comma-separated min_similarity values to test.",
    )
    p.add_argument(
        "--ref_score", type=float, default=0.45,
        help="Reference min_score (current pipeline default).",
    )
    p.add_argument(
        "--ref_similarity", type=float, default=0.40,
        help="Reference min_similarity (current pipeline default).",
    )
    return p.parse_args()


def norm_ein(x: str) -> str:
    digits = "".join(c for c in str(x or "") if c.isdigit())
    return digits.zfill(9) if digits else ""


EvidenceRec = Dict[str, str]


def load_evidence(
    path: str,
) -> Dict[str, Dict[str, List[EvidenceRec]]]:
    """Return {ein: {group: [records sorted desc by score]}}."""
    out: Dict[str, Dict[str, List[EvidenceRec]]] = defaultdict(
        lambda: defaultdict(list)
    )
    if not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ein = norm_ein(row.get("ein", ""))
            group = (row.get("group") or "").strip().lower()
            if ein and group in ("asian", "latino"):
                out[ein][group].append(row)
    return out


def best_passing(records: List[EvidenceRec]) -> Optional[EvidenceRec]:
    """Return the highest-score record that passes the cue/term_hits gate."""
    valid = [
        r for r in records
        if float(r.get("cue", 0)) > 0 and int(r.get("term_hits", 0)) > 0
    ]
    if not valid:
        return None
    return max(valid, key=lambda r: float(r.get("score", 0)))


def count_reclassified(
    evidence: Dict[str, Dict[str, List[EvidenceRec]]],
    min_score: float,
    min_similarity: float,
) -> Tuple[int, int, int, int]:
    total = asian = latino = both = 0
    for ein, group_recs in evidence.items():
        ab = best_passing(group_recs.get("asian", []))
        lb = best_passing(group_recs.get("latino", []))
        a_ok = ab is not None and float(ab["score"]) >= min_score and float(ab["similarity"]) >= min_similarity
        l_ok = lb is not None and float(lb["score"]) >= min_score and float(lb["similarity"]) >= min_similarity
        if a_ok and l_ok:
            total += 1; both += 1
        elif a_ok:
            total += 1; asian += 1
        elif l_ok:
            total += 1; latino += 1
    return total, asian, latino, both


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    score_thresholds = sorted({float(x) for x in args.score_thresholds.split(",")})
    sim_thresholds = sorted({float(x) for x in args.similarity_thresholds.split(",")})

    if not os.path.exists(args.evidence_input):
        raise SystemExit(
            f"Evidence file not found: {args.evidence_input}\n"
            "Run step 05 first to generate panethnic_constituency_sentence_evidence.csv."
        )

    print(f"Loading evidence from {args.evidence_input} ...")
    evidence = load_evidence(args.evidence_input)
    n_orgs = len(evidence)
    print(f"  {n_orgs:,} orgs with at least one evidence sentence\n")

    results: List[Dict] = []
    for min_score, min_sim in product(score_thresholds, sim_thresholds):
        total, asian, latino, both = count_reclassified(evidence, min_score, min_sim)
        is_ref = (min_score == args.ref_score and min_sim == args.ref_similarity)
        results.append({
            "min_score": min_score,
            "min_similarity": min_sim,
            "n_reclassified": total,
            "n_asian": asian,
            "n_latino": latino,
            "n_both": both,
            "pct_of_candidates": round(100 * total / n_orgs, 2) if n_orgs > 0 else 0.0,
            "is_current_default": "yes" if is_ref else "",
        })

    results.sort(key=lambda r: (r["min_score"], r["min_similarity"]))

    out_path = os.path.join(args.out_dir, "threshold_sensitivity.csv")
    fields = [
        "min_score", "min_similarity", "n_reclassified",
        "n_asian", "n_latino", "n_both",
        "pct_of_candidates", "is_current_default",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(results)

    print(f"Sensitivity table → {out_path}\n")
    hdr = f"{'min_score':>10}  {'min_sim':>8}  {'n_reclass':>10}  {'asian':>7}  {'latino':>7}  {'both':>5}  {'%cands':>7}  {'default?':>9}"
    print(hdr)
    print("-" * len(hdr))
    for r in results:
        tag = " ← default" if r["is_current_default"] else ""
        print(
            f"{r['min_score']:>10.2f}  {r['min_similarity']:>8.2f}  "
            f"{r['n_reclassified']:>10}  {r['n_asian']:>7}  {r['n_latino']:>7}  "
            f"{r['n_both']:>5}  {r['pct_of_candidates']:>6.2f}%{tag}"
        )


if __name__ == "__main__":
    main()
