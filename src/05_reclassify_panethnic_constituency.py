#!/usr/bin/env python3
"""Sentence-level panethnic constituency reclassification for ethnic-named orgs.

This script scans about-page text and upgrades candidate group labels only when
there is sentence-level evidence that panethnic groups are described as the
organization's constituencies.

Design goals:
- Use sentence-transformers (all-MiniLM-L6-v2) for semantic similarity.
- Scan all sentences in the about page (no sentence cap).
- Emit auditable evidence sentences and confidence scores.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
from sentence_transformers import SentenceTransformer

ASIAN_TERMS = {
    "asian",
    "asian american",
    "asian americans",
    "aapi",
    "api",
    "apida",
    "pacific islander",
    "pacific islanders",
}

LATINO_TERMS = {
    "latino",
    "latinos",
    "latina",
    "latinas",
    "latinx",
    "latine",
    "hispanic",
    "hispanics",
    "hispana",
    "hispanas",
}

CUE_PATTERNS = [
    r"\b(serve|serves|serving)\b",
    r"\b(support|supports|supporting)\b",
    r"\b(advocate|advocates|advocating)\b",
    r"\b(work|works|working)\s+(with|for|among)\b",
    r"\b(programs?|services?)\s+(for|to)\b",
    r"\b(community|communities|famil(?:y|ies)|residents|youth|people)\b",
    r"\b(our|the)\s+(community|communities|constituents?|members?)\b",
    r"\bconstituenc(?:y|ies)\b",
]

PROTOTYPES = {
    "asian": [
        "we serve asian american communities and families",
        "our programs support aapi residents and youth",
        "we advocate for asian and pacific islander communities",
    ],
    "latino": [
        "we serve latino and hispanic communities and families",
        "our organization supports latinx residents and youth",
        "we advocate for latina and latino communities",
    ],
}

SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
BAD_SCRAPE_RE = re.compile(
    r"^(?:scrape_error:|does not have about page|is broken|is flat|php error)",
    re.IGNORECASE,
)

# Module-level model and prototype embeddings (loaded once)
_MODEL: Optional[SentenceTransformer] = None
_PROTO_EMBS: Dict[str, np.ndarray] = {}


def load_model(model_name: str = "all-MiniLM-L6-v2") -> None:
    global _MODEL, _PROTO_EMBS
    _MODEL = SentenceTransformer(model_name)
    for group, sents in PROTOTYPES.items():
        embs = _MODEL.encode(sents, normalize_embeddings=True, show_progress_bar=False)
        agg = embs.mean(axis=0)
        _PROTO_EMBS[group] = agg / np.linalg.norm(agg)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--about_input",
        default="processed_data/org_matching/candidate_about_pages.csv",
    )
    p.add_argument(
        "--candidates_input",
        default="processed_data/org_matching/similar_org_candidates.csv",
    )
    p.add_argument(
        "--out_file",
        default="processed_data/org_matching/panethnic_constituency_reclass.csv",
    )
    p.add_argument(
        "--evidence_file",
        default="processed_data/org_matching/panethnic_constituency_sentence_evidence.csv",
    )
    p.add_argument("--min_score", type=float, default=0.45)
    p.add_argument("--min_similarity", type=float, default=0.40)
    p.add_argument("--model_name", type=str, default="all-MiniLM-L6-v2")
    p.add_argument("--batch_size", type=int, default=128)
    return p.parse_args()


def norm_ein(x: str) -> str:
    digits = "".join(ch for ch in str(x or "") if ch.isdigit())
    return digits.zfill(9) if digits else ""


def clean_text(x: str) -> str:
    txt = (x or "").strip()
    return re.sub(r"\s+", " ", txt)


def split_sentences(text: str) -> List[str]:
    if not text:
        return []
    return [s for s in (clean_text(s) for s in SPLIT_RE.split(text)) if len(s) >= 20]


def has_phrase(sentence_norm: str, phrase: str) -> bool:
    return bool(re.search(r"\b" + re.escape(phrase) + r"\b", sentence_norm))


def group_term_hits(sentence_norm: str, group: str) -> int:
    terms = ASIAN_TERMS if group == "asian" else LATINO_TERMS
    return sum(1 for t in terms if has_phrase(sentence_norm, t))


def cue_score(sentence_norm: str) -> float:
    hits = sum(1 for pat in CUE_PATTERNS if re.search(pat, sentence_norm))
    return min(1.0, hits / 3.0)


def should_skip_text(text: str) -> bool:
    t = (text or "").strip()
    return not t or bool(BAD_SCRAPE_RE.search(t))


def pick_reclass(
    best_asian: Optional[Dict],
    best_latino: Optional[Dict],
    min_score: float,
    min_similarity: float,
) -> Tuple[str, float, str]:
    def passes(b: Optional[Dict]) -> bool:
        return (
            b is not None
            and float(b["score"]) >= min_score
            and float(b["similarity"]) >= min_similarity
            and float(b["cue"]) > 0
            and int(b["term_hits"]) > 0
        )

    asian_ok = passes(best_asian)
    latino_ok = passes(best_latino)

    if asian_ok and latino_ok:
        conf = max(float(best_asian["score"]), float(best_latino["score"]))
        evidence = (
            str(best_asian["sentence"])
            if float(best_asian["score"]) >= float(best_latino["score"])
            else str(best_latino["sentence"])
        )
        return "both", conf, evidence
    if asian_ok:
        return "asian", float(best_asian["score"]), str(best_asian["sentence"])
    if latino_ok:
        return "latino", float(best_latino["score"]), str(best_latino["sentence"])
    return "uncertain", 0.0, ""


def process_org(
    row: Dict[str, str],
    min_score: float,
    min_similarity: float,
) -> Tuple[Optional[Dict], List[Dict]]:
    ein = row["ein"]
    candidate_type = row["candidate_type"]
    irs_name = row["irs_name_raw"]
    text = row["about_page_text"]

    sentences = split_sentences(text)
    if not sentences:
        return None, []

    # Batch encode all sentences at once
    norms = [s.lower() for s in sentences]
    embs = _MODEL.encode(sentences, normalize_embeddings=True, show_progress_bar=False)

    evidence_rows: List[Dict] = []
    best_asian: Optional[Dict] = None
    best_latino: Optional[Dict] = None

    for s, s_norm, emb in zip(sentences, norms, embs):
        asian_hits = group_term_hits(s_norm, "asian")
        latino_hits = group_term_hits(s_norm, "latino")
        if asian_hits == 0 and latino_hits == 0:
            continue

        cue = cue_score(s_norm)

        for group, hits in (("asian", asian_hits), ("latino", latino_hits)):
            if hits == 0:
                continue
            sim = float(np.dot(emb, _PROTO_EMBS[group]))
            term_bonus = min(1.0, hits / 2.0)
            score = 0.6 * sim + 0.3 * cue + 0.1 * term_bonus

            rec = {
                "ein": ein,
                "candidate_type": candidate_type,
                "irs_name_raw": irs_name,
                "sentence": s,
                "group": group,
                "score": round(score, 6),
                "similarity": round(sim, 6),
                "cue": round(cue, 6),
                "term_hits": hits,
            }
            evidence_rows.append(rec)

            if group == "asian" and (best_asian is None or score > best_asian["score"]):
                best_asian = rec
            if group == "latino" and (best_latino is None or score > best_latino["score"]):
                best_latino = rec

    if best_asian is None and best_latino is None:
        return None, evidence_rows

    reclass_group, conf, evidence = pick_reclass(
        best_asian, best_latino, min_score=min_score, min_similarity=min_similarity
    )

    reclass_row = {
        "ein": ein,
        "candidate_type": candidate_type,
        "irs_name_raw": irs_name,
        "reclass_group": reclass_group,
        "reclass_confidence": round(conf, 6),
        "reclass_evidence_sentence": evidence,
        "asian_sentence_score": round(float(best_asian["score"]), 6) if best_asian else "",
        "latino_sentence_score": round(float(best_latino["score"]), 6) if best_latino else "",
        "asian_similarity": round(float(best_asian["similarity"]), 6) if best_asian else "",
        "latino_similarity": round(float(best_latino["similarity"]), 6) if best_latino else "",
        "method": "sentence_transformers_all-MiniLM-L6-v2",
    }
    return reclass_row, evidence_rows


def read_candidates_map(path: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ein = norm_ein(row.get("ein", ""))
            if ein:
                out[ein] = {
                    "candidate_type": (row.get("candidate_type") or "").strip(),
                    "irs_name_raw": (row.get("irs_name_raw") or "").strip(),
                }
    return out


def resolve_about_path(path: str) -> str:
    if os.path.exists(path):
        return path
    alt = path.replace("candidate_about_pages.csv", "candidate_about_pages_browser.csv")
    return alt if alt != path and os.path.exists(alt) else path


def main() -> None:
    args = parse_args()

    about_path = resolve_about_path(args.about_input)
    if not os.path.exists(about_path):
        raise SystemExit(f"about_input not found: {args.about_input}")

    os.makedirs(os.path.dirname(args.out_file), exist_ok=True)
    os.makedirs(os.path.dirname(args.evidence_file), exist_ok=True)

    print(f"Loading model: {args.model_name} ...")
    load_model(args.model_name)

    candidates_map = read_candidates_map(args.candidates_input)

    prepared_rows: List[Dict[str, str]] = []
    with open(about_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ein = norm_ein(row.get("ein", ""))
            if not ein:
                continue
            candidate_type = (row.get("candidate_type") or "").strip()
            irs_name = (row.get("irs_name_raw") or "").strip()
            if not candidate_type and ein in candidates_map:
                candidate_type = candidates_map[ein].get("candidate_type", "")
            if not irs_name and ein in candidates_map:
                irs_name = candidates_map[ein].get("irs_name_raw", "")
            if candidate_type != "ethnic_named":
                continue
            text = clean_text(row.get("about_page_text", ""))
            if should_skip_text(text):
                continue
            prepared_rows.append({
                "ein": ein,
                "candidate_type": candidate_type,
                "irs_name_raw": irs_name,
                "about_page_text": text,
            })

    print(f"Processing {len(prepared_rows)} ethnic-named orgs with usable text ...")

    reclass_rows: List[Dict] = []
    evidence_rows: List[Dict] = []

    for i, row in enumerate(prepared_rows):
        rc_row, ev_rows = process_org(row, args.min_score, args.min_similarity)
        if rc_row is not None:
            reclass_rows.append(rc_row)
        evidence_rows.extend(ev_rows)
        if (i + 1) % 500 == 0:
            print(f"  {i + 1}/{len(prepared_rows)} processed ...")

    reclass_rows.sort(key=lambda r: (r["ein"], str(r["reclass_group"])))
    evidence_rows.sort(key=lambda r: (r["ein"], -float(r["score"])))

    reclass_fields = [
        "ein", "candidate_type", "irs_name_raw", "reclass_group",
        "reclass_confidence", "reclass_evidence_sentence",
        "asian_sentence_score", "latino_sentence_score",
        "asian_similarity", "latino_similarity", "method",
    ]
    with open(args.out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=reclass_fields)
        writer.writeheader()
        writer.writerows(reclass_rows)

    evidence_fields = [
        "ein", "candidate_type", "irs_name_raw",
        "group", "score", "similarity", "cue", "term_hits", "sentence",
    ]
    with open(args.evidence_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=evidence_fields)
        writer.writeheader()
        writer.writerows(evidence_rows)

    total = len(reclass_rows)
    kept = sum(1 for r in reclass_rows if r["reclass_group"] in {"asian", "latino", "both"})
    print(f"Wrote {args.out_file} ({total} rows; {kept} upgraded)")
    print(f"Wrote {args.evidence_file} ({len(evidence_rows)} sentence evidence rows)")


if __name__ == "__main__":
    main()
