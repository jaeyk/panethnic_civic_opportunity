#!/usr/bin/env python3
"""Sentence-level panethnic constituency reclassification for ethnic-named orgs.

This script scans about-page text and upgrades candidate group labels only when
there is sentence-level evidence that panethnic groups are described as the
organization's constituencies.

Design goals:
- No third-party dependencies (works in restricted environments).
- Use both lexical constraints and an embedding-like similarity signal.
- Emit auditable evidence sentences and confidence scores.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import math
import os
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Dict, List, Optional, Sequence, Tuple

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

TOKEN_RE = re.compile(r"[a-z0-9']+")
SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
BAD_SCRAPE_RE = re.compile(
    r"^(?:scrape_error:|does not have about page|is broken|is flat|php error)",
    re.IGNORECASE,
)

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

_W_PROTO: Dict[str, List[float]] = {}
_W_MIN_SCORE: float = 0.52
_W_MIN_SIMILARITY: float = 0.22
_W_MAX_SENTENCES: int = 30


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--about_input",
        default="processed_data/org_matching/candidate_about_pages.csv",
        help="About-page CSV (must include ein and about_page_text).",
    )
    p.add_argument(
        "--candidates_input",
        default="processed_data/org_matching/similar_org_candidates.csv",
        help="Optional fallback candidate table for candidate_type/name joins.",
    )
    p.add_argument(
        "--out_file",
        default="processed_data/org_matching/panethnic_constituency_reclass.csv",
    )
    p.add_argument(
        "--evidence_file",
        default="processed_data/org_matching/panethnic_constituency_sentence_evidence.csv",
    )
    p.add_argument("--min_score", type=float, default=0.52)
    p.add_argument("--min_similarity", type=float, default=0.22)
    p.add_argument("--max_sentences", type=int, default=30)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--chunk_size", type=int, default=100)
    return p.parse_args()


def norm_ein(x: str) -> str:
    digits = "".join(ch for ch in str(x or "") if ch.isdigit())
    return digits.zfill(9) if digits else ""


def clean_text(x: str) -> str:
    txt = (x or "").strip()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def normalize_sentence(x: str) -> str:
    s = clean_text(x).lower()
    s = re.sub(r"\s+", " ", s)
    return s


def split_sentences(text: str, max_sentences: int) -> List[str]:
    if not text:
        return []
    raw = SPLIT_RE.split(text)
    out: List[str] = []
    for s in raw:
        t = clean_text(s)
        if len(t) < 20:
            continue
        out.append(t)
        if len(out) >= max_sentences:
            break
    return out


def has_phrase(sentence_norm: str, phrase: str) -> bool:
    pattern = r"\b" + re.escape(phrase) + r"\b"
    return bool(re.search(pattern, sentence_norm))


def group_term_hits(sentence_norm: str, group: str) -> int:
    terms = ASIAN_TERMS if group == "asian" else LATINO_TERMS
    return sum(1 for t in terms if has_phrase(sentence_norm, t))


def cue_score(sentence_norm: str) -> float:
    hits = sum(1 for pat in CUE_PATTERNS if re.search(pat, sentence_norm))
    return min(1.0, hits / 3.0)


def token_vector(token: str, dim: int = 192, width: int = 4) -> List[float]:
    """Sparse hashed token embedding with deterministic signed buckets."""
    vec = [0.0] * dim
    h = hashlib.sha1(token.encode("utf-8")).hexdigest()
    for k in range(width):
        start = k * 8
        block = int(h[start : start + 8], 16)
        idx = block % dim
        sign = 1.0 if (block >> 1) % 2 == 0 else -1.0
        vec[idx] += sign
    return vec


def sentence_embedding(text: str, dim: int = 192) -> List[float]:
    toks = TOKEN_RE.findall(normalize_sentence(text))
    if not toks:
        return [0.0] * dim
    vec = [0.0] * dim
    for tk in toks:
        tv = token_vector(tk, dim=dim)
        for i in range(dim):
            vec[i] += tv[i]
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0:
        return vec
    return [v / norm for v in vec]


def cosine(u: Sequence[float], v: Sequence[float]) -> float:
    return sum(a * b for a, b in zip(u, v))


def build_group_prototypes() -> Dict[str, List[float]]:
    out: Dict[str, List[float]] = {}
    for group, lines in PROTOTYPES.items():
        embs = [sentence_embedding(x) for x in lines]
        agg = [0.0] * len(embs[0])
        for e in embs:
            for i, val in enumerate(e):
                agg[i] += val
        norm = math.sqrt(sum(v * v for v in agg))
        out[group] = [v / norm for v in agg] if norm else agg
    return out


def score_sentence(sentence: str, proto: Dict[str, List[float]]) -> Dict[str, float]:
    s_norm = normalize_sentence(sentence)
    emb = sentence_embedding(sentence)
    cue = cue_score(s_norm)

    out: Dict[str, float] = {
        "cue": cue,
        "asian_term_hits": float(group_term_hits(s_norm, "asian")),
        "latino_term_hits": float(group_term_hits(s_norm, "latino")),
    }

    for g in ("asian", "latino"):
        sim = cosine(emb, proto[g])
        term_bonus = min(1.0, out[f"{g}_term_hits"] / 2.0)
        out[f"sim_{g}"] = sim
        out[f"score_{g}"] = 0.6 * sim + 0.3 * cue + 0.1 * term_bonus

    return out


def resolve_about_path(path: str) -> str:
    if os.path.exists(path):
        return path
    alt = path.replace("candidate_about_pages.csv", "candidate_about_pages_browser.csv")
    if alt != path and os.path.exists(alt):
        return alt
    return path


def read_candidates_map(path: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ein = norm_ein(row.get("ein", ""))
            if not ein:
                continue
            out[ein] = {
                "candidate_type": (row.get("candidate_type") or "").strip(),
                "irs_name_raw": (row.get("irs_name_raw") or "").strip(),
            }
    return out


def should_skip_text(text: str) -> bool:
    if not text:
        return True
    t = text.strip()
    if not t:
        return True
    if BAD_SCRAPE_RE.search(t):
        return True
    return False


def pick_reclass(best_asian: Optional[Dict[str, object]], best_latino: Optional[Dict[str, object]], min_score: float, min_similarity: float) -> Tuple[str, float, str]:
    asian_ok = False
    latino_ok = False

    if best_asian is not None:
        asian_ok = (
            float(best_asian["score"]) >= min_score
            and float(best_asian["similarity"]) >= min_similarity
            and float(best_asian["cue"]) > 0
            and int(best_asian["term_hits"]) > 0
        )
    if best_latino is not None:
        latino_ok = (
            float(best_latino["score"]) >= min_score
            and float(best_latino["similarity"]) >= min_similarity
            and float(best_latino["cue"]) > 0
            and int(best_latino["term_hits"]) > 0
        )

    if asian_ok and latino_ok:
        conf = max(float(best_asian["score"]), float(best_latino["score"]))
        evidence = str(best_asian["sentence"]) if float(best_asian["score"]) >= float(best_latino["score"]) else str(best_latino["sentence"])
        return "both", conf, evidence
    if asian_ok:
        return "asian", float(best_asian["score"]), str(best_asian["sentence"])
    if latino_ok:
        return "latino", float(best_latino["score"]), str(best_latino["sentence"])
    return "uncertain", 0.0, ""


def _init_worker(proto: Dict[str, List[float]], min_score: float, min_similarity: float, max_sentences: int) -> None:
    global _W_PROTO, _W_MIN_SCORE, _W_MIN_SIMILARITY, _W_MAX_SENTENCES
    _W_PROTO = proto
    _W_MIN_SCORE = min_score
    _W_MIN_SIMILARITY = min_similarity
    _W_MAX_SENTENCES = max_sentences


def _process_prepared_row(row: Dict[str, str]) -> Tuple[Optional[Dict[str, object]], List[Dict[str, object]]]:
    ein = row["ein"]
    candidate_type = row["candidate_type"]
    irs_name = row["irs_name_raw"]
    text = row["about_page_text"]

    sentences = split_sentences(text, max_sentences=_W_MAX_SENTENCES)
    if not sentences:
        return None, []

    evidence_rows: List[Dict[str, object]] = []
    best_asian: Optional[Dict[str, object]] = None
    best_latino: Optional[Dict[str, object]] = None

    for s in sentences:
        metrics = score_sentence(s, _W_PROTO)
        asian_hits = int(metrics["asian_term_hits"])
        latino_hits = int(metrics["latino_term_hits"])

        if asian_hits > 0:
            rec = {
                "ein": ein,
                "candidate_type": candidate_type,
                "irs_name_raw": irs_name,
                "sentence": s,
                "group": "asian",
                "score": float(metrics["score_asian"]),
                "similarity": float(metrics["sim_asian"]),
                "cue": float(metrics["cue"]),
                "term_hits": asian_hits,
            }
            evidence_rows.append(rec)
            if best_asian is None or rec["score"] > best_asian["score"]:
                best_asian = rec

        if latino_hits > 0:
            rec = {
                "ein": ein,
                "candidate_type": candidate_type,
                "irs_name_raw": irs_name,
                "sentence": s,
                "group": "latino",
                "score": float(metrics["score_latino"]),
                "similarity": float(metrics["sim_latino"]),
                "cue": float(metrics["cue"]),
                "term_hits": latino_hits,
            }
            evidence_rows.append(rec)
            if best_latino is None or rec["score"] > best_latino["score"]:
                best_latino = rec

    if best_asian is None and best_latino is None:
        return None, evidence_rows

    reclass_group, conf, evidence = pick_reclass(
        best_asian,
        best_latino,
        min_score=_W_MIN_SCORE,
        min_similarity=_W_MIN_SIMILARITY,
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
        "method": "lexical_plus_hash_embedding",
    }
    return reclass_row, evidence_rows


def main() -> None:
    args = parse_args()
    about_path = resolve_about_path(args.about_input)
    if not os.path.exists(about_path):
        raise SystemExit(f"about_input not found: {args.about_input}")

    os.makedirs(os.path.dirname(args.out_file), exist_ok=True)
    os.makedirs(os.path.dirname(args.evidence_file), exist_ok=True)

    candidates_map = read_candidates_map(args.candidates_input)
    proto = build_group_prototypes()

    reclass_rows: List[Dict[str, object]] = []
    evidence_rows: List[Dict[str, object]] = []
    prepared_rows: List[Dict[str, str]] = []

    with open(about_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ein = norm_ein(row.get("ein", ""))
            if not ein:
                continue

            candidate_type = (row.get("candidate_type") or "").strip()
            irs_name = (row.get("irs_name_raw") or "").strip()

            if not candidate_type and ein in candidates_map:
                candidate_type = candidates_map[ein].get("candidate_type", "")
            if not irs_name and ein in candidates_map:
                irs_name = candidates_map[ein].get("irs_name_raw", "")

            # Scope: only ethnic-name candidates are eligible for this upgrade.
            if candidate_type != "ethnic_named":
                continue

            text = clean_text(row.get("about_page_text", ""))
            if should_skip_text(text):
                continue

            prepared_rows.append(
                {
                    "ein": ein,
                    "candidate_type": candidate_type,
                    "irs_name_raw": irs_name,
                    "about_page_text": text,
                }
            )

    workers = max(1, int(args.workers))
    chunk_size = max(1, int(args.chunk_size))
    _init_worker(proto, args.min_score, args.min_similarity, args.max_sentences)

    if workers == 1:
        for row in prepared_rows:
            rc_row, ev_rows = _process_prepared_row(row)
            if rc_row is not None:
                reclass_rows.append(rc_row)
            if ev_rows:
                evidence_rows.extend(ev_rows)
    else:
        try:
            with ProcessPoolExecutor(
                max_workers=workers,
                initializer=_init_worker,
                initargs=(proto, args.min_score, args.min_similarity, args.max_sentences),
            ) as ex:
                for rc_row, ev_rows in ex.map(_process_prepared_row, prepared_rows, chunksize=chunk_size):
                    if rc_row is not None:
                        reclass_rows.append(rc_row)
                    if ev_rows:
                        evidence_rows.extend(ev_rows)
        except (PermissionError, OSError):
            # Some restricted environments disallow process semaphores.
            print("Process pool unavailable in this environment; falling back to thread pool.")
            with ThreadPoolExecutor(max_workers=workers) as ex:
                for rc_row, ev_rows in ex.map(_process_prepared_row, prepared_rows):
                    if rc_row is not None:
                        reclass_rows.append(rc_row)
                    if ev_rows:
                        evidence_rows.extend(ev_rows)

    reclass_rows.sort(key=lambda r: (r["ein"], str(r["reclass_group"])))
    evidence_rows.sort(key=lambda r: (r["ein"], -float(r["score"])))

    with open(args.out_file, "w", newline="", encoding="utf-8") as f:
        fields = [
            "ein",
            "candidate_type",
            "irs_name_raw",
            "reclass_group",
            "reclass_confidence",
            "reclass_evidence_sentence",
            "asian_sentence_score",
            "latino_sentence_score",
            "asian_similarity",
            "latino_similarity",
            "method",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in reclass_rows:
            writer.writerow(row)

    with open(args.evidence_file, "w", newline="", encoding="utf-8") as f:
        fields = [
            "ein",
            "candidate_type",
            "irs_name_raw",
            "group",
            "score",
            "similarity",
            "cue",
            "term_hits",
            "sentence",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in evidence_rows:
            writer.writerow(row)

    total = len(reclass_rows)
    kept = sum(1 for r in reclass_rows if r["reclass_group"] in {"asian", "latino", "both"})
    print(f"Wrote {args.out_file} ({total} rows; {kept} upgraded)")
    print(f"Wrote {args.evidence_file} ({len(evidence_rows)} sentence evidence rows)")


if __name__ == "__main__":
    main()
