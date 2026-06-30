#!/usr/bin/env python3
"""Verify that scraped about-page text belongs to the IRS-registered organization.

Problem: The IRS URL table sometimes links an EIN to the wrong website — a
directory listing, a defunct domain repurposed by another org, or an affiliated
org sharing the same URL. When a mismatch passes into step 05, reclassification
fires on the wrong org's content.

This script uses two complementary checks:

  1. Hard-coded exclusions: EINs confirmed (via manual audit) to have been
     assigned an entirely wrong URL. Text is nullified unconditionally.

  2. IRS self-reported URL mismatch: when an org filed its own URL with the IRS
     (irs_url field) and the scraped URL is on a different domain, the scraped
     content is from the wrong site. Text is nullified.

  3. Advisory review candidates (NOT auto-nullified): rows where (a) no IRS
     self-reported URL is available to confirm the link AND (b) the scraped
     domain shares no meaningful tokens with the IRS organization name. These
     are written to --review_output for manual inspection.  A previous
     auto-nullification approach was discarded because acronym-based domains
     (e.g. NAKASEC → nakasec.org) and activity-based names produce too many
     false positives when applied as a hard rule; the review list is advisory.

Outputs:
  - Verified about-pages file (same schema, mismatched texts set to "").
  - Mismatch report CSV listing every auto-flagged EIN with diagnostics.
  - Review candidates CSV listing rows that warrant manual inspection.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Dict, List
from urllib.parse import urlparse

# EINs confirmed via manual audit to have a wrong URL in the IRS data.
# The scraped text came from a completely different organization's website.
CONFIRMED_MISMATCHES: Dict[str, str] = {
    "842132619": "committee100.org → should be Chinese American Lawyers Org",
    "521268658": "acrs.org → should be Chinese-American Forum Inc",
    "742782540": "naaotexas.org → should be Chinese Society of Austin Inc",
    "371584421": "acccolorado.org → should be Chinese Divine Culture Association",
    "953645886": "ccrcca.org → should be Valley Korean Community Church",
}

_COMMON_WORDS = frozenset({
    "the", "of", "and", "a", "an", "for", "in", "on", "at", "to", "inc",
    "llc", "org", "corp", "association", "foundation", "center", "society",
    "community", "service", "services", "american", "national", "united",
})

JUNK_TEXT_SIGNALS: List[str] = [
    "cloudflare", "you do not have permission", "captcha",
    "security verification", "access denied",
    "performing security", "please solve", "verifying browser",
    "สล็อต", "คาสิโน",  # Thai gambling spam on repurposed domains
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--about_input",
        default="processed_data/org_matching/candidate_about_pages.csv",
    )
    p.add_argument(
        "--irs_urls",
        default="raw_data/irs_data/irs_urls.csv",
        help="IRS URL table containing self-reported irs_url field.",
    )
    p.add_argument(
        "--out_file",
        default="processed_data/org_matching/candidate_about_pages.csv",
        help="Verified output (can overwrite input).",
    )
    p.add_argument(
        "--mismatch_report",
        default="processed_data/org_matching/about_page_ownership_mismatches.csv",
    )
    p.add_argument(
        "--review_output",
        default="processed_data/org_matching/about_page_review_candidates.csv",
        help=(
            "Advisory review candidates: rows where domain shares no tokens "
            "with org name and no IRS self-reported URL exists. "
            "NOT auto-nullified — for manual inspection only."
        ),
    )
    p.add_argument(
        "--min_text_len", type=int, default=80,
        help="Pages shorter than this are skipped (likely errors).",
    )
    return p.parse_args()


def norm_ein(x: str) -> str:
    digits = "".join(c for c in str(x or "") if c.isdigit())
    return digits.zfill(9) if digits else ""


def base_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc.lower()
        netloc = re.sub(r"^www\.", "", netloc).split(":")[0]
        return netloc
    except Exception:
        return ""


def is_junk_text(text: str) -> bool:
    t = text.lower()
    return any(sig in t for sig in JUNK_TEXT_SIGNALS)


_IRS_URL_PLACEHOLDERS = {"na", "n/a", "none", "null", "nan", "#n/a", ""}


def load_irs_urls(path: str) -> Dict[str, str]:
    """Return {ein: irs_url} for orgs with a valid self-reported IRS URL."""
    out: Dict[str, str] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            ein = row.get("ein", "").strip().zfill(9)
            irs_url = (row.get("irs_url") or "").strip()
            if ein and irs_url.lower() not in _IRS_URL_PLACEHOLDERS and irs_url.startswith("http"):
                out[ein] = irs_url
    return out


def _name_tokens(name: str) -> set:
    """Significant words (≥ 4 chars, not common) from an IRS org name."""
    words = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    return {w for w in words if len(w) >= 4 and w not in _COMMON_WORDS}


def _acronym(name: str) -> str:
    """First-letter acronym of significant words in org name."""
    words = re.sub(r"[^a-z ]", "", name.lower()).split()
    sig = [w for w in words if w and w not in {"of", "the", "and", "for", "in", "a"}]
    return "".join(w[0] for w in sig)


def is_suspicious_for_review(name: str, domain: str, irs_url: str) -> bool:
    """
    Return True if this row is an advisory review candidate.

    Conditions (all must hold):
      1. No IRS self-reported URL to confirm the link.
      2. Domain shares no significant token (≥ 4 chars) with org name.
      3. Domain does not contain the org name acronym (≥ 3 chars).

    This is NOT used for auto-nullification — it flags rows for human inspection.
    Acronym-based and activity-based domains frequently fail the token test but
    are correct (e.g. nakasec.org for Korean Community Service Center LA).
    """
    if irs_url:
        return False  # IRS URL available — normal mismatch check already covers this
    toks = _name_tokens(name)
    if not toks:
        return False
    domain_lc = domain.lower()
    if any(tok in domain_lc for tok in toks):
        return False
    acro = _acronym(name)
    if len(acro) >= 3 and acro in domain_lc:
        return False
    return True


def flag_reason(ein: str, url: str, text: str, irs_url: str) -> str | None:
    """Return a reason string if this row should be flagged, else None."""
    # 1. Hard-coded confirmed mismatches
    if ein in CONFIRMED_MISMATCHES:
        return f"confirmed_mismatch: {CONFIRMED_MISMATCHES[ein]}"

    if not text or len(text) < 10:
        return None  # nothing to check

    # 2. Junk text content (spam/bots/blocked pages)
    if is_junk_text(text):
        return "junk_text"

    # 3. IRS self-reported URL differs from scraped domain
    if irs_url:
        irs_dom = base_domain(irs_url)
        scraped_dom = base_domain(url)
        if irs_dom and scraped_dom and irs_dom != scraped_dom:
            return f"irs_url_mismatch: irs={irs_dom} scraped={scraped_dom}"

    return None


def main() -> None:
    args = parse_args()

    os.makedirs(os.path.dirname(args.out_file), exist_ok=True)
    os.makedirs(os.path.dirname(args.mismatch_report), exist_ok=True)

    print(f"Loading IRS self-reported URLs from {args.irs_urls} ...")
    irs_url_map = load_irs_urls(args.irs_urls)
    print(f"  {len(irs_url_map):,} orgs with self-reported irs_url")

    with open(args.about_input, encoding="utf-8") as _fh:
        rows = list(csv.DictReader(_fh))
    fields = list(rows[0].keys()) if rows else []

    verified_rows: List[Dict] = []
    mismatch_rows: List[Dict] = []
    review_rows: List[Dict] = []
    skipped = flagged = passed = 0

    for row in rows:
        ein = norm_ein(row.get("ein", ""))
        name = (row.get("irs_name_raw") or "").strip()
        url = (row.get("preferred_link") or row.get("selected_about_url") or "").strip()
        text = (row.get("about_page_text") or "").strip()

        irs_url = irs_url_map.get(ein, "")
        reason = flag_reason(ein, url, text, irs_url)

        if reason is None:
            verified_rows.append(row)
            if text and len(text) >= args.min_text_len:
                passed += 1
                # Advisory check: flag for human review (does NOT nullify text).
                domain = base_domain(url)
                if domain and is_suspicious_for_review(name, domain, irs_url):
                    review_rows.append({
                        "ein": ein,
                        "irs_name_raw": name,
                        "domain": domain,
                        "url": url,
                        "char_count": len(text),
                        "text_preview": text[:200],
                    })
            else:
                skipped += 1
        else:
            flagged_row = dict(row)
            flagged_row["about_page_text"] = ""
            verified_rows.append(flagged_row)
            flagged += 1
            mismatch_rows.append({
                "ein": ein,
                "irs_name_raw": name,
                "url": url,
                "irs_url": irs_url,
                "reason": reason,
                "text_preview": text[:200],
            })

    with open(args.out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(verified_rows)

    mismatch_fields = ["ein", "irs_name_raw", "url", "irs_url", "reason", "text_preview"]
    with open(args.mismatch_report, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=mismatch_fields)
        writer.writeheader()
        writer.writerows(mismatch_rows)

    os.makedirs(os.path.dirname(args.review_output) or ".", exist_ok=True)
    review_fields = ["ein", "irs_name_raw", "domain", "url", "char_count", "text_preview"]
    with open(args.review_output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=review_fields)
        writer.writeheader()
        writer.writerows(review_rows)

    print(f"Total rows: {len(rows)}")
    print(f"  Skipped (no/short text):    {skipped}")
    print(f"  Verified:                   {passed}")
    print(f"  Flagged (text nullified):   {flagged}")
    print(f"  Review candidates (advisory, text kept): {len(review_rows)}")
    print(f"Verified file:   {args.out_file}")
    print(f"Mismatch report: {args.mismatch_report}  ({len(mismatch_rows)} rows)")
    print(f"Review output:   {args.review_output}  ({len(review_rows)} rows)")


if __name__ == "__main__":
    main()
