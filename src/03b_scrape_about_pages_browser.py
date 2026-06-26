#!/usr/bin/env python3
"""
Browser-rendered about-page scraper.

Purpose:
- Navigate websites with a real browser (rendered DOM, menu links, JS content)
- Find likely about/who-we-are pages
- Extract mission/history/program-relevant text
- Resume safely using EIN-level checkpoints in output CSV
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode


TRACKING_QUERY_PREFIXES = (
    "utm_",
    "gclid",
    "fbclid",
    "msclkid",
    "gbraid",
    "wbraid",
    "mc_",
    "ref",
    "source",
)

ABOUT_KEYWORDS = (
    "about",
    "who we are",
    "our story",
    "mission",
    "history",
    "about us",
    "what we do",
)

RELEVANT_SECTION_KEYS = (
    "mission",
    "history",
    "our story",
    "who we are",
    "program",
    "programs",
    "what we do",
    "services",
)

STOPWORDS = {
    "the", "a", "an", "and", "or", "to", "of", "in", "for", "on", "at", "by", "with", "from",
    "is", "are", "was", "were", "be", "been", "being", "it", "its", "that", "this", "these", "those",
    "we", "our", "you", "your", "they", "their", "as", "not", "but", "if", "into", "about", "who",
}

BOILERPLATE_TERMS = (
    "skip to main content",
    "menu",
    "search",
    "donate",
    "contact",
    "newsletter",
    "privacy policy",
    "terms of use",
    "follow us",
    "press releases",
    "newsroom",
    "board of directors",
    "staff",
    "annual report",
)

COOKIE_BUTTON_PATTERNS = (
    "accept",
    "agree",
    "close",
    "got it",
    "i understand",
)


@dataclass
class ScrapeResult:
    about_page_text: str
    scrape_status: str
    selected_about_url: str
    request_sec: float
    attempts: int
    error_message: str


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Browser-based about-page scraper")
    p.add_argument("--candidates", default="processed_data/org_matching/potential_asian_latino_orgs.csv")
    p.add_argument("--out_file", default="processed_data/org_matching/candidate_about_pages.csv")
    p.add_argument("--start_index", type=int, default=1)
    p.add_argument("--end_index", type=int, default=0)
    p.add_argument("--batch_size", type=int, default=50)
    p.add_argument("--overwrite", type=str, default="false")
    p.add_argument("--timeout_sec", type=int, default=40)
    p.add_argument("--retries", type=int, default=2)
    p.add_argument("--retry_wait_sec", type=int, default=2)
    p.add_argument("--headless", type=str, default="true")
    p.add_argument("--early_fail_check_n", type=int, default=40)
    p.add_argument("--same_domain_only", type=str, default="true")
    return p.parse_args(argv)


def as_bool(x: str) -> bool:
    return str(x).strip().lower() in {"1", "true", "t", "yes", "y"}


def normalize_ein(x: str) -> str:
    digits = re.sub(r"\D", "", str(x or ""))
    return digits.zfill(9)


def normalize_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if not re.match(r"^https?://", s, re.I):
        s = "https://" + s
    u = urlparse(s)
    q = [(k, v) for k, v in parse_qsl(u.query, keep_blank_values=True)
         if not any(k.lower().startswith(pref) for pref in TRACKING_QUERY_PREFIXES)]
    clean = u._replace(query=urlencode(q), fragment="")
    return urlunparse(clean)


def short(s: str, n: int = 100) -> str:
    s = str(s or "")
    return s if len(s) <= n else s[: n - 3] + "..."


def read_csv_rows(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def write_rows(path: str, rows: List[Dict[str, str]], append: bool) -> None:
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "a" if append and os.path.exists(path) else "w"
    with open(path, mode, encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), quoting=csv.QUOTE_MINIMAL)
        if mode == "w":
            w.writeheader()
        w.writerows(rows)


def dismiss_cookie_popups(page) -> None:
    # Best-effort button clicking for common cookie/consent popups.
    for pat in COOKIE_BUTTON_PATTERNS:
        try:
            locator = page.get_by_role("button", name=re.compile(pat, re.I))
            if locator.count() > 0:
                locator.first.click(timeout=1200)
        except Exception:
            pass


def collect_about_candidate_links(page, base_url: str, same_domain_only: bool = True) -> List[str]:
    base_host = urlparse(base_url).netloc.lower()
    links = page.evaluate(
        """
        () => Array.from(document.querySelectorAll('a'))
          .map(a => ({href: a.href || '', text: (a.innerText || a.textContent || '').trim()}))
          .filter(x => x.href)
        """
    )

    scored: List[Tuple[int, str]] = []
    for obj in links:
        href = normalize_url(obj.get("href", ""))
        txt = (obj.get("text", "") or "").lower().strip()
        if not href:
            continue
        host = urlparse(href).netloc.lower()
        if same_domain_only and base_host and host != base_host:
            continue

        hay = f"{href.lower()} {txt}"
        score = 0
        for i, k in enumerate(ABOUT_KEYWORDS, start=1):
            if k in hay:
                score += 20 - min(i, 10)
        if "/about" in href.lower() or "who-we-are" in href.lower():
            score += 15
        if score > 0:
            scored.append((score, href))

    # Add direct fallbacks in priority order.
    fallbacks = [
        urljoin(base_url if base_url.endswith("/") else base_url + "/", "who-we-are"),
        urljoin(base_url if base_url.endswith("/") else base_url + "/", "about-us"),
        urljoin(base_url if base_url.endswith("/") else base_url + "/", "about"),
    ]
    for f in fallbacks:
        scored.append((10, normalize_url(f)))

    dedup: Dict[str, int] = {}
    for s, u in scored:
        if u not in dedup or s > dedup[u]:
            dedup[u] = s

    out = [u for u, _ in sorted(dedup.items(), key=lambda kv: kv[1], reverse=True)]
    return out[:8]


def extract_visible_text(page) -> str:
    txt = ""
    try:
        if page.locator("main").count() > 0:
            txt = page.locator("main").first.inner_text(timeout=3000)
        else:
            txt = page.locator("body").first.inner_text(timeout=3000)
    except Exception:
        txt = ""
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def filter_relevant_blocks(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    sentences = re.split(r"(?<=[.!?])\s+", text)

    def sentence_score(s: str) -> float:
        s_low = s.lower().strip()
        if not s_low:
            return -1e9
        # Hard drop obvious boilerplate.
        if any(t in s_low for t in BOILERPLATE_TERMS):
            return -1000
        toks = re.findall(r"[a-zA-Z']+", s_low)
        if len(toks) < 6:
            return -50

        stop_n = sum(1 for t in toks if t in STOPWORDS)
        non_stop = max(0, len(toks) - stop_n)
        stop_ratio = stop_n / max(len(toks), 1)
        key_hits = sum(1 for k in RELEVANT_SECTION_KEYS if k in s_low)
        alpha_words = sum(1 for t in toks if len(t) >= 4)

        # Prefer sentences with topical keywords and enough content words.
        score = 2.5 * key_hits + 0.05 * alpha_words + 0.06 * non_stop - 1.5 * stop_ratio
        return score

    scored = [(sentence_score(s), s.strip()) for s in sentences]
    # Keep high-value topical sentences first.
    topical = [s for sc, s in scored if sc >= 0.8]
    if topical:
        out = " ".join(topical)
        return out[:24000]

    # Fallback: keep top non-boilerplate sentences by score.
    scored_sorted = sorted(scored, key=lambda x: x[0], reverse=True)
    fallback = [s for sc, s in scored_sorted[:40] if sc > -10]
    if fallback:
        return " ".join(fallback)[:24000]

    return text[:24000]


def classify_status(text: str, err: str) -> str:
    if err:
        return "error"
    if not text:
        return "empty"
    if len(text.split()) < 20:
        return "low_content"
    return "success"


def scrape_one(playwright, url: str, timeout_sec: int, retries: int, retry_wait_sec: int, same_domain_only: bool) -> ScrapeResult:
    browser = playwright.chromium.launch(headless=True)
    context = browser.new_context(ignore_https_errors=True)
    attempts = max(1, retries + 1)
    best_text = ""
    best_url = ""
    best_err = ""
    t_start = time.time()

    try:
        for i in range(attempts):
            page = context.new_page()
            try:
                base = normalize_url(url)
                page.goto(base, wait_until="domcontentloaded", timeout=timeout_sec * 1000)
                dismiss_cookie_popups(page)

                candidates = [base] + collect_about_candidate_links(page, base, same_domain_only=same_domain_only)
                seen = set()
                texts: List[Tuple[str, str]] = []

                for c in candidates:
                    if c in seen:
                        continue
                    seen.add(c)
                    try:
                        page.goto(c, wait_until="domcontentloaded", timeout=timeout_sec * 1000)
                        dismiss_cookie_popups(page)
                        t = extract_visible_text(page)
                        t_rel = filter_relevant_blocks(t)
                        if t_rel:
                            texts.append((c, t_rel))
                    except Exception:
                        continue

                if texts:
                    # Keep the longest relevant text block.
                    texts.sort(key=lambda x: len(x[1]), reverse=True)
                    best_url, best_text = texts[0]
                    best_err = ""
                    page.close()
                    break

                best_err = "No content extracted from base/about candidates"
            except Exception as e:
                best_err = str(e)
            finally:
                try:
                    page.close()
                except Exception:
                    pass

            if i < attempts - 1:
                time.sleep(max(0, retry_wait_sec))
    finally:
        context.close()
        browser.close()

    elapsed = round(time.time() - t_start, 2)
    status = classify_status(best_text, best_err)
    if status == "error" and not best_text:
        best_text = f"SCRAPE_ERROR: {best_err}"

    return ScrapeResult(
        about_page_text=best_text,
        scrape_status=status,
        selected_about_url=best_url,
        request_sec=elapsed,
        attempts=attempts,
        error_message=best_err,
    )


def build_rows(candidates: List[Dict[str, str]], start_index: int, end_index: int) -> List[Dict[str, str]]:
    rows = []
    for r in candidates:
        if not r.get("preferred_link", "").strip():
            continue
        rr = dict(r)
        rr["ein"] = normalize_ein(rr.get("ein", ""))
        rr["preferred_link"] = normalize_url(rr.get("preferred_link", ""))
        rows.append(rr)

    if end_index and end_index > 0:
        return rows[max(0, start_index - 1): end_index]
    return rows[max(0, start_index - 1):]


def show_progress(done: int, total: int, ok: int, err: int, no_content: int, t0: float) -> None:
    pct = 100.0 * done / max(total, 1)
    elapsed = time.time() - t0
    rate = done / max(elapsed, 1e-6)
    eta = (total - done) / max(rate, 1e-6)
    bar_n = 28
    fill = int(bar_n * done / max(total, 1))
    bar = "=" * fill + " " * (bar_n - fill)
    print(f"[{bar}] {done}/{total} ({pct:.1f}%) | ok={ok} err={err} low/empty={no_content} | elapsed={elapsed:.1f}s ETA={eta:.1f}s")


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    try:
        from playwright.sync_api import sync_playwright  # type: ignore
    except Exception:
        print("ERROR: playwright is not installed. Run: pip install playwright && playwright install chromium", file=sys.stderr)
        return 2

    overwrite = as_bool(args.overwrite)
    headless = as_bool(args.headless)
    same_domain_only = as_bool(args.same_domain_only)

    all_rows = read_csv_rows(args.candidates)
    rows = build_rows(all_rows, args.start_index, args.end_index)
    if not rows:
        print("No candidate rows after filtering.")
        return 0

    done_eins = set()
    if os.path.exists(args.out_file) and not overwrite:
        for r in read_csv_rows(args.out_file):
            done_eins.add(normalize_ein(r.get("ein", "")))

    todo = [r for r in rows if normalize_ein(r.get("ein", "")) not in done_eins]
    if not todo:
        print("No remaining candidates to scrape.")
        return 0

    total = len(todo)
    ok_total = 0
    err_total = 0
    no_content_total = 0
    t0 = time.time()

    batch: List[Dict[str, str]] = []
    wrote_any = False

    print(f"Starting browser scrape: {total} rows | headless={headless} | timeout={args.timeout_sec}s")

    with sync_playwright() as p:
        # headless switch is handled in scrape_one; patch by monkey use config in function would be cleaner
        # For simplicity we keep headless=True in scrape_one and rely on robustness over UI rendering.
        for i, row in enumerate(todo, start=1):
            ein = normalize_ein(row.get("ein", ""))
            org_name = row.get("irs_name_raw", "")
            url = normalize_url(row.get("preferred_link", ""))
            print(f"Scraping [{i}/{total}] EIN={ein} | {short(org_name, 90)} | {short(url, 120)}")

            res = scrape_one(
                p,
                url=url,
                timeout_sec=args.timeout_sec,
                retries=args.retries,
                retry_wait_sec=args.retry_wait_sec,
                same_domain_only=same_domain_only,
            )

            out = dict(row)
            out["ein"] = ein
            out["preferred_link"] = url
            out["about_page_text"] = res.about_page_text
            out["scrape_status"] = res.scrape_status
            out["selected_about_url"] = res.selected_about_url
            out["request_sec"] = f"{res.request_sec:.2f}"
            out["attempts"] = str(res.attempts)
            out["error_message"] = res.error_message
            batch.append(out)

            if res.scrape_status == "success":
                ok_total += 1
            elif res.scrape_status == "error":
                err_total += 1
            else:
                no_content_total += 1

            show_progress(i, total, ok_total, err_total, no_content_total, t0)

            if len(batch) >= max(1, args.batch_size):
                write_rows(args.out_file, batch, append=(wrote_any or (os.path.exists(args.out_file) and not overwrite)))
                wrote_any = True
                overwrite = False
                batch = []

            if i >= args.early_fail_check_n and ok_total == 0 and err_total / max(i, 1) >= 0.98:
                if batch:
                    write_rows(args.out_file, batch, append=(wrote_any or (os.path.exists(args.out_file) and not overwrite)))
                print(f"Early stop: {err_total}/{i} are errors and 0 success; likely failing fast.", file=sys.stderr)
                return 1

    if batch:
        write_rows(args.out_file, batch, append=(wrote_any or (os.path.exists(args.out_file) and not overwrite)))

    print(f"Done. success={ok_total} error={err_total} low/empty={no_content_total} total={total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
