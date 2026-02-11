#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# Optional overrides
ORG_DIR="${ORG_DIR:-raw_data/org_data_ground_truth}"
IRS_MBF="${IRS_MBF:-raw_data/irs_data/irs_mbf.csv}"
IRS_URLS="${IRS_URLS:-raw_data/irs_data/irs_urls.csv}"
IRS_URL_CHECKS="${IRS_URL_CHECKS:-raw_data/irs_data/irs_url_checks.csv}"
MATCH_OUT="${MATCH_OUT:-outputs/org_matching}"
ENRICH_OUT="${ENRICH_OUT:-outputs/org_enriched}"
POP_OUT="${POP_OUT:-outputs/population/population_series.csv}"
GAP_OUT="${GAP_OUT:-outputs/gap_analysis}"
FIG_OUT="${FIG_OUT:-outputs/figures}"
PLACES_INPUT="${PLACES_INPUT:-misc/selected_places.csv}"
HIST_POP_INPUT="${HIST_POP_INPUT:-raw_data/population_manual_1980_2008.csv}"
TOP_N="${TOP_N:-5}"
START_YEAR="${START_YEAR:-1980}"
SCRAPE_ABOUT="${SCRAPE_ABOUT:-false}"
SCRAPE_LIMIT="${SCRAPE_LIMIT:-500}"
CENSUS_API_KEY="${CENSUS_API_KEY:-}"
SHUTDOWN_DELAY_MIN="${SHUTDOWN_DELAY_MIN:-1}"

echo "[01/06] Phase 01-03: organization linkage, expansion, and optional about-page scraping"
Rscript src/match_and_expand_orgs.R \
  --org_dir "$ORG_DIR" \
  --irs_mbf "$IRS_MBF" \
  --irs_urls "$IRS_URLS" \
  --irs_url_checks "$IRS_URL_CHECKS" \
  --matching_method linkorgs \
  --linkorgs_algorithm bipartite \
  --fallback_to_fuzzy true \
  --out_dir "$MATCH_OUT" \
  --match_threshold 0.90 \
  --target_match_rate 0.90 \
  --scrape_about "$SCRAPE_ABOUT" \
  --scrape_limit "$SCRAPE_LIMIT"

echo "[02/06] Phase 04: civic opportunity + organization type enrichment"
Rscript src/enrich_org_civic_type.R \
  --matches "$MATCH_OUT/org_to_irs_matches.csv" \
  --candidates "$MATCH_OUT/similar_org_candidates.csv" \
  --about_pages "$MATCH_OUT/candidate_about_pages.csv" \
  --irs_org_activities raw_data/irs_data/irs_org_activities.csv \
  --irs_nonweb_activities raw_data/irs_data/irs_nonweb_activities.csv \
  --predictions raw_data/web_data/predictions.csv \
  --out_dir "$ENRICH_OUT" \
  --include_uncertain false

echo "[03/06] Phase 05a: fetch population series"
if [[ -n "$CENSUS_API_KEY" ]]; then
  python3 src/fetch_population_series.py \
    --output "$POP_OUT" \
    --historical-input "$HIST_POP_INPUT" \
    --places-input "$PLACES_INPUT" \
    --api-key "$CENSUS_API_KEY"
else
  python3 src/fetch_population_series.py \
    --output "$POP_OUT" \
    --historical-input "$HIST_POP_INPUT" \
    --places-input "$PLACES_INPUT"
fi

echo "[04/06] Phase 05b: compute growth gaps + select highest and smallest gap cases"
Rscript src/select_gap_cases.R \
  --org_enriched "$ENRICH_OUT/org_civic_enriched.csv" \
  --population "$POP_OUT" \
  --places_input "$PLACES_INPUT" \
  --out_dir "$GAP_OUT" \
  --start_year "$START_YEAR" \
  --top_n "$TOP_N" \
  --urban_cutoff 50000 \
  --suburban_cutoff 10000

echo "[05/06] Phase 06: visualize national + place + urban/suburban/rural comparisons"
Rscript src/visualize_growth_gap.R \
  --org_enriched "$ENRICH_OUT/org_civic_enriched.csv" \
  --population "$POP_OUT" \
  --selected_places "$GAP_OUT/selected_places_from_gaps.csv" \
  --gap_scores "$GAP_OUT/place_gap_scores.csv" \
  --out_dir "$FIG_OUT"

echo "[06/06] Pipeline completed successfully. Initiating system shutdown."

if [[ "$(uname -s)" == "Darwin" ]]; then
  # macOS usually requires elevated privileges for shutdown.
  sudo shutdown -h +"$SHUTDOWN_DELAY_MIN"
else
  sudo shutdown -h now
fi
