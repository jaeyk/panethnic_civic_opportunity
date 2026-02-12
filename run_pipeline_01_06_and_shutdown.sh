#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

# Load local secrets/env overrides without committing them.
if [[ -f ".env.local" ]]; then
  set -a
  source ./.env.local
  set +a
fi

# Optional overrides
ORG_DIR="${ORG_DIR:-raw_data/org_data_ground_truth}"
IRS_MBF="${IRS_MBF:-raw_data/irs_data/irs_mbf.csv}"
IRS_URLS="${IRS_URLS:-raw_data/irs_data/irs_urls.csv}"
IRS_URL_CHECKS="${IRS_URL_CHECKS:-raw_data/irs_data/irs_url_checks.csv}"
STATE_DIR="${STATE_DIR:-processed_data/pipeline_state}"
FORCE_RERUN="${FORCE_RERUN:-false}"
MATCH_OUT="${MATCH_OUT:-processed_data/org_matching}"
ENRICH_OUT="${ENRICH_OUT:-processed_data/org_enriched}"
POP_OUT="${POP_OUT:-processed_data/population/population_series.csv}"
GAP_OUT="${GAP_OUT:-processed_data/gap_analysis}"
FIG_OUT="${FIG_OUT:-outputs/figures}"
PLACES_INPUT="${PLACES_INPUT:-misc/selected_places.csv}"
HIST_POP_INPUT="${HIST_POP_INPUT:-raw_data/population_manual_1980_2008.csv}"
TOP_N="${TOP_N:-5}"
START_YEAR="${START_YEAR:-1980}"
SCRAPE_LIMIT="${SCRAPE_LIMIT:-500}"
SCRAPE_WORKERS="${SCRAPE_WORKERS:-4}"
SCRAPE_PAGE_TIMEOUT_SEC="${SCRAPE_PAGE_TIMEOUT_SEC:-40}"
SCRAPE_BATCH_SIZE="${SCRAPE_BATCH_SIZE:-50}"
SCRAPE_RETRIES="${SCRAPE_RETRIES:-2}"
SCRAPE_RETRY_WAIT_SEC="${SCRAPE_RETRY_WAIT_SEC:-2}"
CENSUS_API_KEY="${CENSUS_API_KEY:-}"
SHUTDOWN_DELAY_MIN="${SHUTDOWN_DELAY_MIN:-1}"
TOPIC_OUT="${TOPIC_OUT:-processed_data/topic_analysis}"
SAFETY_DICT="${SAFETY_DICT:-misc/safety_net_dictionary.csv}"
ML_OUT="${ML_OUT:-processed_data/ml_validation}"
ASIAN_GT="${ASIAN_GT:-raw_data/org_data_ground_truth/asian_org.csv}"
LATINO_GT="${LATINO_GT:-raw_data/org_data_ground_truth/latino_org.csv}"

# About-page scraping is required for downstream issue attributes.
SCRAPE_ABOUT="${SCRAPE_ABOUT:-true}"

mkdir -p "$STATE_DIR" "$MATCH_OUT" "$ENRICH_OUT" "$(dirname "$POP_OUT")" "$GAP_OUT" "$FIG_OUT" "$TOPIC_OUT" "$ML_OUT"

phase_done() {
  local phase_id="$1"
  [[ "$FORCE_RERUN" != "true" && -f "$STATE_DIR/${phase_id}.done" ]]
}

mark_done() {
  local phase_id="$1"
  touch "$STATE_DIR/${phase_id}.done"
}

format_duration_hms() {
  local total_sec="$1"
  if [[ "$total_sec" -lt 0 ]]; then
    printf "--:--:--"
    return
  fi
  local hh=$(( total_sec / 3600 ))
  local mm=$(( (total_sec % 3600) / 60 ))
  local ss=$(( total_sec % 60 ))
  printf "%02d:%02d:%02d" "$hh" "$mm" "$ss"
}

echo "[01/06] Phase 01: organization linkage and candidate expansion"
if phase_done "01"; then
  echo "  - already completed; skipping (STATE_DIR/${STATE_DIR##*/}/01.done)"
else
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
    --scrape_about false \
    --scrape_limit "$SCRAPE_LIMIT"
  mark_done "01"
fi

ABOUT_PAGES_INPUT="$MATCH_OUT/candidate_about_pages.csv"
if [[ "$SCRAPE_ABOUT" == "true" ]]; then
  echo "[01b/06] Phase 01b: bulk about-page scraping (resumable)"
  if phase_done "01b"; then
    echo "  - already completed; skipping"
  else
    Rscript src/extract_potential_orgs.R \
      --irs_mbf "$IRS_MBF" \
      --irs_urls "$IRS_URLS" \
      --out_file "$MATCH_OUT/potential_asian_latino_orgs.csv"

    # Resumable scrape: appends progress and skips EINs already scraped.
    # If FORCE_RERUN=true, restart scrape output from scratch.
    if [[ "$FORCE_RERUN" == "true" ]]; then
      SCRAPE_OVERWRITE="true"
    else
      SCRAPE_OVERWRITE="false"
    fi

    CANDIDATES_FILE="$MATCH_OUT/potential_asian_latino_orgs.csv"
    TOTAL_SCRAPE_ROWS="$(python3 - "$CANDIDATES_FILE" <<'PY'
import csv
import sys

path = sys.argv[1]
count = 0
with open(path, "r", encoding="utf-8", newline="") as f:
    for row in csv.DictReader(f):
        if (row.get("preferred_link") or "").strip():
            count += 1
print(count)
PY
)"

    if [[ "$TOTAL_SCRAPE_ROWS" -eq 0 ]]; then
      echo "No candidates with preferred_link to scrape."
      python3 src/scrape_about_pages_browser.py \
        --candidates "$CANDIDATES_FILE" \
        --out_file "$ABOUT_PAGES_INPUT" \
        --start_index 1 \
        --batch_size "$SCRAPE_BATCH_SIZE" \
        --overwrite "$SCRAPE_OVERWRITE" \
        --timeout_sec "$SCRAPE_PAGE_TIMEOUT_SEC" \
        --retries "$SCRAPE_RETRIES" \
        --retry_wait_sec "$SCRAPE_RETRY_WAIT_SEC" \
        --headless true \
        --same_domain_only true
    elif [[ "$SCRAPE_WORKERS" -le 1 ]]; then
      python3 src/scrape_about_pages_browser.py \
        --candidates "$CANDIDATES_FILE" \
        --out_file "$ABOUT_PAGES_INPUT" \
        --start_index 1 \
        --batch_size "$SCRAPE_BATCH_SIZE" \
        --overwrite "$SCRAPE_OVERWRITE" \
        --timeout_sec "$SCRAPE_PAGE_TIMEOUT_SEC" \
        --retries "$SCRAPE_RETRIES" \
        --retry_wait_sec "$SCRAPE_RETRY_WAIT_SEC" \
        --headless true \
        --same_domain_only true
    else
      WORKERS="$SCRAPE_WORKERS"
      if [[ "$WORKERS" -gt "$TOTAL_SCRAPE_ROWS" ]]; then
        WORKERS="$TOTAL_SCRAPE_ROWS"
      fi

      PART_DIR="$MATCH_OUT/candidate_about_pages_parts"
      mkdir -p "$PART_DIR"
      CHUNK_SIZE=$(( (TOTAL_SCRAPE_ROWS + WORKERS - 1) / WORKERS ))
      PIDS=()
      PART_FILES=()
      WORKER_LOGS=()

      echo "Parallel scraping: rows=$TOTAL_SCRAPE_ROWS workers=$WORKERS chunk_size=$CHUNK_SIZE"

      for ((w = 1; w <= WORKERS; w++)); do
        START_IDX=$(( (w - 1) * CHUNK_SIZE + 1 ))
        END_IDX=$(( w * CHUNK_SIZE ))
        if [[ "$END_IDX" -gt "$TOTAL_SCRAPE_ROWS" ]]; then
          END_IDX="$TOTAL_SCRAPE_ROWS"
        fi
        if [[ "$START_IDX" -gt "$TOTAL_SCRAPE_ROWS" ]]; then
          break
        fi

        PART_FILE="$PART_DIR/candidate_about_pages.part${w}.csv"
        LOG_FILE="$PART_DIR/candidate_about_pages.part${w}.log"
        PART_FILES+=("$PART_FILE")
        WORKER_LOGS+=("$LOG_FILE")
        echo "  - worker $w scraping indices ${START_IDX}-${END_IDX} -> ${PART_FILE}"
        : > "$LOG_FILE"

        python3 src/scrape_about_pages_browser.py \
          --candidates "$CANDIDATES_FILE" \
          --out_file "$PART_FILE" \
          --start_index "$START_IDX" \
          --end_index "$END_IDX" \
          --batch_size "$SCRAPE_BATCH_SIZE" \
          --overwrite "$SCRAPE_OVERWRITE" \
          --timeout_sec "$SCRAPE_PAGE_TIMEOUT_SEC" \
          --retries "$SCRAPE_RETRIES" \
          --retry_wait_sec "$SCRAPE_RETRY_WAIT_SEC" \
          --headless true \
          --same_domain_only true > "$LOG_FILE" 2>&1 &
        PIDS+=("$!")
      done

      BAR_WIDTH=32
      BAR_T0="$(date +%s)"
      while true; do
        ALIVE=0
        for pid in "${PIDS[@]}"; do
          if kill -0 "$pid" 2>/dev/null; then
            ALIVE=$((ALIVE + 1))
          fi
        done

        DONE_ROWS="$(python3 - "${PART_FILES[@]}" <<'PY'
import csv
import os
import sys

count = 0
for path in sys.argv[1:]:
    if not os.path.exists(path):
        continue
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for _ in reader:
            count += 1
print(count)
PY
)"
        if [[ -z "$DONE_ROWS" ]]; then
          DONE_ROWS=0
        fi
        if [[ "$DONE_ROWS" -gt "$TOTAL_SCRAPE_ROWS" ]]; then
          DONE_ROWS="$TOTAL_SCRAPE_ROWS"
        fi

        FILLED=$(( DONE_ROWS * BAR_WIDTH / TOTAL_SCRAPE_ROWS ))
        EMPTY=$(( BAR_WIDTH - FILLED ))
        FILLED_BAR="$(printf '%*s' "$FILLED" '' | tr ' ' '=')"
        EMPTY_BAR="$(printf '%*s' "$EMPTY" '' | tr ' ' '.')"
        PCT="$(awk "BEGIN {printf \"%.1f\", (100 * $DONE_ROWS / $TOTAL_SCRAPE_ROWS)}")"
        NOW_T="$(date +%s)"
        ELAPSED_SEC=$(( NOW_T - BAR_T0 ))
        if [[ "$DONE_ROWS" -gt 0 ]]; then
          ETA_SEC=$(( ELAPSED_SEC * (TOTAL_SCRAPE_ROWS - DONE_ROWS) / DONE_ROWS ))
        else
          ETA_SEC=-1
        fi
        ELAPSED_TXT="$(format_duration_hms "$ELAPSED_SEC")"
        ETA_TXT="$(format_duration_hms "$ETA_SEC")"
        printf "\r[%s%s] %s/%s (%s%%) workers_alive=%s elapsed=%s ETA=%s" "$FILLED_BAR" "$EMPTY_BAR" "$DONE_ROWS" "$TOTAL_SCRAPE_ROWS" "$PCT" "$ALIVE" "$ELAPSED_TXT" "$ETA_TXT"

        if [[ "$ALIVE" -eq 0 ]]; then
          break
        fi
        sleep 2
      done
      echo

      FAILED=0
      for pid in "${PIDS[@]}"; do
        if ! wait "$pid"; then
          FAILED=1
        fi
      done
      if [[ "$FAILED" -ne 0 ]]; then
        echo "ERROR: one or more scrape workers failed. Recent log tails:"
        for log_file in "${WORKER_LOGS[@]}"; do
          if [[ -f "$log_file" ]]; then
            echo "--- ${log_file} (tail) ---"
            tail -n 20 "$log_file" || true
          fi
        done
        exit 1
      fi

      python3 - "$ABOUT_PAGES_INPUT" "${PART_FILES[@]}" <<'PY'
import csv
import os
import sys

out_file = sys.argv[1]
part_files = [p for p in sys.argv[2:] if os.path.exists(p)]

all_rows = []
fieldnames = None
for path in part_files:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and fieldnames is None:
            fieldnames = reader.fieldnames
        for row in reader:
            all_rows.append(row)

if fieldnames is None:
    fieldnames = ["ein", "preferred_link", "about_page_text", "scrape_status", "selected_about_url", "request_sec", "attempts", "error_message"]

seen = set()
dedup = []
for row in all_rows:
    ein = (row.get("ein") or "").strip()
    if ein and ein in seen:
        continue
    if ein:
        seen.add(ein)
    dedup.append(row)

os.makedirs(os.path.dirname(out_file), exist_ok=True)
with open(out_file, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(dedup)

print(f"Merged {len(part_files)} part files -> {out_file} ({len(dedup)} rows)")
PY
    fi

    mark_done "01b"
  fi
else
  echo "WARNING: SCRAPE_ABOUT=false, skipping required about-page attributes (safety-net/democracy)."
fi

echo "[01c/06] Phase 01c: topic tagging from about pages (safety-net + democracy)"
if phase_done "01c"; then
  echo "  - already completed; skipping"
else
  if [[ ! -f "$ABOUT_PAGES_INPUT" ]]; then
    echo "ERROR: missing $ABOUT_PAGES_INPUT. Run with SCRAPE_ABOUT=true."
    exit 1
  fi
  Rscript src/analyze_about_topics.R \
    --candidates "$MATCH_OUT/similar_org_candidates.csv" \
    --about_pages "$ABOUT_PAGES_INPUT" \
    --safety_dict "$SAFETY_DICT" \
    --out_dir "$TOPIC_OUT"
  mark_done "01c"
fi

echo "[01d/06] Phase 01d: supervised ML validation/filtering (SuperLearner)"
if phase_done "01d"; then
  echo "  - already completed; skipping"
else
  Rscript src/train_validate_panethnic_ml.R \
    --asian_input "$ASIAN_GT" \
    --latino_input "$LATINO_GT" \
    --matches_input "$MATCH_OUT/org_to_irs_matches.csv" \
    --about_input "$ABOUT_PAGES_INPUT" \
    --candidates_input "$MATCH_OUT/similar_org_candidates.csv" \
    --out_dir "$ML_OUT"
  mark_done "01d"
fi

CANDIDATES_FOR_ENRICH="$MATCH_OUT/similar_org_candidates.csv"
if [[ -f "$ML_OUT/candidate_predictions_pass_ml_filter.csv" ]]; then
  CANDIDATES_FOR_ENRICH="$ML_OUT/candidate_predictions_pass_ml_filter.csv"
  echo "Using ML-pass candidates for enrichment: $CANDIDATES_FOR_ENRICH"
fi

echo "[02/06] Phase 04: civic opportunity + organization type enrichment"
if phase_done "02"; then
  echo "  - already completed; skipping"
else
  Rscript src/enrich_org_civic_type.R \
    --matches "$MATCH_OUT/org_to_irs_matches.csv" \
    --candidates "$CANDIDATES_FOR_ENRICH" \
    --about_pages "$ABOUT_PAGES_INPUT" \
    --irs_org_activities raw_data/irs_data/irs_org_activities.csv \
    --irs_nonweb_activities raw_data/irs_data/irs_nonweb_activities.csv \
    --predictions raw_data/web_data/predictions.csv \
    --out_dir "$ENRICH_OUT" \
    --include_uncertain false
  mark_done "02"
fi

echo "[03/06] Phase 05a: fetch population series"
if phase_done "03"; then
  echo "  - already completed; skipping"
else
  if [[ -n "$CENSUS_API_KEY" ]]; then
    python3 src/fetch_population_series.py \
      --output "$POP_OUT" \
      --historical-input "$HIST_POP_INPUT" \
      --places-input "$PLACES_INPUT" \
      --variable-map misc/census_variable_map.csv \
      --api-key "$CENSUS_API_KEY"
  else
    python3 src/fetch_population_series.py \
      --output "$POP_OUT" \
      --historical-input "$HIST_POP_INPUT" \
      --places-input "$PLACES_INPUT" \
      --variable-map misc/census_variable_map.csv
  fi
  mark_done "03"
fi

echo "[04/06] Phase 05b: compute growth gaps + select highest and smallest gap cases"
if phase_done "04"; then
  echo "  - already completed; skipping"
else
  Rscript src/select_gap_cases.R \
    --org_enriched "$ENRICH_OUT/org_civic_enriched.csv" \
    --population "$POP_OUT" \
    --places_input "$PLACES_INPUT" \
    --out_dir "$GAP_OUT" \
    --start_year "$START_YEAR" \
    --top_n "$TOP_N" \
    --urban_cutoff 50000 \
    --suburban_cutoff 10000
  mark_done "04"
fi

echo "[05/06] Phase 06: visualize national + place + urban/suburban/rural comparisons"
if phase_done "05"; then
  echo "  - already completed; skipping"
else
  Rscript src/visualize_growth_gap.R \
    --org_enriched "$ENRICH_OUT/org_civic_enriched.csv" \
    --population "$POP_OUT" \
    --selected_places "$GAP_OUT/selected_places_from_gaps.csv" \
    --gap_scores "$GAP_OUT/place_gap_scores.csv" \
    --out_dir "$FIG_OUT"
  mark_done "05"
fi

echo "[06/06] Pipeline completed successfully. Initiating system shutdown."
mark_done "06"

if [[ "$(uname -s)" == "Darwin" ]]; then
  # macOS usually requires elevated privileges for shutdown.
  sudo shutdown -h +"$SHUTDOWN_DELAY_MIN"
else
  sudo shutdown -h now
fi
