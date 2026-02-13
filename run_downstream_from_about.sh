#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env.local" ]]; then
  set -a
  source ./.env.local
  set +a
fi

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
CENSUS_API_KEY="${CENSUS_API_KEY:-}"
TOPIC_OUT="${TOPIC_OUT:-processed_data/topic_analysis}"
SAFETY_DICT="${SAFETY_DICT:-misc/safety_net_dictionary.csv}"

# Parallelize only the embedding reclassification phase.
EMBED_WORKERS="${EMBED_WORKERS:-4}"
EMBED_CHUNK_SIZE="${EMBED_CHUNK_SIZE:-100}"

mkdir -p "$STATE_DIR" "$MATCH_OUT" "$ENRICH_OUT" "$(dirname "$POP_OUT")" "$GAP_OUT" "$FIG_OUT" "$TOPIC_OUT"

phase_done() {
  local phase_id="$1"
  [[ "$FORCE_RERUN" != "true" && -f "$STATE_DIR/${phase_id}.done" ]]
}

mark_done() {
  local phase_id="$1"
  touch "$STATE_DIR/${phase_id}.done"
}

ABOUT_PAGES_INPUT="${ABOUT_PAGES_INPUT:-$MATCH_OUT/candidate_about_pages.csv}"
PANETHNIC_RECLASS_OUT="${PANETHNIC_RECLASS_OUT:-$MATCH_OUT/panethnic_constituency_reclass.csv}"
PANETHNIC_RECLASS_EVIDENCE_OUT="${PANETHNIC_RECLASS_EVIDENCE_OUT:-$MATCH_OUT/panethnic_constituency_sentence_evidence.csv}"

if [[ ! -f "$ABOUT_PAGES_INPUT" ]]; then
  if [[ -f "$MATCH_OUT/candidate_about_pages_browser.csv" ]]; then
    ABOUT_PAGES_INPUT="$MATCH_OUT/candidate_about_pages_browser.csv"
  else
    echo "ERROR: missing about-page input. Expected $ABOUT_PAGES_INPUT or $MATCH_OUT/candidate_about_pages_browser.csv"
    exit 1
  fi
fi

echo "[D1/6] About-page topic tagging"
if phase_done "downstream_01c"; then
  echo "  - already completed; skipping"
else
  Rscript src/analyze_about_topics.R \
    --candidates "$MATCH_OUT/similar_org_candidates.csv" \
    --about_pages "$ABOUT_PAGES_INPUT" \
    --safety_dict "$SAFETY_DICT" \
    --out_dir "$TOPIC_OUT"
  mark_done "downstream_01c"
fi

echo "[D2/6] Panethnic sentence reclassification (embedding step; workers=$EMBED_WORKERS)"
if phase_done "downstream_01d_reclass"; then
  echo "  - already completed; skipping"
else
  python3 src/reclassify_panethnic_constituency.py \
    --about_input "$ABOUT_PAGES_INPUT" \
    --candidates_input "$MATCH_OUT/similar_org_candidates.csv" \
    --out_file "$PANETHNIC_RECLASS_OUT" \
    --evidence_file "$PANETHNIC_RECLASS_EVIDENCE_OUT" \
    --workers "$EMBED_WORKERS" \
    --chunk_size "$EMBED_CHUNK_SIZE"
  mark_done "downstream_01d_reclass"
fi

echo "[D3/6] Civic opportunity + org type enrichment"
if phase_done "downstream_02"; then
  echo "  - already completed; skipping"
else
  Rscript src/enrich_org_civic_type.R \
    --matches "$MATCH_OUT/org_to_irs_matches.csv" \
    --candidates "$MATCH_OUT/similar_org_candidates.csv" \
    --about_pages "$ABOUT_PAGES_INPUT" \
    --reclassifications "$PANETHNIC_RECLASS_OUT" \
    --irs_org_activities raw_data/irs_data/irs_org_activities.csv \
    --irs_nonweb_activities raw_data/irs_data/irs_nonweb_activities.csv \
    --predictions raw_data/web_data/predictions.csv \
    --out_dir "$ENRICH_OUT" \
    --include_uncertain false
  mark_done "downstream_02"
fi

echo "[D4/6] Population series"
if phase_done "downstream_03"; then
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
  mark_done "downstream_03"
fi

echo "[D5/6] Growth-gap selection"
if phase_done "downstream_04"; then
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
  mark_done "downstream_04"
fi

echo "[D6/6] Visualization"
if phase_done "downstream_05"; then
  echo "  - already completed; skipping"
else
  Rscript src/visualize_growth_gap.R \
    --org_enriched "$ENRICH_OUT/org_civic_enriched.csv" \
    --population "$POP_OUT" \
    --selected_places "$GAP_OUT/selected_places_from_gaps.csv" \
    --gap_scores "$GAP_OUT/place_gap_scores.csv" \
    --out_dir "$FIG_OUT"
  mark_done "downstream_05"
fi

echo "Downstream pipeline completed."
mark_done "downstream_06"
