# panethnic_civic_opportunity

## Raw data structure

This repository contains two raw-data pipelines:

1. **IRS + web data**
   - IRS-derived files are in `raw_data/irs_data/`.
   - Web-derived outputs are in `raw_data/web_data/`.
   - URL linkage files:
     - `raw_data/irs_data/irs_urls.csv`
     - `raw_data/irs_data/irs_urls_websites.csv`
     - `raw_data/irs_data/irs_url_checks.csv`
   - These URL-linked IRS records are used to connect organizations to websites and support web scraping/enrichment.

2. **Org data**
   - The organization dataset generation notes are in `misc/org_data`.
   - This documents how the Asian American and Latino advocacy/CBO organization data were compiled and validated.

Method documentation:
- `misc/Kim_et_al-2025-Scientific_Data.pdf` explains the IRS + web data pipeline.
- `misc/org_data` explains the org-data construction process.

## Output directories

- `processed_data/`: intermediate and pipeline-stage data products (matching, enrichment, population panel, gap tables, topic scoring).
- `outputs/figures/`: presentation-ready figures.
- `outputs/tables/`: presentation/export tables (if created in downstream reporting workflows).

## Org matching and expansion pipeline

Use `src/match_and_expand_orgs.R` to:
- probabilistically link org ground truth (`raw_data/org_data_ground_truth/asian_org.csv` and `raw_data/org_data_ground_truth/latino_org.csv`) to IRS records by name (LinkOrgs-style approach with blocking + bipartite matching),
- report match rate against a target (default: 90%),
- generate additional IRS candidates with similar naming patterns (direct panethnic, ethnic-named, unique/neighbors),
- scrape about-page text for ethnic/unique candidates using `func/get_about_pages.R`.

Example (no scraping):

```bash
Rscript src/match_and_expand_orgs.R \
  --org_dir raw_data/org_data_ground_truth \
  --irs_mbf raw_data/irs_data/irs_mbf.csv \
  --irs_urls raw_data/irs_data/irs_urls.csv \
  --irs_url_checks raw_data/irs_data/irs_url_checks.csv \
  --matching_method linkorgs \
  --linkorgs_algorithm bipartite \
  --fallback_to_fuzzy true \
  --out_dir processed_data/org_matching \
  --match_threshold 0.90 \
  --target_match_rate 0.90 \
  --scrape_about false
```

Outputs:
- `processed_data/org_matching/match_summary.csv`
- `processed_data/org_matching/org_to_irs_matches.csv`
- `processed_data/org_matching/org_to_irs_unmatched.csv`
- `processed_data/org_matching/similar_org_candidates.csv`
- `processed_data/org_matching/candidate_about_pages.csv`

## Project plan (current stage)

Phase 01: Fuzzy match org data to IRS data
- Goal: match org ground-truth records to IRS records using the organization name field.
- Target: at least 90% match rate.
- Approach: LinkOrgs-style probabilistic linkage (paper-aligned) with state/token blocking to control candidate search size; fallback string-distance matching is available.
- Success check: `processed_data/org_matching/match_summary.csv` shows `match_rate >= 0.90`.

Phase 02: Expand to additional similar organizations
- Goal: identify additional IRS organizations similar to org data naming patterns.
- Candidate classes:
  - direct panethnic names (e.g., Asian American, Latino, Hispanic),
  - ethnic-name organizations likely connected to Asian American/Latino communities,
  - unique-name neighbors flagged by overlap with seed naming patterns.
- Output: `processed_data/org_matching/similar_org_candidates.csv`.

Phase 03: Validate ethnic/unique candidates with website text
- Goal: reduce false positives among non-direct names.
- Method: scrape homepage/about-page text for ethnic/unique candidates using `func/get_about_pages.R`.
- Output: `processed_data/org_matching/candidate_about_pages.csv`.

Phase 04: Add civic opportunity and organization type
- Goal: calculate civic opportunity measures and assign organization type for each matched/expanded Asian American and Latino organization.
- Inputs:
  - matched org-to-IRS links from Phase 01,
  - similar-name candidates from Phase 02,
  - about-page text from Phase 03 (for validation and refinement),
  - existing IRS + web civic-opportunity fields in this repo (`raw_data/irs_data/irs_org_activities.csv`, `raw_data/irs_data/irs_nonweb_activities.csv`, `raw_data/web_data/predictions.csv`).
- External script reference:
  - `https://github.com/snfagora/american_civic_opportunity_datasets/tree/main/src`
- Deliverable:
  - a consolidated table for Asian American and Latino organizations with EIN, matched name, org type, and civic opportunity indicators (membership, volunteering, events, civic/political action and related fields).
- Panethnic classification rule (in enrichment):
  - classify as panethnic when either the organization name signals panethnic relevance, or the about-page text explicitly indicates panethnic service scope.
  - about-page service-scope signal takes precedence when present.
  - subgroup-named organizations are included when they explicitly state panethnic service (e.g., a Chinese organization serving Asian Americans; a Mexican organization serving Latino/Latina/Latinx communities).

Phase 05: Add Census population trends and estimate the representation gap
- Goal: test whether Asian American and Latino population growth has outpaced growth in corresponding organizations over the longest feasible horizon (target start: 1980, depending on data availability and comparability).
- Organization scope for gap analysis:
  - restrict to Asian American and Latino organizations that provide civic opportunity in at least one IRS activity dimension: `membership`, `events`, `volunteer`, or `take_action`.
  - treat each of these four dimensions as binary indicators.
- Population source:
  - U.S. Census Bureau decennial Census + ACS.
  - Use decennial series for early historical coverage (including 1980 onward where available), then harmonize with ACS-era estimates for recent years.
  - Variable mappings are explicitly versioned in `misc/census_variable_map.csv`.
- Core population series:
  - Asian American population counts by year.
  - Latino/Hispanic population counts by year.
- Organization series:
  - annual counts of Asian American and Latino organizations from Phases 01-04 (using founding year and/or active-year proxy where available).
- Core comparison outputs:
  - year-by-year growth rates for population vs. organization counts.
  - organization-per-100,000 population trends for each group.
  - county-level population-organization growth gap:
    - estimate population growth by county-year-group (Asian vs Latino) from Census series.
    - estimate organization growth by county-year-group using cumulative organizations by IRS incorporation year (`fnd_yr`).
    - compute the gap as `population growth % - organization growth %`.
  - long-run change decomposition (from the earliest comparable year, ideally 1980) showing whether civic infrastructure is keeping pace with demographic change.
  - case selection for comparisons:
    - counties with the highest positive gap (`population growth > organization growth`),
    - counties with the smallest gap (closest parity between population growth and organization growth).
  - urbanicity comparison:
    - compare gap patterns across urban, suburban, and rural counties.
- Deliverables:
  - merged annual panel with population and organization metrics.
  - summary tables/plots highlighting the population-organization growth gap.
  - selected high-gap and low-gap comparison cases for downstream visualization.
- Script:
  - `src/fetch_population_series.py` (decennial + ACS pull using explicit variable map)
- Script:
  - `src/select_gap_cases.R` (R-based gap scoring and case selection).
  - requirement: `org_enriched` must include a county FIPS column (one of `irs_county_fips`, `county_fips`, `county_geoid`, `fips_county`, `county_geo_id`) for county-level scoring.

Example (Phase 05 case selection in R):

```bash
Rscript src/select_gap_cases.R \
  --org_enriched processed_data/org_enriched/org_civic_enriched.csv \
  --population processed_data/population/population_series.csv \
  --places_input misc/selected_places.csv \
  --out_dir processed_data/gap_analysis \
  --start_year 1980 \
  --top_n 5 \
  --urban_cutoff 50000 \
  --suburban_cutoff 10000
```

Example (Phase 05 population pull with variable map):

```bash
python3 src/fetch_population_series.py \
  --output processed_data/population/population_series.csv \
  --historical-input raw_data/population_manual_1980_2008.csv \
  --places-input misc/selected_places.csv \
  --variable-map misc/census_variable_map.csv
```

Phase 05 outputs:
- `processed_data/gap_analysis/county_gap_scores.csv`
- `processed_data/gap_analysis/selected_county_cases.csv`
- `processed_data/gap_analysis/place_gap_scores.csv`
- `processed_data/gap_analysis/selected_gap_cases.csv`
- `processed_data/gap_analysis/region_gap_scores.csv`
- `processed_data/gap_analysis/urbanicity_gap_scores.csv`
- `processed_data/gap_analysis/selected_places_from_gaps.csv`
- `processed_data/population/population_series.csv` (includes source metadata: `source_id`, `source_dataset`, `var_total`, `var_asian`, `var_latino`)

Phase 06: Visualization and communication
- Goal: visualize population growth, organization growth, and representation gaps at both national and local levels.
- Geographic scope:
  - national trends for Asian American and Latino populations and organizations,
  - selected city/metro case studies across all U.S. regions.
- Suggested outputs:
  - long-run national trend lines (population, organization counts, orgs per 100,000),
  - city-level comparison panels for selected places across regions,
  - gap-focused charts showing where population growth outpaces organization growth most strongly.
- Deliverables:
  - publication-ready figures and a compact city-selection rationale appendix.

Current focus figure scripts (kept):
- `src/visualize_panethnic_trend_over_time.R`
- `src/visualize_panethnic_share_by_category_decade_sizeaware.R`
- `src/visualize_civic_opportunity_simple.R`
- `src/visualize_civic_source_family_composition_by_scope_group.R`
- `src/visualize_panethnic_county_growth_index_map.R`
- `src/visualize_panethnic_flow_share_by_county_size_tier.R`

Current focus figure outputs:
- `outputs/figures/panethnic_trend_over_time.png`
- `outputs/figures/panethnic_share_by_category_decade_sizeaware.png`
- `outputs/figures/civic_opportunity_rate_by_group_scope.png`
- `outputs/figures/civic_source_family_composition_by_scope_group.png`
- `outputs/figures/panethnic_county_growth_index_map.png`
- `outputs/figures/panethnic_flow_share_by_county_size_tier.png`

These scripts read from `processed_data/org_enriched/org_civic_enriched.csv` (directly or via derived analysis table) and reflect embedding-based constituency reclassification merged in Phase `02`.
- county growth index map (restored final version):
  - script: `src/visualize_panethnic_county_growth_index_map.R`
  - output table: `outputs/analysis/panethnic_county_growth_index.csv`
  - output figure: `outputs/figures/panethnic_county_growth_index_map.png`
  - map classes:
    - green classes `1-5`: county growth index quintiles within group,
    - red class: `No panethnic orgs (population suggests presence)`.
- county size-tier flow-share figure (new-incorporation dynamics):
  - script: `src/visualize_panethnic_flow_share_by_county_size_tier.R`
  - output table: `outputs/analysis/panethnic_flow_share_by_county_size_tier_year.csv`
  - output figure: `outputs/figures/panethnic_flow_share_by_county_size_tier.png`
  - metric:
    - uses yearly **new** panethnic incorporations (`fnd_yr`) from `1970` to `2020`,
    - applies 5-year centered rolling average,
    - normalizes by relevant-group county population before share conversion:
      - Asian org flow uses county Asian population (`P1_006N`),
      - Latino org flow uses county Latino population (`P2_002N`),
    - computes within-group shares by year (Asian sums to 100%; Latino sums to 100%).
  - tier definitions (relevant-group county population):
    - `Mega >= 1,000,000`
    - `Large 250,000-999,999`
    - `Mid 100,000-249,999`
    - `Small 50,000-99,999`
    - `Suburban 10,000-49,999`
    - `Rural < 10,000`
  - final plotting choices:
    - grayscale high-contrast lines + distinct linetypes,
    - right-side direct labels (text-first, no marker stubs),
    - `Mega` and `Small` emphasized in line/label contrast,
    - white label lane for annotation readability.
- county type profiling for no-panethnic counties:
  - script: `src/analyze_county_urbanicity_no_panethnic.R`
  - input counties: `outputs/analysis/county_asian_population_no_panethnic_2020.csv`, `outputs/analysis/county_latino_population_no_panethnic_2020.csv`
  - total population source: `processed_data/population/census_county_2020_pl_total_asian_latino.json` (Census 2020 PL, county)
  - urbanicity rule: `urban >= 50,000`, `suburban 10,000-49,999`, `rural < 10,000` (county total population)
  - outputs:
    - `outputs/analysis/county_no_panethnic_urbanicity_2020.csv`
    - `outputs/analysis/county_no_panethnic_urbanicity_summary_2020.csv`
- denominator note for organizational-type-by-decade visuals:
  - total enriched orgs: `12,681`
  - after figure base filters (`panethnic_group` in `asian/latino`, valid `fnd_yr`, non-`unknown` `org_type`): `9,688`
  - plotted in `panethnic_share_by_category_decade` outputs after cell filter `org_n >= 5`: `9,564`
  - excluded only by sparse cell rule (`n < 5`): `124`

Phase 07 (deprecated / not in default pipeline): Supervised ML validation and filtering (ground truth + scraped text)
- Goal: add a supervised validation gate so candidate organizations are retained only when model confidence supports Asian American or Latino classification.
- Method:
  - train on ground-truth labels (`asian_org.csv`, `latino_org.csv`),
  - use cross-validation across multiple learners (`glmnet`, `ranger`, `xgboost`) plus `SuperLearner` ensemble,
  - select the best-performing model by multiple metrics (`macro_f1`, `accuracy`, `auc`, `log_loss`),
  - score all candidates and filter with confidence/margin thresholds.
- Script:
  - `src/train_validate_panethnic_ml.R`
- Core outputs:
  - `processed_data/ml_validation/cv_model_metrics.csv`
  - `processed_data/ml_validation/cv_model_performance.png`
  - `processed_data/ml_validation/model_selection.csv`
  - `processed_data/ml_validation/candidate_predictions_with_ml.csv`
  - `processed_data/ml_validation/candidate_predictions_pass_ml_filter.csv`
  - `processed_data/ml_validation/candidate_predictions_fail_ml_filter.csv`

Example (Phase 07):

```bash
Rscript src/train_validate_panethnic_ml.R \
  --asian_input raw_data/org_data_ground_truth/asian_org.csv \
  --latino_input raw_data/org_data_ground_truth/latino_org.csv \
  --matches_input processed_data/org_matching/org_to_irs_matches.csv \
  --about_input processed_data/org_matching/candidate_about_pages.csv \
  --candidates_input processed_data/org_matching/potential_asian_latino_orgs.csv \
  --out_dir processed_data/ml_validation \
  --folds 5 \
  --confidence_threshold 0.70 \
  --margin_threshold 0.15
```

Note:
- This ML phase is not run in the default pipeline runners because training data scope (advocacy/service-heavy) is narrower than the full target universe of organizations.

## Accuracy and model performance

Ground-truth rule-based classifier (`src/evaluate_org_classifier.R`):
- Accuracy: `97.56%`
- Macro-F1: `98.63%`
- Source: `outputs/evaluation/org_classifier_metrics_overall.csv`

Cross-validated supervised models (`src/train_validate_panethnic_ml.R`):
- `ranger`: accuracy `97.80%`, macro-F1 `97.60%`, AUC `0.9986`
- `xgboost`: accuracy `97.56%`, macro-F1 `97.33%`, AUC `0.9977`
- `superlearner`: accuracy `96.94%`, macro-F1 `96.66%`, AUC `0.9973`
- `glmnet`: accuracy `96.94%`, macro-F1 `96.65%`, AUC `0.9930`
- Best selected model (current run): `ranger`
- Source: `processed_data/ml_validation/cv_model_metrics.csv`, `processed_data/ml_validation/model_selection.csv`

## Bulk scraping and topic scripts

Additional scripts used for large-scale candidate scraping and content scoring:
- `src/extract_potential_orgs.R`:
  - builds candidate universe from IRS names + URL table into
  - `processed_data/org_matching/potential_asian_latino_orgs.csv`
- `src/scrape_about_pages_bulk.R`:
  - resumable batch scraper for all candidates
  - writes `processed_data/org_matching/candidate_about_pages.csv`
- `src/scrape_about_pages_browser.py`:
  - browser-rendered scraper (Playwright) that finds likely About/Who-We-Are pages via menu links
  - extracts mission/history/program-relevant text with resume support and progress logging
  - useful when static scraping returns mostly nav boilerplate or JS-heavy pages
- `src/merge_about_pages_parts.py`:
  - merges multi-worker part outputs into one deduplicated about-page file
  - default dedupe key is `ein`
- `src/dedupe_about_pages.py`:
  - snapshots the current combined about-page file, then writes/keeps unique rows by `ein`
  - outputs a backup file and a `candidate_about_pages_unique.csv` artifact
- `src/analyze_about_topics.R`:
  - tags mentions of safety-net programs and democracy/organizing terms
  - uses `misc/safety_net_dictionary.csv` with state-specific aliases (e.g., `CalFresh` for CA SNAP)
  - writes:
    - `processed_data/topic_analysis/about_topic_summary_overall.csv`
    - `processed_data/topic_analysis/about_topic_summary_by_candidate_type.csv`
    - `processed_data/topic_analysis/about_topic_safety_program_counts.csv`
    - `processed_data/topic_analysis/about_topic_flagged_orgs.csv`
    - `processed_data/topic_analysis/about_topic_scored_all.csv`
- `src/reclassify_panethnic_constituency.py`:
  - sentence-level reclassification for `ethnic_named` organizations
  - requires a sentence that mentions panethnic groups and constituency/service framing
  - combines lexical constraints with dependency-free hash embeddings
  - writes:
    - `processed_data/org_matching/panethnic_constituency_reclass.csv`
    - `processed_data/org_matching/panethnic_constituency_sentence_evidence.csv`

Note:
- In the current runtime environment, external DNS/network calls are blocked, so webpage scraping attempts return timeout errors. Topic counts will remain zero until scraping is run in a network-enabled environment.

If you have worker part files and need to reproduce the merge step manually:

```bash
python3 src/merge_about_pages_parts.py \
  --parts_glob processed_data/org_matching/candidate_about_pages_parts/candidate_about_pages.part*.csv \
  --out_file processed_data/org_matching/candidate_about_pages.csv \
  --dedupe_key ein
```

If you need to preserve the original combined file and create unique-only rows:

```bash
python3 src/dedupe_about_pages.py \
  --input_file processed_data/org_matching/candidate_about_pages.csv \
  --backup_file processed_data/org_matching/candidate_about_pages_original_backup.csv \
  --output_file processed_data/org_matching/candidate_about_pages_unique.csv \
  --dedupe_key ein \
  --overwrite_input true
```

## Pipeline execution order (01-06)

1. `01`: matching/expansion (`src/match_and_expand_orgs.R`)
2. `01b`: resumable bulk scraping (`src/scrape_about_pages_bulk.R`)
3. `01c`: about-page topic scoring (`src/analyze_about_topics.R`)
4. `01d`: sentence-level panethnic constituency reclassification (`src/reclassify_panethnic_constituency.py`)
5. `02`: enrichment (`src/enrich_org_civic_type.R`) using `processed_data/org_matching/similar_org_candidates.csv`
6. `03` + `04`: population fetch + gap selection (`src/fetch_population_series.py`, `src/select_gap_cases.R`)
7. `05` + `06`: visualization + completion/shutdown behavior in runner

Reclassification provenance note:
- `src/enrich_org_civic_type.R` merges the `01d` output (`processed_data/org_matching/panethnic_constituency_reclass.csv`) into `processed_data/org_enriched/org_civic_enriched.csv` and updates `panethnic_group` for eligible `ethnic_named` organizations.
- Downstream panethnic analyses and figures that read `processed_data/org_enriched/org_civic_enriched.csv` (including panethnic trend and organizational-type composition plots) therefore reflect the post reclassification labels, not pre-reclassification labels.

Run-all script:

```bash
./run_pipeline_01_06_and_shutdown.sh
```

Downstream-only runner (starts from existing about-page data):

```bash
./run_downstream_from_about.sh
```

Parallelism note:
- only the embedding-based reclassification step is parallelized via `EMBED_WORKERS` (default `1` in full runners, `4` in downstream-only runner).
- example:

```bash
EMBED_WORKERS=6 EMBED_CHUNK_SIZE=200 ./run_downstream_from_about.sh
```

Resume behavior:
- The runner writes phase checkpoints to `processed_data/pipeline_state/*.done`.
- If interrupted, re-running the script skips completed phases and resumes from the next unfinished phase.
- Bulk webpage scraping is resumable by design and appends progress instead of restarting.
- By default, `SCRAPE_ABOUT=true` in both runner scripts because about-page attributes are required for safety-net/democracy tagging.
- To force a clean rerun from scratch:

```bash
FORCE_RERUN=true ./run_pipeline_01_06_and_shutdown.sh
```

Current note:
- This is a testing stage for pipeline construction and documentation; execution and threshold tuning are handled in the next iteration.
