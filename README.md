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

- `processed_data/`: intermediate and pipeline-stage data products (matching, enrichment, population panel, gap tables, ML filtering, topic scoring).
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

Phase 05: Add Census population trends and estimate the representation gap
- Goal: test whether Asian American and Latino population growth has outpaced growth in corresponding organizations over the longest feasible horizon (target start: 1980, depending on data availability and comparability).
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
  - long-run change decomposition (from the earliest comparable year, ideally 1980) showing whether civic infrastructure is keeping pace with demographic change.
  - case selection for comparisons:
    - cities/places with the highest positive gap (`population growth > organization growth`),
    - cities/places with the smallest gap (closest parity between population and organization growth).
  - urbanicity comparison:
    - compare gap patterns across urban, suburban, and rural places.
- Deliverables:
  - merged annual panel with population and organization metrics.
  - summary tables/plots highlighting the population-organization growth gap.
  - selected high-gap and low-gap comparison cases for downstream visualization.
- Script:
  - `src/fetch_population_series.py` (decennial + ACS pull using explicit variable map)
- Script:
  - `src/select_gap_cases.R` (R-based gap scoring and case selection).

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
- Script:
  - `src/visualize_growth_gap.R` (R-based plotting workflow).

Example (Phase 06 in R):

```bash
Rscript src/visualize_growth_gap.R \
  --org_enriched processed_data/org_enriched/org_civic_enriched.csv \
  --population processed_data/population/population_series.csv \
  --selected_places processed_data/gap_analysis/selected_places_from_gaps.csv \
  --gap_scores processed_data/gap_analysis/place_gap_scores.csv \
  --out_dir outputs/figures
```

Additional Phase 06 output:
- `outputs/figures/urbanicity_gap_comparison.png`

Phase 07: Supervised ML validation and filtering (ground truth + scraped text)
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
- `src/analyze_about_topics.R`:
  - tags mentions of safety-net programs and democracy/organizing terms
  - uses `misc/safety_net_dictionary.csv` with state-specific aliases (e.g., `CalFresh` for CA SNAP)
  - writes:
    - `processed_data/topic_analysis/about_topic_summary_overall.csv`
    - `processed_data/topic_analysis/about_topic_summary_by_candidate_type.csv`
    - `processed_data/topic_analysis/about_topic_safety_program_counts.csv`
    - `processed_data/topic_analysis/about_topic_flagged_orgs.csv`
    - `processed_data/topic_analysis/about_topic_scored_all.csv`

Note:
- In the current runtime environment, external DNS/network calls are blocked, so webpage scraping attempts return timeout errors. Topic counts will remain zero until scraping is run in a network-enabled environment.

## Pipeline execution order (01-07)

1. `01`: matching/expansion (`src/match_and_expand_orgs.R`)
2. `01b`: resumable bulk scraping (`src/scrape_about_pages_bulk.R`)
3. `01c`: about-page topic scoring (`src/analyze_about_topics.R`)
4. `01d`: supervised ML validation/filtering (`src/train_validate_panethnic_ml.R`)
5. `02`: enrichment (`src/enrich_org_civic_type.R`) using ML-pass candidates when available
6. `03` + `04`: population fetch + gap selection (`src/fetch_population_series.py`, `src/select_gap_cases.R`)
7. `05` + `06`: visualization + completion/shutdown behavior in runner

Run-all script:

```bash
./run_pipeline_01_06_and_shutdown.sh
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
