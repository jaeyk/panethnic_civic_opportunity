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
   - **URL reliability note**: `irs_urls.csv` contains two URL fields per EIN: `irs_url` (self-reported by the organization to the IRS; available for ~16% of ethnic-named orgs) and `first_link` / `preferred_link` (inferred from external crawls). The vast majority of inferred links are correct, but a small number map an EIN to an entirely different organization's website — a consequential error when the wrong page contains panethnic language. Step `04c` targets these rare cases using hard-coded confirmed mismatches, IRS self-reported URL domain conflicts, and bot-blocked content signals.

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

## Methods

We construct the universe of Asian American and Latino organizations from IRS Exempt Organization (EO) Business Master File (BMF) records combined with web-scraped organizational self-descriptions. The IRS BMF provides registered names, employer identification numbers (EINs), incorporation years, and activity codes for all 501(c)-registered nonprofits in the United States. To identify Asian American and Latino organizations, we apply two complementary naming strategies. First, we flag organizations whose IRS-registered name contains direct panethnic keywords (e.g., *Asian American*, *AAPI*, *Latino*, *Hispanic*), yielding a *direct naming* set. Second, we flag organizations whose name contains ethnic subgroup keywords (e.g., *Chinese*, *Korean*, *Vietnamese*, *Mexican*) as *ethnic-named* candidates, recognizing that many organizations serving panethnic communities adopted subgroup-specific names.

To validate and expand classifications beyond naming rules, we scrape the about-page text of each candidate organization's website. We match a hand-curated ground truth of 818 Asian American and Latino advocacy and community-based organizations to IRS records using probabilistic name linkage (bipartite blocking-and-matching; match rate 82.0%). For ethnic-named candidates with scraped text, we apply a sentence-transformer model (`all-MiniLM-L6-v2`) to score every sentence in each organization's about-page against panethnic constituency prototype sentences. An organization is reclassified from *ethnic-named* to *panethnic* when its about-page explicitly describes panethnic service scope (e.g., a Chinese organization stating it serves Asian Americans broadly), clearing both a composite score threshold (≥ 0.40) and a raw cosine similarity threshold (≥ 0.40). Before reclassification, we apply a URL ownership verification step that nullifies about-page text for confirmed URL mismatches — cases where the scraped page belongs to a different organization entirely — as well as IRS-self-reported URL domain conflicts and bot-blocked or spam-contaminated pages. This verification step reduces reclassifications from 212 to 178 (2.1% of 8,473 ethnic-named candidates), yielding a final panethnic count of 3,304 organizations (1,259 Asian American; 2,012 Latino; 33 both).

We validate panethnic classifications using two supervised machine learning tasks trained on the ground-truth set. A group classifier (XGBoost, 5-fold CV accuracy: 96.5%, AUC: 0.996) distinguishes Asian American from Latino organizations across the full candidate set. A panethnic-vs-ethnic classifier (ensemble of lasso, random forest, and XGBoost; 5-fold CV accuracy: 96.6%, AUC: 0.992) distinguishes panethnic organizations from unconfirmed ethnic-named candidates. Organizational types are assigned using a rule-based classifier applied to IRS name tokens and activity codes, producing 15 categories (civic/political, religious, professional, economic, arts and cultural, community, healthcare, housing, education, research, foundations, hobby and sports, social and fraternal, youth, unions); classifier accuracy is 97.6% (macro-F1: 98.6%) against a held-out ground-truth set.

**Figure 1 — Org-type composition flow (`orgtype_flow_great_society_vs_reagan.png`)** compares the organizational type composition of panethnic organizations across two eras defined by IRS incorporation year: the 1960s–1970s (n = 131 Asian American; n = 175 Latino) and Post-1981 (n = 864 Asian American; n = 1,269 Latino). For each group, we compute the proportion of organizations in each type within each period and render the shifts as sigmoid Bézier ribbons. Types that declined in share after 1981 are stacked at the top of each bar (dark grey) and types that increased are stacked at the bottom (light grey), allowing the aggregate recomposition to be read directly from the area of each shaded region.

**Figure 2 — Population vs. organizational presence by urbanicity (`population_org_urbanicity_mismatch.png`)** shows the mismatch between where the Asian American and Latino populations live and where panethnic organizations and civic opportunity are concentrated. For each group, we compare the population share by urbanicity tier (2020 Census county-level counts joined to USDA Rural-Urban Continuum Codes) against the organizational share by urbanicity tier (from `org_civic_enriched.csv`). Urbanicity is defined as: Urban (RUCC 1), Suburban (RUCC 2–3), and Rural (RUCC 4–9), consistent with the classification in `org_civic_enriched.csv`. A civic opportunity index — the average of four binary indicators (membership, volunteering, events, and civic and political action) from IRS activity records — is shown alongside to indicate the quality of civic infrastructure available by place type.

  **Asian American** (n orgs: Urban 1,094; Suburban 134; Rural 20):

  | Urbanicity | Population share | Org share | Civic opp. index |
  | --- | ---: | ---: | ---: |
  | Urban    | 80.1% | 87.7% | 0.31 |
  | Suburban | 17.6% | 10.7% | 0.25 |
  | Rural    |  2.2% |  1.6% | 0.11 |

  **Latino** (n orgs: Urban 1,454; Suburban 478; Rural 73):

  | Urbanicity | Population share | Org share | Civic opp. index |
  | --- | ---: | ---: | ---: |
  | Urban    | 66.4% | 72.5% | 0.24 |
  | Suburban | 27.1% | 23.8% | 0.24 |
  | Rural    |  6.6% |  3.6% | 0.14 |

## Org identification strategy

Organizations are identified through two complementary naming strategies, both
documented in the `detection_strategy` column of `org_civic_enriched.csv`.

**Strategy 1 — Direct panethnic naming (direct_RE)**
IRS names containing panethnic keywords (`asian`, `aapi`, `latino`,
`hispanic`, etc.) are flagged during the IRS name scan in
`01_extract_potential_orgs.R` and labelled `direct_panethnic`.

**Strategy 2 — Indirect panethnic naming (indirect_RE)**
Organizations whose IRS name contains ethnic keywords (`chinese`, `korean`,
`vietnamese`, `mexican`, etc.) are labelled `ethnic_named`. Those whose
scraped about-page text explicitly describes panethnic service scope (e.g., a
Chinese organization that states it serves Asian Americans broadly) are
reclassified to panethnic by `05_reclassify_panethnic_constituency.py` and
appear as `indirect_RE` in `detection_strategy`.

Reclassification uses `sentence-transformers` (`all-MiniLM-L6-v2`) to compute
cosine similarity between each about-page sentence and panethnic prototype
sentences. A sentence must clear both a composite score threshold (`--min_score`,
default 0.40) and a raw similarity threshold (`--min_similarity`, default 0.40)
to trigger an upgrade. All sentences in the page are scanned (no cap).

Reclassification results (current run, `min_score=0.40`, `min_similarity=0.40`):

| Step | Upgraded | Rate | Notes |
| --- | --- | --- | --- |
| Original (hash embedding, ≤30 sentences, score ≥ 0.52) | 131 | 2.1% | baseline |
| Real embeddings (`all-MiniLM-L6-v2`), score ≥ 0.45 | 182 | 3.6% | +51 |
| Lower threshold (score ≥ 0.40) | 212 | 4.1% | +30 |
| + URL ownership verification (04c) | **178** | **2.1%** | −34 |

The −34 from adding URL verification breaks down as: 5 confirmed URL mismatches (IRS linked wrong website), 2 self-reported IRS URL domain mismatches, and 32 blocked/bot-challenged pages whose partial text was removed.

Final breakdown: Asian 132, Latino 45, Both 1 (out of 4,466 `ethnic_named` orgs with usable about-page text).

**Final organization counts (post-reclassification, from `org_civic_enriched.csv`):**

| Scope | Asian | Latino | Total |
| --- | ---: | ---: | ---: |
| Panethnic (direct name + reclassified + ground truth) | 1,259 | 2,012 | 3,304 |
| — of which: reclassified ethnic → panethnic (`indirect_RE`) | 125 | 43 | 169 |
| Ethnic (ethnic-named, not reclassified) | 7,228 | 1,020 | 8,304 |
| **Grand total** | **8,487** | **3,032** | **11,608** |

Note: `indirect_RE` counts (169) differ slightly from step 05's upgraded count (178) because step 06 applies additional cross-referencing filters.

Each org also carries a `detection_method` field:

- `RE` — identified by regex only (direct or indirect)
- `ML` — identified by the panethnic ML classifier only
- `both` — both RE and ML confirm panethnic classification (higher confidence)
- `ground_truth` — directly matched from the hand-curated org lists

**Strategy counts from the most recent run (2026-06-26):**

| detection_strategy   | detection_method | n_orgs | n_asian | n_latino |
|----------------------|------------------|-------:|--------:|---------:|
| ground_truth         | ground_truth     |    640 |     242 |      398 |
| direct_RE            | both             |  1,241 |     466 |      768 |
| direct_RE            | RE               |  1,254 |     426 |      803 |
| indirect_RE          | both             |     14 |      12 |        2 |
| indirect_RE          | RE               |    117 |      87 |       30 |
| ethnic_unconfirmed   | ML               |      8 |       2 |        6 |
| ethnic_unconfirmed   | none             |  8,334 |   7,252 |    1,028 |
| neighbor_RE          | none             |  1,094 |     138 |      956 |

Three key covariates are present in `org_civic_enriched.csv`:

- `fnd_yr` — IRS incorporation year (proxy for founding year)
- `org_type` — predicted 15-category type (`arts`, `civic`, `community`, `econ`, `education`, `foundations`, `health`, `hobby`, `housing`, `professional`, `religious`, `research`, `socialfraternal`, `unions`, `youth`)
- `urbanicity` — `urban` (RUCC=1), `suburban` (RUCC=2–3), or `rural` (RUCC=4–9), from `raw_data/County_Classifications.csv`

## Org matching and expansion pipeline

Use `src/02_match_and_expand_orgs.R` to:

- probabilistically link org ground truth (`raw_data/org_data_ground_truth/asian_org.csv` and `raw_data/org_data_ground_truth/latino_org.csv`) to IRS records by name (LinkOrgs-style approach with blocking + bipartite matching),
- report match rate against a target (default: 90%),
- generate additional IRS candidates with similar naming patterns (direct panethnic, ethnic-named, unique/neighbors),
- scrape about-page text for ethnic/unique candidates using `func/get_about_pages.R`.

Current match rate: **82.0%** (671/818 ground-truth orgs matched at threshold 0.87). See header comment in `src/02_match_and_expand_orgs.R` for the failure-mode breakdown.

Example (no scraping):

```bash
Rscript src/02_match_and_expand_orgs.R \
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

- org-type composition flow figure (1960s–70s vs. Post-1981):
  - script: `src/visualize_orgtype_flow_by_era.R`
  - output figure: `outputs/figures/orgtype_flow_great_society_vs_reagan.png`
  - output table: `outputs/analysis/orgtype_flow_great_society_vs_reagan.csv`
  - scope: panethnic orgs only (`detection_strategy` ∈ `direct_RE`, `indirect_RE`, `ground_truth`); period defined by IRS incorporation year (`fnd_yr`)
  - design: alluvial flow plot with decreasing types (dark grey) stacked at top, increasing types (light grey) at bottom; black lines separate each flow
  - sample sizes: Asian 1960s–70s n=131, Post-1981 n=864; Latino 1960s–70s n=175, Post-1981 n=1,269
  - **Asian American proportion shifts** (1960s–70s → Post-1981, percentage points):

    | Org type | 1960s–70s | Post-1981 | Change (pp) |
    | --- | ---: | ---: | ---: |
    | Civic/political | 32.1% | 10.9% | −21.2 |
    | Religious | 19.8% | 10.4% | −9.4 |
    | Community | 13.0% | 9.4% | −3.6 |
    | Healthcare | 6.1% | 3.1% | −3.0 |
    | Youth | 4.6% | 1.6% | −3.0 |
    | Housing | 3.1% | 2.0% | −1.1 |
    | Professional | 2.3% | 14.7% | +12.4 |
    | Hobby and sports | 1.5% | 10.0% | +8.4 |
    | Economic | 5.3% | 11.2% | +5.9 |
    | Arts and cultural | 5.3% | 10.9% | +5.5 |
    | Social and fraternal | 0.0% | 4.3% | +4.3 |
    | Foundations | 0.0% | 3.1% | +3.1 |
    | Research | 3.8% | 4.9% | +1.0 |
    | Education | 3.1% | 3.4% | +0.3 |

  - **Latino proportion shifts** (1960s–70s → Post-1981, percentage points):

    | Org type | 1960s–70s | Post-1981 | Change (pp) |
    | --- | ---: | ---: | ---: |
    | Religious | 57.1% | 14.3% | −42.9 |
    | Community | 9.7% | 5.8% | −3.9 |
    | Housing | 3.4% | 1.3% | −2.1 |
    | Healthcare | 2.9% | 1.0% | −1.8 |
    | Civic/political | 8.6% | 19.4% | +10.8 |
    | Economic | 3.4% | 13.8% | +10.4 |
    | Professional | 2.3% | 11.3% | +9.1 |
    | Hobby and sports | 0.0% | 5.4% | +5.4 |
    | Social and fraternal | 0.0% | 5.4% | +5.4 |
    | Arts and cultural | 3.4% | 6.5% | +3.1 |
    | Youth | 1.1% | 3.8% | +2.6 |
    | Education | 6.3% | 8.4% | +2.1 |
    | Foundations | 1.1% | 2.2% | +1.1 |
    | Research | 0.0% | 1.0% | +1.0 |

Current focus figures:

- **Org-type composition flow (1960s–70s vs. Post-1981)**
  - script: `src/visualize_orgtype_flow_by_era.R`
  - output figure: `outputs/figures/orgtype_flow_great_society_vs_reagan.png`
  - output table: `outputs/analysis/orgtype_flow_great_society_vs_reagan.csv`
  - scope: panethnic orgs only; periods defined by IRS incorporation year (`fnd_yr`)
  - design: alluvial flow with decreasing types (dark grey) stacked at top, increasing (light grey) at bottom; black separator lines between flows

- **Population vs. organizational presence by urbanicity**
  - script: `src/visualize_population_org_urbanicity.R`
  - output figure: `outputs/figures/population_org_urbanicity_mismatch.png`
  - output table: `outputs/analysis/population_org_urbanicity_mismatch.csv`
  - scope: panethnic orgs only; population from 2020 Census (county-level)
  - design: dumbbell plot (population share vs org share by urbanicity) paired with civic opportunity index bars; urbanicity classified by USDA RUCC 2013 codes (Urban = RUCC 1; Suburban = RUCC 2–3; Rural = RUCC 4–9)
  - civic opportunity index: average of membership, volunteering, events, and civic and political action indicators

Phase 08: Supervised ML validation — group classification + panethnic/ethnic prediction

- Script: `src/08_train_validate_panethnic_ml.R`
- **Task 1 — Asian vs. Latino group classifier**
  - Train on 818 ground-truth orgs; 5-fold cross-validation across `glmnet`, `ranger`, `xgboost`, `SuperLearner`.
  - Best model (current run): `xgboost` (accuracy 0.965, balanced accuracy 0.956, macro-F1 0.961, AUC 0.996).
  - Scores all candidates and applies a confidence/margin filter.
  - Pass ML filter (conf ≥ 0.70, margin ≥ 0.15): **10,764 / 12,677** candidates.
- **Task 2 — Panethnic vs. Ethnic classifier**
  - Positive class (panethnic = 1): 818 ground-truth orgs.
  - Negative class (ethnic = 0): 2,454 ethnic-named IRS candidates not reclassified by Stage 05.
  - Ensemble (avg glmnet + ranger + xgb), 5-fold CV: accuracy 0.966, balanced accuracy 0.941, macro-F1 0.954, AUC 0.992.
  - Scored labels across all 12,677 candidates: panethnic = 1,870, ethnic = 8,976, uncertain = 1,831.
  - Output drives the `detection_method` field in `org_civic_enriched.csv` (upgrades RE → "both" when ML agrees).
- Core outputs:
  - `processed_data/ml_validation/cv_model_metrics.csv`
  - `processed_data/ml_validation/cv_model_performance.png`
  - `processed_data/ml_validation/model_selection.csv`
  - `processed_data/ml_validation/candidate_predictions_with_ml.csv`
  - `processed_data/ml_validation/candidate_predictions_pass_ml_filter.csv`
  - `processed_data/ml_validation/candidate_predictions_fail_ml_filter.csv`
  - `processed_data/ml_validation/candidate_panethnic_predictions.csv`
  - `processed_data/ml_validation/panethnic_classifier_cv_metrics.csv`

Example (Phase 08):

```bash
Rscript src/08_train_validate_panethnic_ml.R \
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

Note: run `08` before `06` so that `candidate_panethnic_predictions.csv` is available for the `detection_method` join in `06_enrich_org_civic_type.R`.

## Accuracy and model performance

Ground-truth rule-based org-type classifier (`src/07_evaluate_org_classifier.R`):

- Accuracy: `97.56%`
- Macro-F1: `98.63%`
- Source: `outputs/evaluation/org_classifier_metrics_overall.csv`

Cross-validated supervised models — **Asian vs. Latino group** (`src/08_train_validate_panethnic_ml.R`):

| Model        | Accuracy | Balanced Acc | Macro-F1 | AUC   |
|--------------|----------|--------------|----------|-------|
| xgboost      | 0.965    | 0.956        | 0.961    | 0.996 |
| ranger       | 0.961    | 0.948        | 0.957    | 0.991 |
| superlearner | 0.951    | 0.935        | 0.946    | 0.995 |
| glmnet       | 0.763    | 0.676        | 0.681    | 0.967 |

Best selected model (current run): `xgboost`

Cross-validated supervised models — **Panethnic vs. Ethnic** (`src/08_train_validate_panethnic_ml.R`):

| Model    | Accuracy | Balanced Acc | Macro-F1 | AUC   |
|----------|----------|--------------|----------|-------|
| ensemble | 0.966    | 0.941        | 0.954    | 0.992 |

Training set: 818 panethnic (ground truth) vs. 2,454 ethnic-named candidates.
Source: `processed_data/ml_validation/cv_model_metrics.csv`, `processed_data/ml_validation/panethnic_classifier_cv_metrics.csv`

## Bulk scraping and topic scripts

Additional scripts used for large-scale candidate scraping and content scoring:

Scripts are numbered by pipeline stage. Core identification pipeline:

- `src/01_extract_potential_orgs.R` — builds the candidate universe from IRS names + URL table; labels each org as `direct_panethnic` or `ethnic_named`; writes `processed_data/org_matching/potential_asian_latino_orgs.csv`
- `src/02_match_and_expand_orgs.R` — probabilistically links ground-truth orgs to IRS records and expands to additional IRS candidates; distinctive-token blocking with ethnic/panethnic term priority; writes match outputs to `processed_data/org_matching/`
- `src/03a_scrape_about_pages_bulk.R` — resumable batch scraper (cache-aware: skips EINs already in `candidate_about_pages.csv`); writes `processed_data/org_matching/candidate_about_pages.csv`
- `src/03b_scrape_about_pages_browser.py` — browser-rendered scraper (Playwright) for JS-heavy pages; EIN-level resume support; writes `processed_data/org_matching/candidate_about_pages_browser.csv`
- `src/04a_merge_about_pages_parts.py` — merges multi-worker part files; default dedupe key is `ein`
- `src/04b_dedupe_about_pages.py` — deduplicates by `ein`; writes backup and `candidate_about_pages_unique.csv`
- `src/04c_verify_about_page_ownership.py` — nullifies about-page text for confirmed URL mismatches (hard-coded EIN exclusions), self-reported IRS URL domain conflicts, and bot-blocked pages; writes `processed_data/org_matching/about_page_ownership_mismatches.csv`
- `src/05_reclassify_panethnic_constituency.py` — sentence-level reclassification for `ethnic_named` orgs; uses `sentence-transformers` (`all-MiniLM-L6-v2`) to score all sentences in the about-page text (no sentence cap) against panethnic constituency prototypes; requires a sentence mentioning panethnic groups with constituency/service framing above score and similarity thresholds; writes `processed_data/org_matching/panethnic_constituency_reclass.csv` and `processed_data/org_matching/panethnic_constituency_sentence_evidence.csv`
- `src/06_enrich_org_civic_type.R` — joins all sources; assigns `detection_strategy`, `detection_method`, `urbanicity`, `org_type`; optionally joins `08` ML panethnic predictions; writes `processed_data/org_enriched/org_civic_enriched.csv` and strategy-count tables
- `src/07_evaluate_org_classifier.R` — evaluates the rule-based org-type classifier against ground truth
- `src/08_train_validate_panethnic_ml.R` — two-task ML: (1) Asian vs. Latino group classification; (2) panethnic vs. ethnic classification; writes ML predictions used by `06`

Analysis scripts (not part of core identification):

- `src/analyze_about_topics.R` — tags safety-net and democracy/organizing mentions; uses `misc/safety_net_dictionary.csv`
- `src/select_gap_cases.R` — population-organization growth gap scoring and case selection

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

## Pipeline execution order

Core identification pipeline (run in this order):

1. `src/01_extract_potential_orgs.R` — IRS name scan → candidate universe
2. `src/02_match_and_expand_orgs.R` — ground-truth matching + IRS expansion
3. `src/03a_scrape_about_pages_bulk.R` — bulk about-page scraping (cache-aware)
4. `src/03b_scrape_about_pages_browser.py` — browser-rendered fallback scraping
5. *(optional)* re-scrape orgs with thin about pages (1–2 sentences): generate a candidates file of short-page orgs, run `03b` against it, and merge improved text back into `candidate_about_pages.csv` before step 6
6. `src/04a_merge_about_pages_parts.py` + `src/04b_dedupe_about_pages.py` — merge/dedupe
6b. `src/04c_verify_about_page_ownership.py` — URL ownership verification (removes confirmed mismatches, irs_url domain conflicts, bot-blocked pages)
7. `src/05_reclassify_panethnic_constituency.py` — ethnic → panethnic reclassification (uses `all-MiniLM-L6-v2`; scans all sentences; default `--min_score 0.40 --min_similarity 0.40`)
8. `src/08_train_validate_panethnic_ml.R` — ML classifiers (run before 06 so panethnic predictions are available)
9. `src/06_enrich_org_civic_type.R` — final enrichment, joins ML output, adds urbanicity

Analysis pipeline (downstream, uses `org_civic_enriched.csv`):

1. `src/fetch_population_series.py` — Census population pull
2. `src/select_gap_cases.R` — population-organization gap scoring

Reclassification provenance note:

- `src/06_enrich_org_civic_type.R` merges the Stage 05 output (`processed_data/org_matching/panethnic_constituency_reclass.csv`) into `org_civic_enriched.csv` and updates `panethnic_group` for eligible `ethnic_named` organizations. This drives the `indirect_RE` detection strategy.
- Downstream panethnic analyses and figures read `processed_data/org_enriched/org_civic_enriched.csv` and therefore reflect post-reclassification labels.

Run-all script:

```bash
./run_pipeline_01_06_and_shutdown.sh
```

Downstream-only runner (starts from existing about-page data):

```bash
./run_downstream_from_about.sh
```

Dependencies note:

- Step 05 requires `sentence-transformers` (`pip install sentence-transformers`). The model `all-MiniLM-L6-v2` is downloaded automatically on first run (~80 MB). The `.venv` in this repo already has it installed.
- Step 05 runs serially (no multiprocessing) because the sentence-transformer model is shared across calls. The `EMBED_WORKERS` environment variable is no longer used.

Parallelism note:

- Only the bulk scraping steps (`03a`, `03b`) benefit from parallelism; step 05 is single-threaded by design.

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
