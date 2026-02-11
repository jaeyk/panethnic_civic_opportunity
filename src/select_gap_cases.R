#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    org_enriched = "processed_data/org_enriched/org_civic_enriched.csv",
    population = "processed_data/population/population_series.csv",
    places_input = "misc/selected_places.csv",
    out_dir = "processed_data/gap_analysis",
    start_year = 1980,
    top_n = 5,
    urban_cutoff = 50000,
    suburban_cutoff = 10000
  )

  if (length(args) == 0) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% c("org_enriched", "population", "places_input", "out_dir")) {
      cfg[[key]] <- val
    } else if (key %in% c("start_year", "top_n", "urban_cutoff", "suburban_cutoff")) {
      cfg[[key]] <- as.integer(val)
    } else {
      stop(sprintf("Unknown argument: --%s", key))
    }
    i <- i + 2L
  }

  cfg
}

norm_group <- function(x) {
  y <- tolower(trimws(as.character(x)))
  y[y %in% c("asian", "asian american", "aapi")] <- "asian"
  y[y %in% c("latino", "latina", "latinx", "hispanic")] <- "latino"
  y
}

get_year_col <- function(dt) {
  opts <- c("F.year", "fnd_yr", "founding_year")
  hit <- opts[opts %in% names(dt)]
  if (length(hit) == 0) stop("Expected one of F.year, fnd_yr, founding_year in org_enriched file")
  hit[[1]]
}

build_org_place_series <- function(org_dt, places_dt, start_year) {
  if (!all(c("ein", "irs_city", "irs_state", "panethnic_group") %in% names(org_dt))) {
    stop("org_enriched file must include ein, irs_city, irs_state, panethnic_group")
  }
  if (!all(c("city", "state_abbr", "geo_id") %in% names(places_dt))) {
    stop("places_input must include city, state_abbr, geo_id")
  }

  year_col <- get_year_col(org_dt)
  org_dt[, year := suppressWarnings(as.integer(get(year_col)))]
  org_dt[, panethnic_group := norm_group(panethnic_group)]
  org_dt <- org_dt[!is.na(year) & year >= start_year & panethnic_group %in% c("asian", "latino")]

  places_dt[, city_norm := toupper(trimws(city))]
  places_dt[, state_norm := toupper(trimws(state_abbr))]
  org_dt[, city_norm := toupper(trimws(irs_city))]
  org_dt[, state_norm := toupper(trimws(irs_state))]

  org_sel <- merge(
    org_dt,
    places_dt[, .(geo_id, city, state_abbr, region_focus, city_norm, state_norm)],
    by = c("city_norm", "state_norm"),
    all = FALSE
  )

  if (nrow(org_sel) == 0L) return(data.table())

  annual <- org_sel[, .(new_orgs = uniqueN(ein)), by = .(geo_id, city, state_abbr, region_focus, panethnic_group, year)]
  setorder(annual, geo_id, panethnic_group, year)
  annual[, cumulative_orgs := cumsum(new_orgs), by = .(geo_id, panethnic_group)]
  annual
}

build_pop_place_series <- function(pop_dt, start_year) {
  pop_dt[, year := suppressWarnings(as.integer(year))]
  places <- pop_dt[tolower(geo_level) == "place" & !is.na(year) & year >= start_year]
  if (nrow(places) == 0L) return(data.table())

  out <- rbindlist(list(
    places[, .(geo_id = as.character(geo_id), year, panethnic_group = "asian", population = suppressWarnings(as.numeric(population_asian)))],
    places[, .(geo_id = as.character(geo_id), year, panethnic_group = "latino", population = suppressWarnings(as.numeric(population_latino)))]
  ), fill = TRUE)

  out <- out[!is.na(population) & population > 0]
  out
}

score_gap <- function(panel_dt) {
  if (nrow(panel_dt) == 0L) return(data.table())

  setorder(panel_dt, geo_id, panethnic_group, year)

  stats <- panel_dt[, {
    y0 <- min(year)
    y1 <- max(year)

    p0 <- population[which.min(year)]
    p1 <- population[which.max(year)]
    o0 <- cumulative_orgs[which.min(year)]
    o1 <- cumulative_orgs[which.max(year)]

    org_per_100k_0 <- ifelse(is.na(p0) || p0 <= 0, NA_real_, o0 / p0 * 100000)
    org_per_100k_1 <- ifelse(is.na(p1) || p1 <= 0, NA_real_, o1 / p1 * 100000)

    .(
      year_start = y0,
      year_end = y1,
      pop_start = p0,
      pop_end = p1,
      org_start = o0,
      org_end = o1,
      pop_growth_pct = ifelse(!is.na(p0) && p0 > 0, (p1 / p0 - 1) * 100, NA_real_),
      org_growth_pct = ifelse(!is.na(o0) && o0 > 0, (o1 / o0 - 1) * 100, NA_real_),
      org_per_100k_start = org_per_100k_0,
      org_per_100k_end = org_per_100k_1,
      org_per_100k_change = org_per_100k_1 - org_per_100k_0
    )
  }, by = .(geo_id, city, state_abbr, region_focus, panethnic_group)]

  stats[, gap_growth_pct := pop_growth_pct - org_growth_pct]
  stats[, abs_gap_growth_pct := abs(gap_growth_pct)]
  stats
}

select_cases <- function(scores, top_n = 5L) {
  valid <- scores[is.finite(gap_growth_pct)]
  if (nrow(valid) == 0L) return(data.table())

  highest <- valid[order(-gap_growth_pct)][1:min(.N, top_n)]
  highest[, case_type := "highest_gap"]

  smallest <- valid[order(abs_gap_growth_pct)][1:min(.N, top_n)]
  smallest[, case_type := "smallest_gap"]

  out <- rbindlist(list(highest, smallest), fill = TRUE)
  unique(out, by = c("geo_id", "panethnic_group", "case_type"))
}

build_region_scores <- function(panel_dt) {
  dt <- copy(panel_dt)
  dt[is.na(region_focus) | trimws(region_focus) == "", region_focus := "unknown"]

  reg <- dt[, .(
    population = sum(population, na.rm = TRUE),
    cumulative_orgs = sum(cumulative_orgs, na.rm = TRUE)
  ), by = .(region_focus, panethnic_group, year)]

  reg[, geo_id := region_focus]
  reg[, city := region_focus]
  reg[, state_abbr := NA_character_]
  reg[, region_focus := region_focus]

  score_gap(reg)
}

classify_urbanicity <- function(pop_total, urban_cutoff = 50000L, suburban_cutoff = 10000L) {
  out <- rep(NA_character_, length(pop_total))
  out[!is.na(pop_total) & pop_total >= urban_cutoff] <- "urban"
  out[!is.na(pop_total) & pop_total < urban_cutoff & pop_total >= suburban_cutoff] <- "suburban"
  out[!is.na(pop_total) & pop_total < suburban_cutoff] <- "rural"
  out
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  org_dt <- fread(cfg$org_enriched, encoding = "UTF-8")
  pop_dt <- fread(cfg$population, encoding = "UTF-8")

  places_dt <- if (file.exists(cfg$places_input)) fread(cfg$places_input, encoding = "UTF-8") else data.table()
  if (nrow(places_dt) == 0L) {
    fwrite(data.table(), file.path(cfg$out_dir, "place_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_gap_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "region_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "urbanicity_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_places_from_gaps.csv"))
    message("places_input is empty or missing. Wrote empty gap-analysis outputs.")
    return(invisible(NULL))
  }

  if (!"region_focus" %in% names(places_dt)) places_dt[, region_focus := NA_character_]
  places_dt[, geo_id := as.character(geo_id)]

  org_place <- build_org_place_series(org_dt, places_dt, cfg$start_year)
  pop_place <- build_pop_place_series(pop_dt, cfg$start_year)

  panel <- merge(
    org_place,
    pop_place,
    by = c("geo_id", "year", "panethnic_group"),
    all = FALSE
  )

  if (nrow(panel) == 0L) {
    fwrite(data.table(), file.path(cfg$out_dir, "place_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_gap_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "region_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_places_from_gaps.csv"))
    message("No overlap between org and population place series. Wrote empty outputs.")
    return(invisible(NULL))
  }

  scores <- score_gap(panel)
  pop_places_all <- pop_dt[tolower(geo_level) == "place"]
  pop_places_all[, year := suppressWarnings(as.integer(year))]
  pop_places_all[, geo_id := as.character(geo_id)]
  pop_places_all[, population_total := suppressWarnings(as.numeric(population_total))]
  latest_tot <- pop_places_all[!is.na(year), .SD[which.max(year)], by = geo_id]
  latest_tot[, urbanicity := classify_urbanicity(population_total, cfg$urban_cutoff, cfg$suburban_cutoff)]

  if ("urbanicity" %in% names(places_dt)) {
    places_dt[, geo_id := as.character(geo_id)]
    manual_u <- unique(places_dt[, .(geo_id, urbanicity_manual = tolower(trimws(as.character(urbanicity))))])
    scores <- merge(scores, manual_u, by = "geo_id", all.x = TRUE)
  } else {
    scores[, urbanicity_manual := NA_character_]
  }

  scores <- merge(scores, latest_tot[, .(geo_id, population_total_latest = population_total, urbanicity_auto = urbanicity)], by = "geo_id", all.x = TRUE)
  scores[, urbanicity := fifelse(!is.na(urbanicity_manual) & urbanicity_manual != "", urbanicity_manual, urbanicity_auto)]

  selected <- select_cases(scores, top_n = cfg$top_n)
  region_scores <- build_region_scores(panel)
  urbanicity_scores <- scores[, .(
    place_n = uniqueN(geo_id),
    median_gap_growth_pct = median(gap_growth_pct, na.rm = TRUE),
    mean_gap_growth_pct = mean(gap_growth_pct, na.rm = TRUE),
    median_org_per_100k_change = median(org_per_100k_change, na.rm = TRUE)
  ), by = .(urbanicity, panethnic_group)]

  selected_places <- unique(selected[, .(city, state_abbr, geo_id, region_focus, urbanicity, panethnic_group, case_type)])

  fwrite(scores, file.path(cfg$out_dir, "place_gap_scores.csv"))
  fwrite(selected, file.path(cfg$out_dir, "selected_gap_cases.csv"))
  fwrite(region_scores, file.path(cfg$out_dir, "region_gap_scores.csv"))
  fwrite(urbanicity_scores, file.path(cfg$out_dir, "urbanicity_gap_scores.csv"))
  fwrite(selected_places, file.path(cfg$out_dir, "selected_places_from_gaps.csv"))

  message(sprintf("Done. Scored places: %s | Selected cases: %s",
                  format(nrow(scores), big.mark = ","),
                  format(nrow(selected), big.mark = ",")))
}

main()
