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

normalize_geo_id <- function(x) {
  y <- gsub("[^0-9]", "", as.character(x))
  y[y == ""] <- NA_character_
  sprintf("%05s", y)
}

get_year_col <- function(dt) {
  opts <- c("F.year", "fnd_yr", "founding_year")
  hit <- opts[opts %in% names(dt)]
  if (length(hit) == 0) stop("Expected one of F.year, fnd_yr, founding_year in org_enriched file")
  hit[[1]]
}

get_county_col <- function(dt) {
  opts <- c("irs_county_fips", "county_fips", "county_geoid", "fips_county", "county_geo_id")
  hit <- opts[opts %in% names(dt)]
  if (length(hit) == 0) {
    stop("County-level gap analysis requires one of these columns in org_enriched: irs_county_fips, county_fips, county_geoid, fips_county, county_geo_id")
  }
  hit[[1]]
}

to_binary <- function(x) {
  y <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(y))
  out[!is.na(y)] <- ifelse(y[!is.na(y)] > 0, 1, 0)
  out
}

build_org_county_series <- function(org_dt, start_year) {
  req <- c("ein", "panethnic_group")
  if (!all(req %in% names(org_dt))) {
    stop("org_enriched file must include ein and panethnic_group")
  }

  year_col <- get_year_col(org_dt)
  county_col <- get_county_col(org_dt)

  org_dt[, year := suppressWarnings(as.integer(get(year_col)))]
  org_dt[, panethnic_group := norm_group(panethnic_group)]
  org_dt[, geo_id := normalize_geo_id(get(county_col))]

  if ("county_name" %in% names(org_dt)) {
    org_dt[, county_name := trimws(as.character(county_name))]
  } else if ("irs_county" %in% names(org_dt)) {
    org_dt[, county_name := trimws(as.character(irs_county))]
  } else {
    org_dt[, county_name := NA_character_]
  }

  for (cc in c("membership", "events", "volunteer", "take_action")) {
    if (!cc %in% names(org_dt)) org_dt[, (cc) := NA_real_]
  }

  org_dt[, membership_bin := to_binary(membership)]
  org_dt[, events_bin := to_binary(events)]
  org_dt[, volunteer_bin := to_binary(volunteer)]
  org_dt[, take_action_bin := to_binary(take_action)]

  org_dt[, civic_any_four := as.integer(
    pmax(
      fifelse(is.na(membership_bin), 0, membership_bin),
      fifelse(is.na(events_bin), 0, events_bin),
      fifelse(is.na(volunteer_bin), 0, volunteer_bin),
      fifelse(is.na(take_action_bin), 0, take_action_bin)
    ) > 0
  )]

  # Restrict to organizations that provide civic opportunity in at least one of four IRS activity dimensions.
  org_dt <- org_dt[
    !is.na(year) &
      year >= start_year &
      panethnic_group %in% c("asian", "latino") &
      !is.na(geo_id) & geo_id != "00000" &
      civic_any_four == 1
  ]

  if (nrow(org_dt) == 0L) return(data.table())

  org_dt[, county_name_nonempty := fifelse(!is.na(county_name) & county_name != "", county_name, NA_character_)]

  annual <- org_dt[, .(
    new_orgs = uniqueN(ein),
    county_name = county_name_nonempty[which.max(!is.na(county_name_nonempty))]
  ), by = .(geo_id, year, panethnic_group)]

  setorder(annual, geo_id, panethnic_group, year)
  annual[, cumulative_orgs := cumsum(new_orgs), by = .(geo_id, panethnic_group)]
  annual
}

build_pop_county_series <- function(pop_dt, start_year) {
  pop_dt[, year := suppressWarnings(as.integer(year))]
  pop_dt[, geo_id := normalize_geo_id(geo_id)]

  county <- pop_dt[
    tolower(geo_level) == "county" &
      !is.na(year) &
      year >= start_year &
      !is.na(geo_id) & geo_id != "00000"
  ]

  if (nrow(county) == 0L) return(data.table())

  county[, county_name := trimws(as.character(geo_name))]
  county[, population_total := suppressWarnings(as.numeric(population_total))]

  out <- rbindlist(list(
    county[, .(geo_id, county_name, year, panethnic_group = "asian", population = suppressWarnings(as.numeric(population_asian)), population_total)],
    county[, .(geo_id, county_name, year, panethnic_group = "latino", population = suppressWarnings(as.numeric(population_latino)), population_total)]
  ), fill = TRUE)

  out[!is.na(population) & population > 0]
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

    org_per_100k_0 <- ifelse(!is.na(p0) && p0 > 0, o0 / p0 * 100000, NA_real_)
    org_per_100k_1 <- ifelse(!is.na(p1) && p1 > 0, o1 / p1 * 100000, NA_real_)

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
  }, by = .(geo_id, county_name, panethnic_group)]

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
  if (nrow(panel_dt) == 0L) return(data.table())

  nat <- panel_dt[, .(
    population = sum(population, na.rm = TRUE),
    cumulative_orgs = sum(cumulative_orgs, na.rm = TRUE)
  ), by = .(panethnic_group, year)]

  nat[, geo_id := "national"]
  nat[, county_name := "national"]

  score_gap(nat)
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

  org_county <- build_org_county_series(org_dt, cfg$start_year)
  pop_county <- build_pop_county_series(pop_dt, cfg$start_year)

  if (nrow(org_county) == 0L || nrow(pop_county) == 0L) {
    fwrite(data.table(), file.path(cfg$out_dir, "county_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_county_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "region_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "urbanicity_gap_scores.csv"))

    # Backward-compatible filenames used by later pipeline steps.
    fwrite(data.table(), file.path(cfg$out_dir, "place_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_gap_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_places_from_gaps.csv"))

    message("Insufficient county-level input: org_county or pop_county is empty. Ensure org_enriched has county FIPS and population has county rows.")
    return(invisible(NULL))
  }

  panel <- merge(
    org_county,
    pop_county,
    by = c("geo_id", "year", "panethnic_group"),
    all = FALSE,
    suffixes = c("_org", "_pop")
  )

  if (nrow(panel) == 0L) {
    fwrite(data.table(), file.path(cfg$out_dir, "county_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_county_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "region_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "urbanicity_gap_scores.csv"))

    # Backward-compatible filenames used by later pipeline steps.
    fwrite(data.table(), file.path(cfg$out_dir, "place_gap_scores.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_gap_cases.csv"))
    fwrite(data.table(), file.path(cfg$out_dir, "selected_places_from_gaps.csv"))

    message("No overlap between county org series and county population series. Wrote empty outputs.")
    return(invisible(NULL))
  }

  panel[, county_name := fifelse(!is.na(county_name_pop) & county_name_pop != "", county_name_pop, county_name_org)]

  scores <- score_gap(panel)

  county_pop <- pop_county[, .(population_total_latest = population_total[which.max(year)]), by = geo_id]
  county_pop[, urbanicity := classify_urbanicity(population_total_latest, cfg$urban_cutoff, cfg$suburban_cutoff)]

  scores <- merge(scores, county_pop, by = "geo_id", all.x = TRUE)
  selected <- select_cases(scores, top_n = cfg$top_n)
  region_scores <- build_region_scores(panel)

  urbanicity_scores <- scores[, .(
    county_n = uniqueN(geo_id),
    median_gap_growth_pct = median(gap_growth_pct, na.rm = TRUE),
    mean_gap_growth_pct = mean(gap_growth_pct, na.rm = TRUE),
    median_org_per_100k_change = median(org_per_100k_change, na.rm = TRUE)
  ), by = .(urbanicity, panethnic_group)]

  selected_places <- unique(selected[, .(
    city = county_name,
    state_abbr = NA_character_,
    geo_id,
    region_focus = "county",
    urbanicity,
    panethnic_group,
    case_type
  )])

  fwrite(scores, file.path(cfg$out_dir, "county_gap_scores.csv"))
  fwrite(selected, file.path(cfg$out_dir, "selected_county_cases.csv"))
  fwrite(region_scores, file.path(cfg$out_dir, "region_gap_scores.csv"))
  fwrite(urbanicity_scores, file.path(cfg$out_dir, "urbanicity_gap_scores.csv"))

  # Backward-compatible filenames.
  fwrite(scores, file.path(cfg$out_dir, "place_gap_scores.csv"))
  fwrite(selected, file.path(cfg$out_dir, "selected_gap_cases.csv"))
  fwrite(selected_places, file.path(cfg$out_dir, "selected_places_from_gaps.csv"))

  message(sprintf("Done. Scored counties: %s | Selected cases: %s",
                  format(nrow(scores), big.mark = ","),
                  format(nrow(selected), big.mark = ",")))
}

main()
