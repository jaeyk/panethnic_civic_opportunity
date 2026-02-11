#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
})

parse_args <- function(args) {
  cfg <- list(
    org_enriched = "outputs/org_enriched/org_civic_enriched.csv",
    population = "outputs/population/population_series.csv",
    selected_places = "misc/selected_places.csv",
    gap_scores = "outputs/gap_analysis/place_gap_scores.csv",
    out_dir = "outputs/figures"
  )

  if (length(args) == 0) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% names(cfg)) {
      cfg[[key]] <- val
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

slugify <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x
}

get_year_col <- function(dt) {
  opts <- c("F.year", "fnd_yr", "founding_year")
  hit <- opts[opts %in% names(dt)]
  if (length(hit) == 0) stop("Expected one of F.year, fnd_yr, founding_year in org_enriched file")
  hit[[1]]
}

build_org_series <- function(org_dt) {
  year_col <- get_year_col(org_dt)
  org_dt[, year := suppressWarnings(as.integer(get(year_col)))]
  org_dt[, panethnic_group := norm_group(panethnic_group)]

  org <- org_dt[!is.na(year) & panethnic_group %in% c("asian", "latino")]
  if (!"ein" %in% names(org)) stop("org_enriched must contain ein")

  out <- org[, .(new_orgs = uniqueN(ein)), by = .(panethnic_group, year)][order(panethnic_group, year)]
  out[, cumulative_orgs := cumsum(new_orgs), by = panethnic_group]
  out
}

build_national_panel <- function(org_series, pop_dt) {
  nat <- pop_dt[tolower(geo_level) == "national"]
  if (nrow(nat) == 0L) return(data.table())

  nat_long <- rbindlist(list(
    nat[, .(year, panethnic_group = "asian", population = suppressWarnings(as.numeric(population_asian)))],
    nat[, .(year, panethnic_group = "latino", population = suppressWarnings(as.numeric(population_latino)))]
  ), fill = TRUE)

  nat_long <- nat_long[!is.na(year) & !is.na(population) & population > 0]
  merged <- merge(org_series, nat_long, by = c("year", "panethnic_group"), all = FALSE)
  merged[, orgs_per_100k := cumulative_orgs / population * 100000]
  merged
}

save_national_plot <- function(panel, out_dir) {
  if (nrow(panel) == 0L) return(invisible(NULL))

  p1 <- ggplot(panel, aes(x = year, y = population, color = panethnic_group)) +
    geom_line(linewidth = 1) +
    scale_color_manual(values = c(asian = "#1b9e77", latino = "#d95f02")) +
    labs(title = "National Population", x = "Year", y = "Population", color = NULL) +
    theme_minimal(base_size = 12)

  p2 <- ggplot(panel, aes(x = year, y = orgs_per_100k, color = panethnic_group)) +
    geom_line(linewidth = 1) +
    scale_color_manual(values = c(asian = "#1b9e77", latino = "#d95f02")) +
    labs(title = "Organizations per 100,000", x = "Year", y = "Org density", color = NULL) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "national_population_trend.png"), p1, width = 9, height = 5, dpi = 200)
  ggsave(file.path(out_dir, "national_orgs_per_100k_trend.png"), p2, width = 9, height = 5, dpi = 200)
}

save_place_plots <- function(org_dt, pop_dt, selected_places_path, out_dir) {
  if (!file.exists(selected_places_path)) return(invisible(NULL))

  sel <- fread(selected_places_path, encoding = "UTF-8")
  req <- c("city", "state_abbr", "geo_id")
  if (!all(req %in% names(sel))) return(invisible(NULL))

  year_col <- get_year_col(org_dt)
  org_dt[, year := suppressWarnings(as.integer(get(year_col)))]
  org_dt[, panethnic_group := norm_group(panethnic_group)]

  if (!all(c("irs_city", "irs_state", "ein") %in% names(org_dt))) return(invisible(NULL))

  sel[, city_norm := toupper(trimws(city))]
  sel[, state_norm := toupper(trimws(state_abbr))]

  # Keep all regions; no regional filter is applied.

  org <- org_dt[!is.na(year) & panethnic_group %in% c("asian", "latino")]
  org[, city_norm := toupper(trimws(irs_city))]
  org[, state_norm := toupper(trimws(irs_state))]

  org_sel <- merge(org, sel[, .(geo_id, city, state_abbr, city_norm, state_norm)], by = c("city_norm", "state_norm"), all = FALSE)
  if (nrow(org_sel) == 0L) return(invisible(NULL))

  org_annual <- org_sel[, .(new_orgs = uniqueN(ein)), by = .(geo_id, city, state_abbr, panethnic_group, year)]
  setorder(org_annual, geo_id, panethnic_group, year)
  org_annual[, cumulative_orgs := cumsum(new_orgs), by = .(geo_id, panethnic_group)]

  places <- pop_dt[tolower(geo_level) == "place"]
  if (nrow(places) == 0L) return(invisible(NULL))

  places_long <- rbindlist(list(
    places[, .(geo_id, year, panethnic_group = "asian", population = suppressWarnings(as.numeric(population_asian)))],
    places[, .(geo_id, year, panethnic_group = "latino", population = suppressWarnings(as.numeric(population_latino)))]
  ), fill = TRUE)

  places_long[, geo_id := as.character(geo_id)]
  org_annual[, geo_id := as.character(geo_id)]
  places_long <- places_long[!is.na(population) & population > 0]

  panel <- merge(org_annual, places_long, by = c("geo_id", "year", "panethnic_group"), all = FALSE)
  if (nrow(panel) == 0L) return(invisible(NULL))

  panel[, orgs_per_100k := cumulative_orgs / population * 100000]

  for (gid in unique(panel$geo_id)) {
    g <- panel[geo_id == gid]
    city <- g$city[[1]]
    state <- g$state_abbr[[1]]

    p <- ggplot(g, aes(x = year, y = orgs_per_100k, color = panethnic_group)) +
      geom_line(linewidth = 1) +
      scale_color_manual(values = c(asian = "#1b9e77", latino = "#d95f02")) +
      labs(
        title = sprintf("%s, %s: Organization Representation Trend", city, state),
        x = "Year",
        y = "Organizations per 100,000",
        color = NULL
      ) +
      theme_minimal(base_size = 12)

    out_name <- sprintf("city_gap_%s_%s.png", slugify(city), tolower(state))
    ggsave(file.path(out_dir, out_name), p, width = 9, height = 5, dpi = 200)
  }
}

save_urbanicity_plot <- function(gap_scores_path, out_dir) {
  if (!file.exists(gap_scores_path)) return(invisible(NULL))
  dt <- fread(gap_scores_path, encoding = "UTF-8")
  req <- c("urbanicity", "panethnic_group", "gap_growth_pct")
  if (!all(req %in% names(dt))) return(invisible(NULL))

  dt[, urbanicity := tolower(trimws(as.character(urbanicity)))]
  dt[, panethnic_group := norm_group(panethnic_group)]
  dt[, gap_growth_pct := suppressWarnings(as.numeric(gap_growth_pct))]
  dt <- dt[urbanicity %in% c("urban", "suburban", "rural") & panethnic_group %in% c("asian", "latino") & is.finite(gap_growth_pct)]
  if (nrow(dt) == 0L) return(invisible(NULL))

  p <- ggplot(dt, aes(x = urbanicity, y = gap_growth_pct, fill = panethnic_group)) +
    geom_boxplot(outlier.alpha = 0.25, position = position_dodge(width = 0.8)) +
    scale_fill_manual(values = c(asian = "#1b9e77", latino = "#d95f02")) +
    labs(
      title = "Population-Organization Growth Gap by Urbanicity",
      subtitle = "Positive values indicate population growth outpaces organization growth",
      x = "Urbanicity",
      y = "Growth gap (population % minus organization %)",
      fill = NULL
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(out_dir, "urbanicity_gap_comparison.png"), p, width = 10, height = 5.5, dpi = 220)
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  org_dt <- fread(cfg$org_enriched, encoding = "UTF-8")
  pop_dt <- fread(cfg$population, encoding = "UTF-8")
  pop_dt[, year := suppressWarnings(as.integer(year))]

  org_series <- build_org_series(org_dt)
  nat_panel <- build_national_panel(org_series, pop_dt)

  save_national_plot(nat_panel, cfg$out_dir)
  save_place_plots(org_dt, pop_dt, cfg$selected_places, cfg$out_dir)
  save_urbanicity_plot(cfg$gap_scores, cfg$out_dir)

  message(sprintf("Figures written to %s", cfg$out_dir))
}

main()
