#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(jsonlite)
  library(ggplot2)
  library(zoo)
  library(scales)
})

cfg <- list(
  org_input = "processed_data/org_enriched/org_civic_enriched.csv",
  census_2020_json = "processed_data/population/census_county_2020_pl_total_asian_latino.json",
  out_table = "outputs/analysis/panethnic_flow_by_urbanicity_year.csv",
  out_fig = "outputs/figures/panethnic_flow_by_urbanicity.png",
  min_year = 1950L,
  max_year = 2020L,
  roll_k = 5L,
  urban_cutoff = 50000L,
  suburban_cutoff = 10000L
)

for (p in c(cfg$org_input, cfg$census_2020_json)) {
  if (!file.exists(p)) stop(sprintf("Missing required file: %s", p))
}
dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

classify_urbanicity <- function(pop_total, urban_cutoff = 50000L, suburban_cutoff = 10000L) {
  out <- rep("unknown", length(pop_total))
  out[!is.na(pop_total) & pop_total >= urban_cutoff] <- "urban"
  out[!is.na(pop_total) & pop_total < urban_cutoff & pop_total >= suburban_cutoff] <- "suburban"
  out[!is.na(pop_total) & pop_total < suburban_cutoff] <- "rural"
  out
}

raw <- fromJSON(cfg$census_2020_json)
hdr <- as.character(raw[1, ])
pop <- as.data.table(raw[-1, , drop = FALSE])
setnames(pop, hdr)
if (!all(c("state", "county", "P1_001N") %in% names(pop))) {
  stop("Census file must include state, county, and P1_001N.")
}
pop[, county_fips := sprintf("%02d%03d", as.integer(state), as.integer(county))]
pop <- pop[county_fips != "00000", .(
  county_fips,
  population_total_2020 = suppressWarnings(as.integer(P1_001N))
)]
pop[, urbanicity := classify_urbanicity(population_total_2020, cfg$urban_cutoff, cfg$suburban_cutoff)]

org <- fread(cfg$org_input, encoding = "UTF-8")
need <- c("ein", "fnd_yr", "panethnic_group", "candidate_type", "reclass_applied", "irs_county_fips")
miss <- need[!need %in% names(org)]
if (length(miss) > 0L) stop(sprintf("Missing required org columns: %s", paste(miss, collapse = ", ")))

org[, fnd_yr := suppressWarnings(as.integer(fnd_yr))]
org <- org[!is.na(fnd_yr) & fnd_yr >= cfg$min_year & fnd_yr <= cfg$max_year]
org[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
org <- org[panethnic_group %in% c("asian", "latino")]
org[, county_fips := sprintf("%05d", suppressWarnings(as.integer(gsub("[^0-9]", "", as.character(irs_county_fips)))))]
org <- org[!is.na(county_fips) & county_fips != "00000"]

org[, is_panethnic := 0L]
org[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", is_panethnic := 1L]
org[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, is_panethnic := 1L]
org <- unique(org[is_panethnic == 1L, .(ein, fnd_yr, panethnic_group, county_fips)])

dt <- merge(org, pop[, .(county_fips, urbanicity)], by = "county_fips", all.x = FALSE)
dt <- dt[urbanicity %in% c("urban", "suburban", "rural")]

flow <- dt[, .(new_org_n = uniqueN(ein)), by = .(panethnic_group, urbanicity, fnd_yr)]
all_years <- data.table(fnd_yr = cfg$min_year:cfg$max_year)
all_groups <- CJ(panethnic_group = c("asian", "latino"), urbanicity = c("urban", "suburban", "rural"), unique = TRUE)
grid <- all_groups[, .(fnd_yr = cfg$min_year:cfg$max_year), by = .(panethnic_group, urbanicity)]
flow <- merge(grid, flow, by = c("panethnic_group", "urbanicity", "fnd_yr"), all.x = TRUE)
flow[is.na(new_org_n), new_org_n := 0L]

flow[, flow_roll5 := zoo::rollmean(new_org_n, k = cfg$roll_k, fill = NA, align = "center"), by = .(panethnic_group, urbanicity)]
flow[, total_roll5 := sum(flow_roll5, na.rm = TRUE), by = .(panethnic_group, fnd_yr)]
flow[, flow_share_roll5 := fifelse(total_roll5 > 0, 100 * flow_roll5 / total_roll5, NA_real_)]
flow[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]
flow[, urbanicity := factor(urbanicity, levels = c("urban", "suburban", "rural"), labels = c("Urban", "Suburban", "Rural"))]

fwrite(flow[order(group_label, urbanicity, fnd_yr)], cfg$out_table)

p <- ggplot(flow, aes(x = fnd_yr, y = flow_roll5, color = urbanicity, linetype = urbanicity)) +
  geom_line(linewidth = 1.0, na.rm = TRUE) +
  facet_wrap(~group_label, ncol = 1, scales = "free_y") +
  scale_color_manual(values = c("Urban" = "#2B2B2B", "Suburban" = "#777777", "Rural" = "#B0B0B0")) +
  scale_linetype_manual(values = c("Urban" = "solid", "Suburban" = "longdash", "Rural" = "dotted")) +
  scale_x_continuous(breaks = pretty_breaks(n = 10)) +
  labs(
    title = "Where Panethnic Growth Is Happening: Urban vs Suburban vs Rural",
    subtitle = "Flow metric: 5-year centered average of new panethnic incorporations per year",
    x = "IRS incorporation year",
    y = "New panethnic organizations (5-year avg)",
    color = "County type",
    linetype = "County type"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold", size = 12),
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12),
    legend.position = "bottom"
  )

ggsave(cfg$out_fig, p, width = 11, height = 8.2, dpi = 220)
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
