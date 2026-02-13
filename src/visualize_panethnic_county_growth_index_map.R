#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(maps)
  library(jsonlite)
})

cfg <- list(
  input = "processed_data/org_enriched/org_civic_enriched.csv",
  census_json = "processed_data/population/census_county_2020_b03002.json",
  out_table = "outputs/analysis/panethnic_county_growth_index.csv",
  out_fig = "outputs/figures/panethnic_county_growth_index_map.png",
  baseline_year = 1980L,
  population_year = 2020L,
  min_expected_panethnic = 1
)

dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("ein", "fnd_yr", "panethnic_group", "candidate_type", "irs_county_fips")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))
if (!file.exists(cfg$census_json)) stop(sprintf("Missing Census JSON: %s", cfg$census_json))

dt[, fnd_yr := suppressWarnings(as.integer(fnd_yr))]
dt <- dt[!is.na(fnd_yr)]
dt[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt <- dt[panethnic_group %in% c("asian", "latino")]
dt[, is_panethnic := 0L]
if ("candidate_type" %in% names(dt)) {
  dt[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", is_panethnic := 1L]
}
if ("reclass_applied" %in% names(dt)) {
  dt[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, is_panethnic := 1L]
}

dt[, county_fips := suppressWarnings(as.integer(gsub("[^0-9]", "", as.character(irs_county_fips))))]
dt <- dt[!is.na(county_fips)]
dt[, county_fips := sprintf("%05d", county_fips)]
dt <- dt[county_fips != "00000"]

recent_year <- max(dt$fnd_yr, na.rm = TRUE)
base <- dt[fnd_yr <= cfg$baseline_year]
recent <- dt[fnd_yr <= recent_year]

base_sum <- base[, .(base_panethnic_n = sum(is_panethnic, na.rm = TRUE)), by = .(panethnic_group, county_fips)]
recent_sum <- recent[, .(recent_panethnic_n = sum(is_panethnic, na.rm = TRUE)), by = .(panethnic_group, county_fips)]

growth <- merge(base_sum, recent_sum, by = c("panethnic_group", "county_fips"), all = TRUE)
for (v in c("base_panethnic_n", "recent_panethnic_n")) growth[is.na(get(v)), (v) := 0L]
growth[, panethnic_total := base_panethnic_n + recent_panethnic_n]

# Growth rate with baseline floor 1 to avoid division-by-zero blowups.
growth[, growth_rate_pct := 100 * (recent_panethnic_n - base_panethnic_n) / pmax(base_panethnic_n, 1L)]

# Load county population from Census API JSON (ACS 2020 B03002).
raw <- fromJSON(cfg$census_json)
hdr <- as.character(raw[1, ])
pop <- as.data.table(raw[-1, , drop = FALSE])
setnames(pop, hdr)
need_pop <- c("state", "county", "B03002_006E", "B03002_012E")
miss_pop <- need_pop[!need_pop %in% names(pop)]
if (length(miss_pop) > 0L) stop(sprintf("Missing population columns: %s", paste(miss_pop, collapse = ", ")))

pop[, county_fips := sprintf("%02d%03d", as.integer(state), as.integer(county))]
pop[, asian_population_2020 := suppressWarnings(as.integer(B03002_006E))]
pop[, latino_population_2020 := suppressWarnings(as.integer(B03002_012E))]

pop_long <- rbindlist(list(
  pop[, .(county_fips, panethnic_group = "asian", recent_population = asian_population_2020)],
  pop[, .(county_fips, panethnic_group = "latino", recent_population = latino_population_2020)]
), use.names = TRUE)
pop_long <- pop_long[county_fips != "00000"]

# Keep all counties with relevant population even if org count is zero.
growth <- merge(pop_long, growth, by = c("panethnic_group", "county_fips"), all.x = TRUE)
for (v in c("base_panethnic_n", "recent_panethnic_n", "panethnic_total", "growth_rate_pct")) {
  growth[is.na(get(v)), (v) := 0]
}

# Normalize by recent population size.
growth[, normalized_growth_per_100k := fifelse(
  recent_population > 0,
  (growth_rate_pct / recent_population) * 100000,
  NA_real_
)]

# National expected-count benchmark by group (orgs per 100k relevant population).
group_rates <- growth[recent_population > 0, .(
  national_recent_panethnic_n = sum(recent_panethnic_n, na.rm = TRUE),
  national_recent_population = sum(recent_population, na.rm = TRUE)
), by = panethnic_group]
group_rates[, national_panethnic_rate_per_100k := fifelse(
  national_recent_population > 0,
  100000 * national_recent_panethnic_n / national_recent_population,
  NA_real_
)]
growth <- merge(
  growth,
  group_rates[, .(panethnic_group, national_panethnic_rate_per_100k)],
  by = "panethnic_group",
  all.x = TRUE
)
growth[, expected_panethnic_n := fifelse(
  recent_population > 0 & !is.na(national_panethnic_rate_per_100k),
  recent_population * national_panethnic_rate_per_100k / 100000,
  NA_real_
)]

# 5-value index among counties that have both relevant population and at least one recent panethnic org.
growth[, growth_rank := as.numeric(NA)]
growth[, growth_index_5 := as.integer(NA)]
growth[
  recent_population > 0 & recent_panethnic_n > 0,
  growth_rank := frank(normalized_growth_per_100k, ties.method = "average", na.last = "keep"),
  by = panethnic_group
]
growth[
  recent_population > 0 & recent_panethnic_n > 0,
  growth_index_5 := as.integer(ceiling(5 * growth_rank / max(growth_rank, na.rm = TRUE))),
  by = panethnic_group
]
growth[, growth_rank := NULL]
growth[growth_index_5 < 1L, growth_index_5 := 1L]
growth[growth_index_5 > 5L, growth_index_5 := 5L]

red_label <- "No panethnic orgs (population suggests presence)"

growth[, fill_key := fifelse(
  recent_population > 0 & recent_panethnic_n == 0 & expected_panethnic_n >= cfg$min_expected_panethnic,
  red_label,
  fifelse(!is.na(growth_index_5), as.character(growth_index_5), NA_character_)
)]
growth[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]

fwrite(growth[order(group_label, county_fips)], cfg$out_table)

# County polygons from maps package.
county_poly <- as.data.table(ggplot2::map_data("county"))
data("county.fips", package = "maps", envir = environment())
cf <- as.data.table(get("county.fips", envir = environment()))
cf[, county_fips := sprintf("%05d", as.integer(fips))]
cf[, polyname := as.character(polyname)]
cf <- unique(cf[, .(polyname, county_fips)])

county_poly[, polyname := sprintf("%s,%s", region, subregion)]
poly <- merge(county_poly, cf, by = "polyname", all.x = TRUE, allow.cartesian = TRUE)

# Duplicate polygons per panel so both groups render complete US county map.
all_groups <- data.table(group_label = c("Asian", "Latino"))
poly[, tmp_join_key := 1L]
all_groups[, tmp_join_key := 1L]
panel_map <- merge(poly, all_groups, by = "tmp_join_key", allow.cartesian = TRUE)
panel_map[, tmp_join_key := NULL]
poly[, tmp_join_key := NULL]
all_groups[, tmp_join_key := NULL]
panel_map <- merge(
  panel_map,
  growth[, .(county_fips, group_label, fill_key)],
  by = c("county_fips", "group_label"),
  all.x = TRUE,
  allow.cartesian = TRUE
)

state_poly <- as.data.table(ggplot2::map_data("state"))

p <- ggplot(panel_map, aes(x = long, y = lat, group = group, fill = fill_key)) +
  geom_polygon(color = "#8A8A8A", linewidth = 0.14) +
  geom_path(
    data = state_poly,
    aes(x = long, y = lat, group = group),
    inherit.aes = FALSE,
    color = "#1F1F1F",
    linewidth = 0.22
  ) +
  coord_quickmap() +
  facet_wrap(~group_label, ncol = 1) +
  scale_fill_manual(
    values = setNames(
      c("#edf8e9", "#bae4b3", "#74c476", "#31a354", "#006d2c", "#E15759"),
      c("1", "2", "3", "4", "5", red_label)
    ),
    breaks = c("1", "2", "3", "4", "5", red_label),
    drop = TRUE,
    na.value = "#D9D9D9",
    name = "County class"
  ) +
  labs(
    title = "County Growth Index of Panethnic Organizations",
    x = NULL,
    y = NULL
  ) +
  theme_void(base_size = 11) +
  theme(
    legend.position = "bottom",
    strip.text = element_text(face = "bold", size = 11),
    plot.title = element_text(face = "bold", margin = margin(b = 14)),
    plot.title.position = "plot",
    panel.spacing = grid::unit(0.4, "lines"),
    plot.background = element_rect(fill = "white", color = NA),
    panel.background = element_rect(fill = "white", color = NA),
    legend.background = element_rect(fill = "white", color = NA),
    legend.box.background = element_rect(fill = "white", color = NA),
    plot.margin = margin(t = 12, r = 10, b = 10, l = 10)
  )

ggsave(cfg$out_fig, p, width = 12, height = 9, dpi = 220)

cat(sprintf("Rows analyzed: %s\n", format(nrow(dt), big.mark = ",")))
cat(sprintf("County-group rows with relevant population: %s\n", format(growth[recent_population > 0, .N], big.mark = ",")))
cat(sprintf("County-group rows flagged as expected>=%.1f and observed=0: %s\n", cfg$min_expected_panethnic, format(growth[fill_key == red_label, .N], big.mark = ",")))
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
