#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(jsonlite)
})

cfg <- list(
  census_2020_json = "processed_data/population/census_county_2020_pl_total_asian_latino.json",
  asian_no_pan = "outputs/analysis/county_asian_population_no_panethnic_2020.csv",
  latino_no_pan = "outputs/analysis/county_latino_population_no_panethnic_2020.csv",
  out_county = "outputs/analysis/county_no_panethnic_urbanicity_2020.csv",
  out_summary = "outputs/analysis/county_no_panethnic_urbanicity_summary_2020.csv",
  urban_cutoff = 50000L,
  suburban_cutoff = 10000L
)

for (p in c(cfg$census_2020_json, cfg$asian_no_pan, cfg$latino_no_pan)) {
  if (!file.exists(p)) stop(sprintf("Missing required file: %s", p))
}
dir.create(dirname(cfg$out_county), recursive = TRUE, showWarnings = FALSE)

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
total_var <- c("B03002_001E", "P1_001N", "P001001")
total_var <- total_var[total_var %in% names(pop)]
if (length(total_var) == 0L) stop("Missing total population variable in Census file.")
total_var <- total_var[1]
need <- c("state", "county", "NAME")
miss <- need[!need %in% names(pop)]
if (length(miss) > 0L) stop(sprintf("Missing Census columns: %s", paste(miss, collapse = ", ")))

pop[, county_fips := sprintf("%02d%03d", as.integer(state), as.integer(county))]
pop <- pop[county_fips != "00000", .(
  county_fips,
  county_name_census = as.character(NAME),
  population_total_2020 = suppressWarnings(as.integer(get(total_var)))
)]

asian <- fread(cfg$asian_no_pan, encoding = "UTF-8")[
  , .(panethnic_group = "asian", county_fips, county_name)
]
latino <- fread(cfg$latino_no_pan, encoding = "UTF-8")[
  , .(panethnic_group = "latino", county_fips, county_name)
]
dt <- rbindlist(list(asian, latino), use.names = TRUE, fill = TRUE)
dt[, county_fips := sprintf("%05d", suppressWarnings(as.integer(gsub("[^0-9]", "", as.character(county_fips)))))]
dt <- dt[!is.na(county_fips) & county_fips != "00000"]

dt <- merge(dt, pop, by = "county_fips", all.x = TRUE)
dt[, county_name := fifelse(!is.na(county_name) & county_name != "", county_name, county_name_census)]
dt[, urbanicity := classify_urbanicity(population_total_2020, cfg$urban_cutoff, cfg$suburban_cutoff)]
dt[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]

setcolorder(dt, c("panethnic_group", "group_label", "county_fips", "county_name", "population_total_2020", "urbanicity"))
setorder(dt, group_label, urbanicity, -population_total_2020, county_name)
fwrite(dt, cfg$out_county)

summary_dt <- dt[, .(
  county_n = .N,
  share_counties_pct = as.numeric(round(100 * .N / dt[panethnic_group == .BY$panethnic_group, .N], 1)),
  median_total_pop_2020 = as.numeric(median(population_total_2020, na.rm = TRUE)),
  mean_total_pop_2020 = as.numeric(round(mean(population_total_2020, na.rm = TRUE), 0))
), by = .(panethnic_group, group_label, urbanicity)]
summary_dt[, urbanicity_order := fifelse(
  urbanicity == "urban", 1L,
  fifelse(urbanicity == "suburban", 2L, fifelse(urbanicity == "rural", 3L, 4L))
)]
setorder(summary_dt, group_label, urbanicity_order)
summary_dt[, urbanicity_order := NULL]
fwrite(summary_dt, cfg$out_summary)

cat(sprintf("Wrote county-level file: %s\n", cfg$out_county))
cat(sprintf("Wrote summary file: %s\n", cfg$out_summary))
print(summary_dt)
