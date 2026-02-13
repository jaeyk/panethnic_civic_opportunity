#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(jsonlite)
})

cfg <- list(
  census_json = "processed_data/population/census_county_2020_b03002.json",
  org_input = "processed_data/org_enriched/org_civic_enriched.csv",
  out_all = "outputs/analysis/county_asian_population_panethnic_coverage_2020.csv",
  out_zero = "outputs/analysis/county_asian_population_no_panethnic_2020.csv",
  out_all_latino = "outputs/analysis/county_latino_population_panethnic_coverage_2020.csv",
  out_zero_latino = "outputs/analysis/county_latino_population_no_panethnic_2020.csv"
)

dir.create(dirname(cfg$out_all), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_zero), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_all_latino), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_zero_latino), recursive = TRUE, showWarnings = FALSE)

if (!file.exists(cfg$census_json)) {
  stop(sprintf("Missing Census JSON file: %s", cfg$census_json))
}

raw <- fromJSON(cfg$census_json)
if (length(raw) < 2L) stop("Census JSON is empty or malformed.")

hdr <- as.character(raw[1, ])
dt_pop <- as.data.table(raw[-1, , drop = FALSE])
setnames(dt_pop, hdr)

req_pop <- c("NAME", "B03002_006E", "B03002_012E", "state", "county")
miss_pop <- req_pop[!req_pop %in% names(dt_pop)]
if (length(miss_pop) > 0L) stop(sprintf("Missing Census columns: %s", paste(miss_pop, collapse = ", ")))

dt_pop[, county_fips := sprintf("%02d%03d", as.integer(state), as.integer(county))]
dt_pop[, county_name := as.character(NAME)]
dt_pop[, asian_population_2020 := suppressWarnings(as.integer(B03002_006E))]
dt_pop[, latino_population_2020 := suppressWarnings(as.integer(B03002_012E))]
dt_pop <- dt_pop[county_fips != "00000"]

dt_org <- fread(
  cfg$org_input,
  select = c("ein", "panethnic_group", "candidate_type", "reclass_applied", "irs_county_fips"),
  encoding = "UTF-8"
)

dt_org[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt_org[, county_fips := sprintf("%05d", suppressWarnings(as.integer(gsub("[^0-9]", "", as.character(irs_county_fips)))))]
dt_org <- dt_org[!is.na(county_fips) & county_fips != "00000"]

dt_org[, is_panethnic := 0L]
dt_org[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", is_panethnic := 1L]
if ("reclass_applied" %in% names(dt_org)) {
  dt_org[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, is_panethnic := 1L]
}

# Asian-focused panethnic organizations by county.
asian_pan_county <- dt_org[panethnic_group == "asian" & is_panethnic == 1L, .(
  asian_panethnic_org_n = uniqueN(ein)
), by = county_fips]

# Latino-focused panethnic organizations by county.
latino_pan_county <- dt_org[panethnic_group == "latino" & is_panethnic == 1L, .(
  latino_panethnic_org_n = uniqueN(ein)
), by = county_fips]

out <- merge(
  dt_pop[!is.na(asian_population_2020) & asian_population_2020 > 0, .(county_fips, county_name, asian_population_2020)],
  asian_pan_county,
  by = "county_fips",
  all.x = TRUE
)
out[is.na(asian_panethnic_org_n), asian_panethnic_org_n := 0L]
out[, has_asian_panethnic_org := as.integer(asian_panethnic_org_n > 0)]
setorder(out, -asian_population_2020, county_name)

out_zero <- out[asian_panethnic_org_n == 0L][order(-asian_population_2020, county_name)]

fwrite(out, cfg$out_all)
fwrite(out_zero, cfg$out_zero)

out_latino <- merge(
  dt_pop[!is.na(latino_population_2020) & latino_population_2020 > 0, .(county_fips, county_name, latino_population_2020)],
  latino_pan_county,
  by = "county_fips",
  all.x = TRUE
)
out_latino[is.na(latino_panethnic_org_n), latino_panethnic_org_n := 0L]
out_latino[, has_latino_panethnic_org := as.integer(latino_panethnic_org_n > 0)]
setorder(out_latino, -latino_population_2020, county_name)

out_zero_latino <- out_latino[latino_panethnic_org_n == 0L][order(-latino_population_2020, county_name)]

fwrite(out_latino, cfg$out_all_latino)
fwrite(out_zero_latino, cfg$out_zero_latino)

cat(sprintf("Counties with Asian population > 0: %s\n", format(nrow(out), big.mark = ",")))
cat(sprintf("Counties with zero Asian panethnic orgs: %s\n", format(nrow(out_zero), big.mark = ",")))
cat(sprintf("Wrote: %s\n", cfg$out_all))
cat(sprintf("Wrote: %s\n", cfg$out_zero))
cat(sprintf("Counties with Latino population > 0: %s\n", format(nrow(out_latino), big.mark = ",")))
cat(sprintf("Counties with zero Latino panethnic orgs: %s\n", format(nrow(out_zero_latino), big.mark = ",")))
cat(sprintf("Wrote: %s\n", cfg$out_all_latino))
cat(sprintf("Wrote: %s\n", cfg$out_zero_latino))
