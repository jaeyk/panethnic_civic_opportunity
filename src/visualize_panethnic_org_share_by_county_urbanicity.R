#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(jsonlite)
  library(ggplot2)
  library(scales)
})

cfg <- list(
  org_input = "processed_data/org_enriched/org_civic_enriched.csv",
  census_2020_json = "processed_data/population/census_county_2020_pl_total_asian_latino.json",
  out_table = "outputs/analysis/panethnic_org_share_by_county_urbanicity_2020.csv",
  out_fig = "outputs/figures/panethnic_org_share_by_county_urbanicity_2020.png",
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
need <- c("ein", "panethnic_group", "candidate_type", "reclass_applied", "irs_county_fips")
miss <- need[!need %in% names(org)]
if (length(miss) > 0L) stop(sprintf("Missing required org columns: %s", paste(miss, collapse = ", ")))

org[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
org <- org[panethnic_group %in% c("asian", "latino")]
org[, county_fips := sprintf("%05d", suppressWarnings(as.integer(gsub("[^0-9]", "", as.character(irs_county_fips)))))]
org <- org[!is.na(county_fips) & county_fips != "00000"]

org[, is_panethnic := 0L]
org[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", is_panethnic := 1L]
org[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, is_panethnic := 1L]
org <- org[is_panethnic == 1L]
org <- unique(org[, .(ein, panethnic_group, county_fips)])

dt <- merge(org, pop[, .(county_fips, urbanicity)], by = "county_fips", all.x = FALSE)
dt <- dt[urbanicity %in% c("urban", "suburban", "rural")]

summary_dt <- dt[, .(panethnic_org_n = uniqueN(ein)), by = .(panethnic_group, urbanicity)]
summary_dt[, share_pct := 100 * panethnic_org_n / sum(panethnic_org_n), by = panethnic_group]
summary_dt[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]
summary_dt[, urbanicity := factor(urbanicity, levels = c("urban", "suburban", "rural"), labels = c("Urban", "Suburban", "Rural"))]
summary_dt[, label := sprintf("%.1f%%", share_pct)]
summary_dt[, n_label := sprintf("n=%s", format(panethnic_org_n, big.mark = ","))]
setorder(summary_dt, group_label, urbanicity)

fwrite(summary_dt[, .(panethnic_group, group_label, urbanicity, panethnic_org_n, share_pct)], cfg$out_table)

p <- ggplot(summary_dt, aes(x = urbanicity, y = share_pct, fill = urbanicity)) +
  geom_col(width = 0.72, color = "grey30", linewidth = 0.2) +
  geom_text(aes(label = label), vjust = -0.25, size = 4.2) +
  geom_text(aes(label = n_label), vjust = 1.7, size = 3.3, color = "white") +
  facet_wrap(~group_label, nrow = 1) +
  scale_fill_manual(values = c("Urban" = "#4D4D4D", "Suburban" = "#7F7F7F", "Rural" = "#BDBDBD"), guide = "none") +
  scale_y_continuous(labels = label_percent(scale = 1), limits = c(0, 100)) +
  labs(
    title = "Where Panethnic Organizations Are Located by County Type",
    subtitle = "Share of panethnic organizations within each group (Asian, Latino), by county urbanicity",
    x = NULL,
    y = "Panethnic organization share (%)"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold", size = 13),
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12)
  )

ggsave(cfg$out_fig, p, width = 11, height = 6, dpi = 220)
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
