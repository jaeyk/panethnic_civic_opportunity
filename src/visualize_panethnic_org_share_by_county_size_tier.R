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
  out_table = "outputs/analysis/panethnic_org_share_by_county_size_tier_2020.csv",
  out_fig = "outputs/figures/panethnic_org_share_by_county_size_tier_2020.png"
)

for (p in c(cfg$org_input, cfg$census_2020_json)) {
  if (!file.exists(p)) stop(sprintf("Missing required file: %s", p))
}
dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

county_size_tier <- function(pop_total) {
  out <- rep("Unknown", length(pop_total))
  out[!is.na(pop_total) & pop_total >= 1000000] <- "Mega urban (1M+)"
  out[!is.na(pop_total) & pop_total >= 250000 & pop_total < 1000000] <- "Large urban (250k-999k)"
  out[!is.na(pop_total) & pop_total >= 100000 & pop_total < 250000] <- "Mid urban (100k-249k)"
  out[!is.na(pop_total) & pop_total >= 50000 & pop_total < 100000] <- "Small urban (50k-99k)"
  out[!is.na(pop_total) & pop_total >= 10000 & pop_total < 50000] <- "Suburban (10k-49k)"
  out[!is.na(pop_total) & pop_total < 10000] <- "Rural (<10k)"
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
pop[, size_tier := county_size_tier(population_total_2020)]

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
org <- unique(org[is_panethnic == 1L, .(ein, panethnic_group, county_fips)])

dt <- merge(org, pop[, .(county_fips, size_tier)], by = "county_fips", all.x = FALSE)
dt <- dt[size_tier != "Unknown"]

summary_dt <- dt[, .(panethnic_org_n = uniqueN(ein)), by = .(panethnic_group, size_tier)]
summary_dt[, share_pct := 100 * panethnic_org_n / sum(panethnic_org_n), by = panethnic_group]
summary_dt[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]
tier_levels <- c(
  "Mega urban (1M+)",
  "Large urban (250k-999k)",
  "Mid urban (100k-249k)",
  "Small urban (50k-99k)",
  "Suburban (10k-49k)",
  "Rural (<10k)"
)
summary_dt[, size_tier := factor(size_tier, levels = tier_levels)]
summary_dt[, label := sprintf("%.1f%%", share_pct)]
setorder(summary_dt, group_label, size_tier)

fwrite(summary_dt[, .(panethnic_group, group_label, size_tier, panethnic_org_n, share_pct)], cfg$out_table)

p <- ggplot(summary_dt, aes(x = size_tier, y = share_pct, fill = size_tier)) +
  geom_col(width = 0.75, color = "grey30", linewidth = 0.2) +
  geom_text(aes(label = label), vjust = -0.3, size = 3.6) +
  facet_wrap(~group_label, ncol = 1) +
  scale_y_continuous(labels = label_percent(scale = 1), limits = c(0, 100)) +
  scale_fill_grey(start = 0.25, end = 0.85, guide = "none") +
  labs(
    title = "Panethnic Organization Share by County Size Tier",
    subtitle = "Urban counties split into mega, large, mid, and small tiers (2020 county population)",
    x = NULL,
    y = "Panethnic organization share (%)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.minor = element_blank(),
    axis.text.x = element_text(angle = 20, hjust = 1),
    strip.text = element_text(face = "bold"),
    plot.title = element_text(face = "bold")
  )

ggsave(cfg$out_fig, p, width = 10.5, height = 8.5, dpi = 220)
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
