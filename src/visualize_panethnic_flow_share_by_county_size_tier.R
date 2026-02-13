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
  rucc_csv = "raw_data/County_Classifications.csv",
  out_table = "outputs/analysis/panethnic_flow_share_by_county_size_tier_year.csv",
  out_fig = "outputs/figures/panethnic_flow_share_by_county_size_tier.png",
  min_year = 1970L,
  max_year = 2020L,
  roll_k = 5L,
  n_boot = 400L,
  seed = 1234L,
  show_ci = FALSE
)

for (p in c(cfg$org_input, cfg$census_2020_json, cfg$rucc_csv)) {
  if (!file.exists(p)) stop(sprintf("Missing required file: %s", p))
}
dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

raw <- fromJSON(cfg$census_2020_json)
hdr <- as.character(raw[1, ])
pop <- as.data.table(raw[-1, , drop = FALSE])
setnames(pop, hdr)
if (!all(c("state", "county", "P1_001N", "P1_006N", "P2_002N") %in% names(pop))) {
  stop("Census file must include state, county, P1_001N, P1_006N (Asian), and P2_002N (Latino).")
}
pop[, county_fips := sprintf("%02d%03d", as.integer(state), as.integer(county))]
pop <- pop[county_fips != "00000"]
pop[, total_pop_2020 := suppressWarnings(as.integer(P1_001N))]
pop_group <- rbindlist(list(
  pop[, .(county_fips, panethnic_group = "asian", group_pop_2020 = suppressWarnings(as.integer(P1_006N)))],
  pop[, .(county_fips, panethnic_group = "latino", group_pop_2020 = suppressWarnings(as.integer(P2_002N)))]
), use.names = TRUE, fill = TRUE)
pop_group <- pop_group[!is.na(group_pop_2020) & group_pop_2020 >= 0]

rucc <- fread(cfg$rucc_csv, encoding = "UTF-8")
need_rucc <- c("FIPStxt", "RuralUrbanContinuumCode2013")
miss_rucc <- need_rucc[!need_rucc %in% names(rucc)]
if (length(miss_rucc) > 0L) stop(sprintf("Missing RUCC columns: %s", paste(miss_rucc, collapse = ", ")))
rucc <- unique(rucc[, .(
  county_fips = sprintf("%05d", suppressWarnings(as.integer(FIPStxt))),
  rucc_code = suppressWarnings(as.integer(RuralUrbanContinuumCode2013))
)])
rucc <- rucc[!is.na(county_fips) & county_fips != "00000" & rucc_code %in% 1:9]
pop_group <- merge(pop_group, pop[, .(county_fips, total_pop_2020)], by = "county_fips", all.x = FALSE)
pop_group <- merge(pop_group, rucc[, .(county_fips, rucc_code)], by = "county_fips", all.x = FALSE)

county_size_tier <- function(group_pop, rucc_code) {
  out <- rep("unknown", length(group_pop))
  out[!is.na(group_pop) & group_pop >= 1000000] <- "mega_urban"
  out[!is.na(group_pop) & group_pop >= 250000 & group_pop < 1000000] <- "large_urban"
  out[!is.na(group_pop) & group_pop >= 100000 & group_pop < 250000] <- "mid_urban"
  out[!is.na(group_pop) & group_pop >= 50000 & group_pop < 100000] <- "small_urban"
  # For low-pop counties, use RUCC adjacency to avoid size-only "suburban" labeling.
  out[!is.na(group_pop) & group_pop < 50000 & rucc_code %in% c(1L, 2L, 3L, 4L, 6L, 8L)] <- "suburban"
  out[!is.na(group_pop) & group_pop < 50000 & rucc_code %in% c(5L, 7L, 9L)] <- "rural"
  out
}
pop_group[, size_tier := county_size_tier(group_pop_2020, rucc_code)]

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

dt <- merge(org, pop_group[, .(county_fips, panethnic_group, group_pop_2020, size_tier)], by = c("county_fips", "panethnic_group"), all.x = FALSE)
tiers <- c("mega_urban", "large_urban", "mid_urban", "small_urban", "suburban", "rural")
dt <- dt[size_tier %in% tiers]

flow <- dt[, .(new_org_n = uniqueN(ein)), by = .(panethnic_group, size_tier, fnd_yr)]
grid <- CJ(
  panethnic_group = c("asian", "latino"),
  size_tier = tiers,
  fnd_yr = cfg$min_year:cfg$max_year,
  unique = TRUE
)
flow <- merge(grid, flow, by = c("panethnic_group", "size_tier", "fnd_yr"), all.x = TRUE)
flow[is.na(new_org_n), new_org_n := 0L]

# Normalize flow by county size before calculating shares:
# per tier denominator = total relevant-group 2020 population across counties in that tier.
tier_pop <- pop_group[size_tier %in% tiers, .(
  tier_pop_2020 = sum(group_pop_2020, na.rm = TRUE)
), by = .(panethnic_group, size_tier)]
flow <- merge(flow, tier_pop, by = c("panethnic_group", "size_tier"), all.x = TRUE)
flow[, flow_rate_per_100k := fifelse(tier_pop_2020 > 0, (new_org_n / tier_pop_2020) * 100000, NA_real_)]
flow[, flow_rate_roll5 := zoo::rollapply(
  flow_rate_per_100k,
  width = cfg$roll_k,
  FUN = mean,
  fill = NA,
  align = "center",
  partial = TRUE,
  na.rm = TRUE
), by = .(panethnic_group, size_tier)]
flow[, total_rate_roll5 := sum(flow_rate_roll5, na.rm = TRUE), by = .(panethnic_group, fnd_yr)]
flow[, flow_share_roll5 := fifelse(total_rate_roll5 > 0, 100 * flow_rate_roll5 / total_rate_roll5, NA_real_)]
flow[, group_label := fifelse(panethnic_group == "asian", "Asian", "Latino")]

# 95% CI via parametric bootstrap on yearly tier counts, then same rolling/share transform.
set.seed(cfg$seed)
boot_grid <- flow[, .(panethnic_group, size_tier, fnd_yr, new_org_n, tier_pop_2020)]
boot_dt <- boot_grid[, .(rep = 1:cfg$n_boot), by = .(panethnic_group, size_tier, fnd_yr, new_org_n, tier_pop_2020)]
boot_dt[, n_sim := rpois(.N, lambda = pmax(new_org_n, 0))]
boot_dt[, rate_sim := fifelse(tier_pop_2020 > 0, (n_sim / tier_pop_2020) * 100000, NA_real_)]
boot_dt[, rate_roll5 := zoo::rollapply(
  rate_sim,
  width = cfg$roll_k,
  FUN = mean,
  fill = NA,
  align = "center",
  partial = TRUE,
  na.rm = TRUE
), by = .(rep, panethnic_group, size_tier)]
boot_dt[, total_rate_roll5 := sum(rate_roll5, na.rm = TRUE), by = .(rep, panethnic_group, fnd_yr)]
boot_dt[, share_roll5 := fifelse(total_rate_roll5 > 0, 100 * rate_roll5 / total_rate_roll5, NA_real_)]
ci_dt <- boot_dt[!is.na(share_roll5), .(
  share_lo = as.numeric(quantile(share_roll5, probs = 0.025, na.rm = TRUE)),
  share_hi = as.numeric(quantile(share_roll5, probs = 0.975, na.rm = TRUE))
), by = .(panethnic_group, size_tier, fnd_yr)]
flow <- merge(flow, ci_dt, by = c("panethnic_group", "size_tier", "fnd_yr"), all.x = TRUE)

tier_labels <- c(
  mega_urban = "Mega urban (1M+)",
  large_urban = "Large urban (250k-999k)",
  mid_urban = "Mid urban (100k-249k)",
  small_urban = "Small urban (50k-99k)",
  suburban = "Suburban (size + RUCC-adj)",
  rural = "Rural (size + RUCC-remote)"
)
flow[, size_tier_label := factor(tier_labels[size_tier], levels = unname(tier_labels))]
flow[, size_tier_short := fifelse(
  size_tier == "mega_urban", "Mega",
  fifelse(size_tier == "large_urban", "Large",
    fifelse(size_tier == "mid_urban", "Mid",
      fifelse(size_tier == "small_urban", "Small",
        fifelse(size_tier == "suburban", "Suburban", "Rural")
      )
    )
  )
)]

label_dt <- flow[!is.na(flow_share_roll5), .SD[which.max(fnd_yr)], by = .(group_label, size_tier, size_tier_label, size_tier_short)]
label_dt <- label_dt[, .(group_label, size_tier, size_tier_label, size_tier_short, y_end = flow_share_roll5)]

spread_labels <- function(y, min_gap = 3, lower = 0, upper = 100) {
  if (length(y) == 0L) return(numeric())
  ys <- y
  if (length(ys) >= 2L) {
    for (i in 2:length(ys)) {
      ys[i] <- min(ys[i], ys[i - 1] - min_gap)
    }
    ys <- pmax(ys, lower)
    if (max(ys, na.rm = TRUE) > upper) {
      ys <- ys - (max(ys, na.rm = TRUE) - upper)
    }
    ys <- pmax(ys, lower)
  }
  ys
}

label_dt <- label_dt[order(group_label, -y_end)]
label_dt[, y_label := spread_labels(y_end), by = group_label]
label_dt[, is_highlight := size_tier_short %in% c("Mega", "Small")]
label_dt[, label_fontface := fifelse(is_highlight, "bold", "plain")]
label_dt[, label_text_color := fifelse(is_highlight, "black", "#6F6F6F")]
label_dt[, label_fill := fifelse(is_highlight, alpha("white", 0.95), alpha("white", 0.75))]
label_dt[, label_size := fifelse(is_highlight, 3.35, 3.0)]
label_dt[, `:=`(
  x_label = cfg$max_year + 0.9,
  x_link_start = cfg$max_year,
  x_link_end = cfg$max_year + 0.75
)]

fwrite(flow[order(group_label, size_tier, fnd_yr)], cfg$out_table)

y_max <- suppressWarnings(max(flow$flow_share_roll5, na.rm = TRUE))
if (!is.finite(y_max)) y_max <- 100
y_upper <- max(20, min(100, ceiling((y_max * 1.12) / 5) * 5))

tier_key <- paste(
  "Tier key (hybrid):",
  "Mega: relevant-group pop >= 1,000,000",
  "Large: 250,000-999,999",
  "Mid: 100,000-249,999",
  "Small: 50,000-99,999",
  "Suburban: <50,000 and RUCC metro/adjacent",
  "Rural: <50,000 and RUCC non-adjacent",
  sep = "\n"
)
key_dt <- data.table(
  group_label = "Asian",
  x = cfg$max_year + 4.6,
  y = y_upper * 0.98,
  label = tier_key
)
line_style_dt <- data.table(
  size_tier_label = factor(unname(tier_labels), levels = unname(tier_labels)),
  size_tier_short = c("Mega", "Large", "Mid", "Small", "Suburban", "Rural")
)
line_style_dt[, `:=`(
  is_highlight = size_tier_short %in% c("Mega", "Small"),
  line_color = fifelse(size_tier_short %in% c("Mega", "Small"), "#111111", "#A0A0A0"),
  line_width = fifelse(size_tier_short %in% c("Mega", "Small"), 1.35, 0.75)
)]
flow <- merge(flow, line_style_dt[, .(size_tier_label, line_color, line_width)], by = "size_tier_label", all.x = TRUE)
label_bg_dt <- data.table(
  group_label = c("Asian", "Latino"),
  xmin = cfg$max_year + 0.2,
  xmax = cfg$max_year + 5,
  ymin = 0,
  ymax = y_upper
)

p <- ggplot(flow, aes(x = fnd_yr, y = flow_share_roll5, linetype = size_tier_label)) +
  geom_line(aes(color = line_color, linewidth = line_width), na.rm = TRUE, show.legend = FALSE) +
  geom_segment(
    data = label_dt,
    aes(x = x_link_start, xend = x_link_end, y = y_end, yend = y_label),
    inherit.aes = FALSE,
    color = "grey65",
    linewidth = 0.25
  ) +
  geom_text(
    data = label_dt,
    aes(x = x_label, y = y_label, label = size_tier_short),
    inherit.aes = FALSE,
    hjust = 0,
    size = label_dt$label_size,
    fontface = label_dt$label_fontface,
    color = label_dt$label_text_color
  ) +
  geom_label(
    data = key_dt,
    aes(x = x, y = y, label = label),
    inherit.aes = FALSE,
    hjust = 1,
    vjust = 1,
    linewidth = 0.2,
    size = 3.0,
    label.padding = unit(0.15, "lines"),
    fill = "white",
    color = "black"
  ) +
  facet_wrap(~group_label, ncol = 1) +
  scale_color_identity() +
  scale_linewidth_identity() +
  scale_linetype_manual(values = c("solid", "longdash", "dotdash", "twodash", "dashed", "dotted")) +
  scale_y_continuous(labels = label_percent(scale = 1), limits = c(0, y_upper)) +
  scale_x_continuous(limits = c(cfg$min_year, cfg$max_year + 5)) +
  coord_cartesian(clip = "off") +
  labs(
    title = "Share of New Panethnic Incorporations by County Size Tier",
    subtitle = "Hybrid tiers: mega/large/mid/small by relevant-group county population,\nsuburban/rural split by RUCC adjacency; within-group shares, 5-year centered average",
    x = "IRS incorporation year",
    y = "Share of new panethnic incorporations (%)",
    color = NULL,
    linetype = NULL
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    panel.grid.minor = element_blank(),
    panel.grid.major = element_line(color = "#E6E6E6", linewidth = 0.35),
    strip.text = element_text(face = "bold", size = 12),
    plot.title = element_text(face = "bold", size = 17),
    legend.position = "none",
    plot.margin = margin(10, 70, 10, 10)
  )

if (isTRUE(cfg$show_ci)) {
  p <- p + geom_ribbon(
    aes(ymin = share_lo, ymax = share_hi, fill = size_tier_label),
    alpha = 0.12,
    color = NA,
    na.rm = TRUE,
    show.legend = FALSE
  ) +
    scale_fill_grey(start = 0.05, end = 0.85)
}

ggsave(cfg$out_fig, p, width = 12, height = 9, dpi = 220)
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
