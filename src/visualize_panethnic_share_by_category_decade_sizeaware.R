#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(scales)
})

cfg <- list(
  input = "processed_data/org_enriched/org_civic_enriched.csv",
  out_table = "outputs/analysis/panethnic_share_by_category_decade.csv",
  min_n_per_cell = 5L,
  out_fig = "outputs/figures/panethnic_share_by_category_decade_sizeaware.png"
)

dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("ein", "fnd_yr", "panethnic_group", "org_type")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))

dt[, fnd_yr := suppressWarnings(as.integer(fnd_yr))]
dt <- dt[!is.na(fnd_yr) & fnd_yr > 0]

dt[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt <- dt[panethnic_group %in% c("asian", "latino")]
dt[, group := fifelse(panethnic_group == "asian", "Asian", "Latino")]

dt[, org_type := tolower(trimws(as.character(org_type)))]
dt[org_type == "civic", org_type := "political"]
dt <- dt[!is.na(org_type) & org_type != "" & org_type != "unknown"]

dt[, is_panethnic := 0L]
if ("candidate_type" %in% names(dt)) {
  dt[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", is_panethnic := 1L]
}
if ("reclass_applied" %in% names(dt)) {
  dt[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, is_panethnic := 1L]
}

dt[, decade := (fnd_yr %/% 10L) * 10L]
dt[, decade_label := sprintf("%ss", decade)]

dt <- dt[, .(
  org_n = uniqueN(ein),
  panethnic_n = sum(is_panethnic, na.rm = TRUE),
  panethnic_share = mean(is_panethnic, na.rm = TRUE) * 100
), by = .(group, org_type, decade, decade_label)]

dt <- dt[org_n >= cfg$min_n_per_cell]
setorder(dt, group, org_type, decade)

# Keep category ordering by overall volume for easier scan.
ord <- dt[, .(overall_n = sum(org_n)), by = org_type][order(overall_n)]$org_type
dt[, org_type := factor(org_type, levels = ord)]
dt[, decade_label := factor(decade_label, levels = unique(dt[order(decade)]$decade_label))]
dt[, panethnic_share_pct := round(panethnic_share, 2)]

fwrite(dt, cfg$out_table)

p <- ggplot(dt, aes(x = decade_label, y = org_type)) +
  geom_tile(fill = "#F4F4F4", color = "white", linewidth = 0.25) +
  geom_point(aes(size = org_n, fill = panethnic_share), shape = 21, color = "#1F1F1F", stroke = 0.25, alpha = 1) +
  facet_wrap(~group, nrow = 1) +
  scale_fill_gradientn(
    colors = c("#FFFFFF", "#E6E6E6", "#BDBDBD", "#7F7F7F", "#3A3A3A", "#000000"),
    values = rescale(c(0, 8, 20, 40, 65, 100)),
    limits = c(0, 100),
    oob = squish
  ) +
  scale_size_continuous(range = c(1.8, 12), breaks = c(5, 20, 50, 100, 250, 500)) +
  labs(
    title = "Panethnic Share by Organizational Type Across Decades",
    subtitle = "Color = panethnic share (%); point size = number of organizations in each organizational-type decade cell",
    x = "IRS incorporation decade",
    y = "Predicted organizational type",
    fill = "Panethnic %",
    size = "Organizations (n)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    axis.text.x = element_text(angle = 35, hjust = 1),
    legend.position = "bottom"
  )

ggsave(cfg$out_fig, p, width = 12.5, height = 7.8, dpi = 220)

cat(sprintf("Rows analyzed: %s\n", format(nrow(dt), big.mark = ",")))
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
