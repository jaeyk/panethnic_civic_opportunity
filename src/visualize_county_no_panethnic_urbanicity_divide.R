#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(scales)
})

cfg <- list(
  input = "outputs/analysis/county_no_panethnic_urbanicity_summary_2020.csv",
  out_fig = "outputs/figures/county_no_panethnic_urbanicity_divide.png"
)

if (!file.exists(cfg$input)) {
  stop(sprintf("Missing input file: %s\nRun src/analyze_county_urbanicity_no_panethnic.R first.", cfg$input))
}
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
need <- c("group_label", "urbanicity", "share_counties_pct", "county_n")
miss <- need[!need %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing columns: %s", paste(miss, collapse = ", ")))

dt <- dt[urbanicity %in% c("urban", "suburban", "rural")]
dt[, urbanicity := factor(urbanicity, levels = c("urban", "suburban", "rural"), labels = c("Urban", "Suburban", "Rural"))]
dt[, label := sprintf("%.1f%%", share_counties_pct)]

p <- ggplot(dt, aes(x = urbanicity, y = share_counties_pct, fill = urbanicity)) +
  geom_col(width = 0.7, color = "grey30", linewidth = 0.2) +
  geom_text(aes(label = label), vjust = -0.3, size = 4) +
  facet_wrap(~group_label, nrow = 1) +
  scale_y_continuous(labels = label_percent(scale = 1), limits = c(0, max(dt$share_counties_pct) * 1.15)) +
  scale_fill_manual(
    values = c("Urban" = "#BDBDBD", "Suburban" = "#636363", "Rural" = "#D9D9D9"),
    guide = "none"
  ) +
  labs(
    title = "Urban-Suburban-Rural Divide in Counties with No Panethnic Organizations",
    subtitle = "Among counties with relevant group population in 2020, suburban counties are the largest share",
    x = NULL,
    y = "Share of counties (%)"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    panel.grid.minor = element_blank(),
    strip.text = element_text(face = "bold", size = 13),
    plot.title = element_text(face = "bold", size = 18),
    plot.subtitle = element_text(size = 12)
  )

ggsave(cfg$out_fig, p, width = 11, height = 5.8, dpi = 220)
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
