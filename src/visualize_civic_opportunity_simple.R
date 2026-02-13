#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
})

cfg <- list(
  input = "processed_data/org_enriched/org_civic_enriched.csv",
  out_table = "outputs/analysis/civic_rate_within_group_scope_summary.csv",
  out_fig = "outputs/figures/civic_opportunity_rate_by_group_scope.png",
  n_boot = 2000L,
  seed = 20260213L
)

dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("ein", "civic_any", "panethnic_group")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))

dt[, civic_any := as.integer(fifelse(is.na(civic_any), 0L, as.integer(civic_any > 0)))]
dt[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt <- dt[panethnic_group %in% c("asian", "latino")]

dt[, identity_scope := "ethnic"]
if ("candidate_type" %in% names(dt)) {
  dt[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", identity_scope := "panethnic"]
}
if ("reclass_applied" %in% names(dt)) {
  dt[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, identity_scope := "panethnic"]
}

bootstrap_ci_pct <- function(x, n_boot = 2000L, probs = c(0.025, 0.975)) {
  n <- length(x)
  if (n == 0L) return(c(NA_real_, NA_real_))
  boots <- replicate(n_boot, mean(sample(x, size = n, replace = TRUE), na.rm = TRUE) * 100)
  as.numeric(stats::quantile(boots, probs = probs, na.rm = TRUE, names = FALSE))
}

set.seed(cfg$seed)
out <- dt[, .(
  org_n = uniqueN(ein),
  civic_any_n = sum(civic_any, na.rm = TRUE),
  civic_any_pct = round(mean(civic_any, na.rm = TRUE) * 100, 2),
  ci_lower_pct = round(bootstrap_ci_pct(civic_any, n_boot = cfg$n_boot)[1], 2),
  ci_upper_pct = round(bootstrap_ci_pct(civic_any, n_boot = cfg$n_boot)[2], 2)
), by = .(panethnic_group, identity_scope)]
fwrite(out, cfg$out_table)

plot_dt <- copy(out)
plot_dt[, panethnic_group := fifelse(panethnic_group == "asian", "Asian", "Latino")]
plot_dt[, identity_scope := fifelse(identity_scope == "ethnic", "Ethnic", "Panethnic")]
plot_dt[, panethnic_group := factor(panethnic_group, levels = c("Asian", "Latino"))]
plot_dt[, identity_scope := factor(identity_scope, levels = c("Ethnic", "Panethnic"))]

p <- ggplot(plot_dt, aes(x = identity_scope, y = civic_any_pct)) +
  geom_col(width = 0.62, fill = "#8C8C8C", show.legend = FALSE) +
  geom_errorbar(aes(ymin = ci_lower_pct, ymax = ci_upper_pct), width = 0.16, linewidth = 0.5, color = "#2F2F2F") +
  geom_text(aes(label = sprintf("%.1f%%", civic_any_pct)), vjust = -0.4, size = 4) +
  facet_wrap(~panethnic_group, nrow = 1) +
  scale_y_continuous(labels = scales::label_percent(scale = 1)) +
  coord_cartesian(ylim = c(0, max(plot_dt$civic_any_pct) + 8)) +
  labs(
    title = "Share of Organizations Providing Civic Opportunity",
    subtitle = "Within each group: ethnic vs panethnic organizations",
    x = NULL,
    y = "Organizations providing civic opportunity (%)"
  ) +
  theme_minimal(base_size = 13)

ggsave(cfg$out_fig, p, width = 9, height = 4.8, dpi = 220)

cat(sprintf("Rows analyzed: %s\n", format(nrow(dt), big.mark = ",")))
cat(sprintf("Bootstrap resamples per estimate: %s\n", format(cfg$n_boot, big.mark = ",")))
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
