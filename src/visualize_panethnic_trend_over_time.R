#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
})

cfg <- list(
  input = "processed_data/org_enriched/org_civic_enriched.csv",
  out_yearly = "outputs/analysis/panethnic_trend_yearly.csv",
  out_model = "outputs/analysis/panethnic_trend_model_summary.csv",
  out_fig = "outputs/figures/panethnic_trend_over_time.png",
  show_ci = FALSE,
  n_boot = 600L,
  seed = 20260213L
)

dir.create(dirname(cfg$out_yearly), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("ein", "fnd_yr", "panethnic_group")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))

dt[, fnd_yr := suppressWarnings(as.integer(fnd_yr))]
dt <- dt[!is.na(fnd_yr) & fnd_yr >= 1950 & fnd_yr <= 2025]

dt[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt <- dt[panethnic_group %in% c("asian", "latino")]

dt[, scope_panethnic := 0L]
if ("candidate_type" %in% names(dt)) {
  dt[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", scope_panethnic := 1L]
}
if ("reclass_applied" %in% names(dt)) {
  dt[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, scope_panethnic := 1L]
}

yearly_by_group <- dt[, .(
  org_n = uniqueN(ein),
  panethnic_n = sum(scope_panethnic, na.rm = TRUE),
  panethnic_share = mean(scope_panethnic, na.rm = TRUE)
), by = .(panethnic_group, year = fnd_yr)]
yearly_by_group[, series := ifelse(panethnic_group == "asian", "Asian", "Latino")]
yearly_by_group[, panethnic_group := NULL]

yearly_overall <- dt[, .(
  org_n = uniqueN(ein),
  panethnic_n = sum(scope_panethnic, na.rm = TRUE),
  panethnic_share = mean(scope_panethnic, na.rm = TRUE)
), by = .(year = fnd_yr)]
yearly_overall[, series := "All"]

yearly <- rbindlist(list(yearly_overall, yearly_by_group), use.names = TRUE)
setorder(yearly, series, year)
yearly[, panethnic_share_5yr := frollmean(panethnic_share, n = 5, align = "center", na.rm = TRUE), by = series]
yearly[, panethnic_share_pct := round(panethnic_share * 100, 2)]
yearly[, panethnic_share_5yr_pct := round(panethnic_share_5yr * 100, 2)]

if (isTRUE(cfg$show_ci)) {
  # Bootstrap CI for 5-year rolling share.
  bootstrap_window_ci <- function(sub_dt, center_year, n_boot = 600L) {
    w <- sub_dt[year >= (center_year - 2L) & year <= (center_year + 2L)]
    if (nrow(w) == 0L) return(c(NA_real_, NA_real_))

    # Expand counts to binary observations for bootstrap.
    vals <- rep.int(1L, sum(w$panethnic_n))
    vals <- c(vals, rep.int(0L, sum(w$org_n - w$panethnic_n)))
    n <- length(vals)
    if (n < 10L) return(c(NA_real_, NA_real_))

    boots <- replicate(n_boot, mean(sample(vals, size = n, replace = TRUE)))
    as.numeric(stats::quantile(boots, probs = c(0.025, 0.975), na.rm = TRUE, names = FALSE))
  }

  set.seed(cfg$seed)
  ci_dt <- yearly[!is.na(panethnic_share_5yr), .(series, year)]
  ci_dt[, `:=`(ci_lower = NA_real_, ci_upper = NA_real_)]
  for (i in seq_len(nrow(ci_dt))) {
    s <- ci_dt$series[[i]]
    y <- ci_dt$year[[i]]
    ci <- bootstrap_window_ci(yearly[series == s], y, n_boot = cfg$n_boot)
    ci_dt[i, `:=`(ci_lower = ci[[1]], ci_upper = ci[[2]])]
  }
  yearly <- merge(yearly, ci_dt, by = c("series", "year"), all.x = TRUE)
} else {
  yearly[, `:=`(ci_lower = NA_real_, ci_upper = NA_real_)]
}
yearly[, `:=`(
  ci_lower_pct = round(ci_lower * 100, 2),
  ci_upper_pct = round(ci_upper * 100, 2)
)]

fwrite(yearly, cfg$out_yearly)

fit_series <- function(dsub, label) {
  if (nrow(dsub) < 30L || uniqueN(dsub$scope_panethnic) < 2L) {
    return(data.table(series = label, n = nrow(dsub), odds_ratio_per_decade = NA_real_, p_value = NA_real_))
  }
  m <- glm(scope_panethnic ~ I((fnd_yr - 1980) / 10), data = dsub, family = binomial())
  b <- coef(summary(m))[2, "Estimate"]
  p <- coef(summary(m))[2, "Pr(>|z|)"]
  data.table(
    series = label,
    n = nrow(dsub),
    odds_ratio_per_decade = round(exp(b), 3),
    p_value = signif(p, 3)
  )
}

model_tbl <- rbindlist(list(
  fit_series(dt, "All"),
  fit_series(dt[panethnic_group == "asian"], "Asian"),
  fit_series(dt[panethnic_group == "latino"], "Latino")
), use.names = TRUE, fill = TRUE)

fwrite(model_tbl, cfg$out_model)

plot_dt <- yearly[!is.na(panethnic_share_5yr)]
plot_dt[, series := factor(series, levels = c("All", "Asian", "Latino"))]

p <- ggplot(plot_dt, aes(x = year, y = panethnic_share_5yr, color = series))

if (isTRUE(cfg$show_ci)) {
  p <- p +
    geom_ribbon(
      aes(ymin = ci_lower, ymax = ci_upper, fill = series),
      alpha = 0.12,
      color = NA,
      inherit.aes = TRUE
    ) +
    scale_fill_manual(values = c("All" = "#7F7F7F", "Asian" = "#A6A6A6", "Latino" = "#595959")) +
    guides(fill = "none")
}

p <- p +
  geom_line(aes(linetype = series), linewidth = 0.9, color = "#666666") +
  geom_point(aes(shape = series), size = 2.2, color = "#1F1F1F", stroke = 0.7) +
  scale_y_continuous(labels = scales::label_percent(accuracy = 1)) +
  scale_shape_manual(values = c("All" = 16, "Asian" = 17, "Latino" = 15)) +
  scale_linetype_manual(values = c("All" = "solid", "Asian" = "longdash", "Latino" = "dotted")) +
  labs(
    title = "Panethnic Share Over Incorporation Cohorts",
    subtitle = "5-year centered rolling average of share classified as panethnic",
    x = "IRS incorporation year",
    y = "Panethnic share",
    shape = NULL,
    linetype = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(legend.position = "top")

ggsave(cfg$out_fig, p, width = 9.5, height = 5.4, dpi = 220)

cat(sprintf("Rows analyzed: %s\n", format(nrow(dt), big.mark = ",")))
if (isTRUE(cfg$show_ci)) {
  cat(sprintf("Bootstrap resamples for ribbons: %s\n", format(cfg$n_boot, big.mark = ",")))
} else {
  cat("Bootstrap ribbons disabled (show_ci = FALSE)\n")
}
cat(sprintf("Wrote yearly trend: %s\n", cfg$out_yearly))
cat(sprintf("Wrote model summary: %s\n", cfg$out_model))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
