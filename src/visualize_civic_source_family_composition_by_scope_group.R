#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(scales)
})

cfg <- list(
  input = "processed_data/org_enriched/org_civic_enriched.csv",
  out_table = "outputs/analysis/civic_source_family_composition_by_scope_group.csv",
  out_fig = "outputs/figures/civic_source_family_composition_by_scope_group.png"
)

dir.create(dirname(cfg$out_table), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("ein", "panethnic_group", "candidate_type", "org_type", "civic_any", "safety_net")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) {
  stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))
}

dt[, panethnic_group := tolower(trimws(as.character(panethnic_group)))]
dt <- dt[panethnic_group %in% c("asian", "latino")]

dt[, identity_scope := "Ethnic"]
dt[tolower(trimws(as.character(candidate_type))) == "direct_panethnic", identity_scope := "Panethnic"]
if ("reclass_applied" %in% names(dt)) {
  dt[as.integer(fifelse(is.na(reclass_applied), 0L, as.integer(reclass_applied > 0))) == 1L, identity_scope := "Panethnic"]
}

dt[, org_type := tolower(trimws(as.character(org_type)))]
dt[org_type == "civic", org_type := "political"]
dt <- dt[!is.na(org_type) & org_type != "" & org_type != "unknown"]

dt[, civic_any := as.integer(fifelse(is.na(civic_any), 0L, as.integer(civic_any > 0)))]
dt[, safety_net := as.integer(fifelse(is.na(safety_net), 0L, as.integer(safety_net > 0)))]
dt <- dt[civic_any == 1L]

# Mutually exclusive source families for interpretation.
dt[, source_family := fifelse(
  org_type %in% c("professional", "econ"), "Professional/Business-oriented",
  fifelse(
    org_type %in% c("youth", "housing", "education", "health", "political", "community"),
    "Advocacy/Service-oriented",
    "Others"
  )
)]

dt[, group := fifelse(panethnic_group == "asian", "Asian", "Latino")]

out <- dt[, .(n = uniqueN(ein)), by = .(group, identity_scope, source_family)]
out[, total := sum(n), by = .(group, identity_scope)]
out[, share_pct := round(100 * n / total, 2)]
out[, source_family := factor(
  source_family,
  levels = c(
    "Professional/Business-oriented",
    "Advocacy/Service-oriented",
    "Others"
  )
)]
setorder(out, group, identity_scope, source_family)
fwrite(out, cfg$out_table)

plot_dt <- copy(out)
plot_dt[, identity_scope := factor(identity_scope, levels = c("Ethnic", "Panethnic"))]
plot_dt[, group := factor(group, levels = c("Asian", "Latino"))]

p <- ggplot(plot_dt, aes(x = identity_scope, y = share_pct, fill = source_family)) +
  geom_col(width = 0.62, color = "#2F2F2F", linewidth = 0.2) +
  geom_text(
    aes(
      label = sprintf("%.1f%%", share_pct),
      color = source_family
    ),
    position = position_stack(vjust = 0.5),
    size = 3.1,
    show.legend = FALSE
  ) +
  facet_wrap(~group, nrow = 1) +
  scale_y_continuous(labels = label_percent(scale = 1), expand = expansion(mult = c(0, 0.02))) +
  scale_fill_manual(
    values = c(
      "Professional/Business-oriented" = "#111111",
      "Advocacy/Service-oriented" = "#595959",
      "Others" = "#D9D9D9"
    )
  ) +
  scale_color_manual(
    values = c(
      "Professional/Business-oriented" = "#FFFFFF",
      "Advocacy/Service-oriented" = "#FFFFFF",
      "Others" = "#1F1F1F"
    )
  ) +
  labs(
    title = "Who Provides Civic Opportunity? Source Composition Within Group",
    subtitle = "Among civic-opportunity organizations only; stacked shares sum to 100% within each bar",
    x = NULL,
    y = "Share of civic-opportunity providers (%)",
    fill = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    legend.position = "bottom",
    strip.text = element_text(face = "bold")
  )

ggsave(cfg$out_fig, p, width = 10, height = 5.6, dpi = 220)

cat(sprintf("Rows analyzed (civic_any == 1): %s\n", format(nrow(dt), big.mark = ",")))
cat(sprintf("Wrote table: %s\n", cfg$out_table))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
