#!/usr/bin/env Rscript
# Org-type composition flow: Pre-1981 vs Post-1980 by panethnic group
#
# Replicates the alluvial/Sankey style in the reference figure:
#   - Two stacked proportion columns (Pre-1981 | Post-1980)
#   - Ribbons colored by direction of change (increase = 
#   - Separate panels for Asian and Latino organizations

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(ggforce)
  library(scales)
})

# ── Parameters ────────────────────────────────────────────────────────────────

ORG_FILE <- "processed_data/org_enriched/org_civic_enriched.csv"
OUT_FIGURE <- "outputs/figures/orgtype_flow_great_society_vs_reagan.png"
OUT_TABLE <- "outputs/analysis/orgtype_flow_great_society_vs_reagan.csv"
dir.create("outputs/figures", showWarnings = FALSE, recursive = TRUE)
dir.create("outputs/analysis", showWarnings = FALSE, recursive = TRUE)

PRE_MIN <- 1960 # Great Society era: 1960–1980
CUTOFF <- 1980 # pre = PRE_MIN <= fnd_yr <= CUTOFF; post = fnd_yr > CUTOFF
MIN_N <- 1

COLORS <- c("Increase" = "grey70", "Decrease" = "grey30")

TYPE_LABELS <- c(
  religious       = "Religious",
  civic           = "Political",
  education       = "Education",
  arts            = "Arts and cultural",
  socialfraternal = "Social and fraternal",
  hobby           = "Hobby and sports",
  professional    = "Professional",
  econ            = "Economic",
  community       = "Community",
  foundation      = "Foundations",
  research        = "Research",
  youth           = "Youth",
  housing         = "Housing",
  health          = "Healthcare",
  unions          = "Unions"
)

# ── Load & filter ─────────────────────────────────────────────────────────────

orgs <- fread(ORG_FILE)
orgs <- orgs[
  detection_method %in% c("both", "RE", "ML", "ground_truth") &
    panethnic_group %in% c("asian", "latino") &
    !is.na(fnd_yr) & fnd_yr >= PRE_MIN & fnd_yr <= 2023 &
    org_type %in% names(TYPE_LABELS)
]
orgs[, period := fifelse(fnd_yr <= CUTOFF, "1960s–1970s", "Post-1981")]
orgs[, group_label := fifelse(
  panethnic_group == "asian",
  "Asian American", "Latino"
)]
orgs[, type_label := TYPE_LABELS[org_type]]

# ── Compute proportions ───────────────────────────────────────────────────────

counts <- orgs[, .N, by = .(group_label, period, type_label)]
totals <- orgs[, .N, by = .(group_label, period)]
setnames(totals, "N", "total")
counts <- merge(counts, totals, by = c("group_label", "period"))
counts[, prop := N / total]

# Pivot wide to get pre/post side by side
wide <- dcast(counts, group_label + type_label ~ period,
  value.var = "prop", fill = 0
)
PRE_COL <- "1960s–1970s"
POST_COL <- "Post-1981"
wide[, change := get(POST_COL) - get(PRE_COL)]
wide[, direction := fifelse(change >= 0, "Increase", "Decrease")]

fwrite(wide, OUT_TABLE)

# Sample sizes for subtitle
ns <- function(grp, per) {
  totals[group_label == grp & period == per, total]
}
n_a_pre <- ns("Asian American", PRE_COL)
n_a_post <- ns("Asian American", POST_COL)
n_l_pre <- ns("Latino", PRE_COL)
n_l_post <- ns("Latino", POST_COL)

# ── Shared stack order: decreasing on top, increasing on bottom ───────────────
# Within each group, sorted by post proportion (for increasing) and pre
# proportion (for decreasing) — same order on both bars keeps ribbons parallel.

stack_order <- function(dt_group) {
  dec <- dt_group[direction == "Decrease"][order(-get(PRE_COL)), type_label]
  inc <- dt_group[direction == "Increase"][order(-get(POST_COL)), type_label]
  c(dec, inc)
}

make_positions <- function(dt_group, period_col, ord) {
  dt <- dt_group[match(ord, type_label)]
  dt[, prop := get(period_col)]
  dt[, ymax := cumsum(prop)]
  dt[, ymin := c(0, head(ymax, -1))]
  dt[, .(type_label, direction, prop, ymin, ymax)]
}

# ── Build ribbon and bar data ─────────────────────────────────────────────────

make_plot_data <- function(dt_group, grp_name) {
  ord <- stack_order(dt_group)

  left_pos <- make_positions(dt_group, PRE_COL, ord)
  right_pos <- make_positions(dt_group, POST_COL, ord)

  merged <- merge(
    left_pos[, .(type_label, direction, ymin_l = ymin, ymax_l = ymax)],
    right_pos[, .(type_label, ymin_r = ymin, ymax_r = ymax)],
    by = "type_label"
  )
  merged[, group_label := grp_name]

  # Sigmoid ribbon polygons + boundary lines
  ribbon_list <- lapply(seq_len(nrow(merged)), function(i) {
    row <- merged[i]
    t <- seq(0, 1, length.out = 80)
    xv <- 1 + t
    s <- 1 / (1 + exp(-12 * (t - 0.5)))
    y_top <- row$ymax_l + (row$ymax_r - row$ymax_l) * s
    y_bot <- row$ymin_l + (row$ymin_r - row$ymin_l) * s
    list(
      poly = data.table(
        x = c(xv, rev(xv)), y = c(y_top, rev(y_bot)),
        type_label = row$type_label, direction = row$direction,
        group_label = grp_name, ribbon_id = i
      ),
      # top and bottom edges as separate paths for white separator lines
      edges = data.table(
        x = c(xv, xv),
        y = c(y_top, y_bot),
        edge = c(rep("top", length(xv)), rep("bot", length(xv))),
        type_label = row$type_label,
        group_label = grp_name,
        ribbon_id = i
      )
    )
  })
  ribbons <- rbindlist(lapply(ribbon_list, `[[`, "poly"))
  edges <- rbindlist(lapply(ribbon_list, `[[`, "edges"))

  # Bar rectangles
  bars <- rbind(
    left_pos[, .(type_label, direction, prop, ymin, ymax, x = 1)],
    right_pos[, .(type_label, direction, prop, ymin, ymax, x = 2)]
  )
  bars[, group_label := grp_name]

  list(ribbons = ribbons, edges = edges, bars = bars)
}

plot_data <- lapply(
  c("Asian American", "Latino"),
  function(g) make_plot_data(wide[group_label == g], g)
)
ribbon_data <- rbindlist(lapply(plot_data, `[[`, "ribbons"))
edge_data <- rbindlist(lapply(plot_data, `[[`, "edges"))
bar_data <- rbindlist(lapply(plot_data, `[[`, "bars"))

# ── Label data ────────────────────────────────────────────────────────────────

label_data <- bar_data[, .(
  y_mid = (ymin + ymax) / 2,
  prop = prop,
  x = x,
  direction = direction
), by = .(group_label, type_label)]

# Show label only if the slice is tall enough; left=right-justified, right=left
label_data[, label := fifelse(prop >= 0.030, type_label, "")]

# ── Bar geometry constants ────────────────────────────────────────────────────
BAR_W <- 0.10 # half-width of each bar
X_LEFT <- 1.0
X_RIGHT <- 2.0

# ── Plot ──────────────────────────────────────────────────────────────────────

p <- ggplot() +
  # Ribbons (between bars)
  geom_polygon(
    data = ribbon_data,
    aes(
      x = x, y = y, group = interaction(group_label, ribbon_id),
      fill = direction
    ),
    alpha = 0.40, color = NA
  ) +
  # White separator lines along ribbon boundaries
  geom_path(
    data = edge_data,
    aes(x = x, y = y, group = interaction(group_label, ribbon_id, edge)),
    color = "black", linewidth = 0.4
  ) +
  # Left bar
  geom_rect(
    data = bar_data[x == 1],
    aes(
      xmin = X_LEFT - BAR_W, xmax = X_LEFT,
      ymin = ymin, ymax = ymax, fill = direction
    ),
    color = "white", linewidth = 0.25
  ) +
  # Right bar
  geom_rect(
    data = bar_data[x == 2],
    aes(
      xmin = X_RIGHT, xmax = X_RIGHT + BAR_W,
      ymin = ymin, ymax = ymax, fill = direction
    ),
    color = "white", linewidth = 0.25
  ) +
  # Left labels (right-justified, outside left bar)
  geom_text(
    data = label_data[x == 1 & label != ""],
    aes(x = X_LEFT - BAR_W - 0.02, y = y_mid, label = label),
    hjust = 1, size = 4.0, lineheight = 0.85
  ) +
  # Right labels (left-justified, outside right bar)
  geom_text(
    data = label_data[x == 2 & label != ""],
    aes(x = X_RIGHT + BAR_W + 0.02, y = y_mid, label = label),
    hjust = 0, size = 4.0, lineheight = 0.85
  ) +
  # Period labels below bars
  annotate("text",
    x = X_LEFT - BAR_W / 2, y = -0.05,
    label = "1960s–1970s", size = 4.2, fontface = "bold", hjust = 0.5
  ) +
  annotate("text",
    x = X_RIGHT + BAR_W / 2, y = -0.05,
    label = "Post-1981", size = 4.2, fontface = "bold", hjust = 0.5
  ) +
  scale_fill_manual(values = COLORS, name = "Proportion change") +
  scale_y_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(-0.10, 1.01),
    breaks = seq(0, 1, 0.25)
  ) +
  scale_x_continuous(limits = c(0.3, 2.7)) +
  coord_cartesian(clip = "off") +
  facet_wrap(~group_label, ncol = 2) +
  labs(
    y        = "Proportion",
    x        = NULL,
    title    = "How panethnic organizational types shifted: 1960s–1970s vs. Post-1981"
  ) +
  theme_minimal(base_size = 15) +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    plot.subtitle      = element_text(size = 12, color = "black"),
    axis.text.y        = element_text(size = 13),
    axis.title.y       = element_text(size = 14),
    strip.text         = element_text(face = "bold", size = 16),
    legend.position    = "bottom",
    legend.title       = element_text(face = "bold", size = 13),
    legend.text        = element_text(size = 13),
    plot.title         = element_text(face = "bold", size = 15),
    plot.margin        = margin(6, 40, 6, 40)
  )

ggsave(OUT_FIGURE, p, width = 15, height = 7.5, dpi = 180)
cat("Figure saved:", OUT_FIGURE, "\n")
cat("Table saved: ", OUT_TABLE, "\n")
cat("\nProportion changes (top movers):\n")
wide[, abs_change := abs(change)]
setorder(wide, group_label, -abs_change)
wide[, abs_change := NULL]
print(wide[, .(group_label, type_label,
  pre = round(get(PRE_COL), 3),
  post = round(get(POST_COL), 3),
  change = round(change, 3), direction
)])
