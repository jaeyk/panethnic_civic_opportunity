#!/usr/bin/env Rscript
# Org-type composition flow: Pre-1981 vs Post-1980 by panethnic group
#
# Replicates the alluvial/Sankey style in the reference figure:
#   - Two stacked proportion columns (Pre-1981 | Post-1980)
#   - Ribbons colored by direction of change (increase = teal, decrease = red)
#   - Separate panels for Asian and Latino organizations

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(ggforce)
  library(scales)
})

# ── Parameters ────────────────────────────────────────────────────────────────

ORG_FILE   <- "processed_data/org_enriched/org_civic_enriched.csv"
OUT_FIGURE <- "outputs/figures/orgtype_flow_pre_post_1981.png"
OUT_TABLE  <- "outputs/analysis/orgtype_flow_pre_post_1981.csv"
dir.create("outputs/figures",  showWarnings = FALSE, recursive = TRUE)
dir.create("outputs/analysis", showWarnings = FALSE, recursive = TRUE)

CUTOFF <- 1980          # pre = fnd_yr <= CUTOFF; post = fnd_yr > CUTOFF
MIN_N  <- 1             # minimum orgs in a cell to display

COLORS <- c("Increase" = "#5bbfbf", "Decrease" = "#d9534f")

TYPE_LABELS <- c(
  religious       = "Religious",
  civic           = "Civic/political",
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
  detection_strategy %in% c("direct_RE", "indirect_RE", "ground_truth") &
    panethnic_group %in% c("asian", "latino") &
    !is.na(fnd_yr) & fnd_yr >= 1900 & fnd_yr <= 2023 &
    org_type %in% names(TYPE_LABELS)
]
orgs[, period := fifelse(fnd_yr <= CUTOFF, "Pre-1981", "Post-1980")]
orgs[, group_label := fifelse(panethnic_group == "asian",
                              "Asian American", "Latino")]
orgs[, type_label := TYPE_LABELS[org_type]]

# ── Compute proportions ───────────────────────────────────────────────────────

counts <- orgs[, .N, by = .(group_label, period, type_label)]
totals <- orgs[, .N, by = .(group_label, period)]
setnames(totals, "N", "total")
counts <- merge(counts, totals, by = c("group_label", "period"))
counts[, prop := N / total]

# Pivot wide to get pre/post side by side
wide <- dcast(counts, group_label + type_label ~ period,
              value.var = "prop", fill = 0)
wide[, change := `Post-1980` - `Pre-1981`]
wide[, direction := fifelse(change >= 0, "Increase", "Decrease")]

fwrite(wide, OUT_TABLE)

# ── Build ribbon data for each group panel ────────────────────────────────────

make_ribbon_data <- function(dt_group, grp_name) {
  # Sort pre-1981 descending for left column
  pre_order  <- dt_group[order(-`Pre-1981`), type_label]
  # Sort post-1980 descending for right column
  post_order <- dt_group[order(-`Post-1980`), type_label]

  # Cumulative positions for left and right stacks
  left_pos  <- dt_group[match(pre_order,  type_label)]
  right_pos <- dt_group[match(post_order, type_label)]

  left_pos[,  ymax_l := cumsum(`Pre-1981`)]
  left_pos[,  ymin_l := c(0, head(ymax_l, -1))]
  right_pos[, ymax_r := cumsum(`Post-1980`)]
  right_pos[, ymin_r := c(0, head(ymax_r, -1))]

  # Merge to pair left/right positions by type
  merged <- merge(
    left_pos[,  .(type_label, ymin_l, ymax_l, direction)],
    right_pos[, .(type_label, ymin_r, ymax_r)],
    by = "type_label"
  )
  merged[, group_label := grp_name]

  # Smooth Bezier ribbon: each ribbon = polygon with sigmoid x-interpolation
  ribbon_list <- lapply(seq_len(nrow(merged)), function(i) {
    row <- merged[i]
    t   <- seq(0, 1, length.out = 60)
    xv  <- 1 + t   # x goes from 1 (left bar) to 2 (right bar)
    # Sigmoid smoothing
    s   <- 1 / (1 + exp(-12 * (t - 0.5)))
    # Top edge: ymax_l → ymax_r
    y_top <- row$ymax_l + (row$ymax_r - row$ymax_l) * s
    # Bottom edge: ymin_l → ymin_r (reversed for polygon closure)
    y_bot <- row$ymin_l + (row$ymin_r - row$ymin_l) * s
    data.table(
      x          = c(xv, rev(xv)),
      y          = c(y_top, rev(y_bot)),
      type_label = row$type_label,
      direction  = row$direction,
      group_label = grp_name,
      ribbon_id  = i
    )
  })
  rbindlist(ribbon_list)
}

ribbon_data <- rbindlist(lapply(
  c("Asian American", "Latino"),
  function(g) make_ribbon_data(wide[group_label == g], g)
))

# ── Bar data ──────────────────────────────────────────────────────────────────

make_bar_data <- function(dt_group, grp_name) {
  pre_order  <- dt_group[order(-`Pre-1981`), type_label]
  post_order <- dt_group[order(-`Post-1980`), type_label]

  left  <- dt_group[match(pre_order,  type_label),
                    .(type_label, prop = `Pre-1981`, direction)]
  right <- dt_group[match(post_order, type_label),
                    .(type_label, prop = `Post-1980`, direction)]

  left[,  `:=`(ymax = cumsum(prop), x = 1)]
  right[, `:=`(ymax = cumsum(prop), x = 2)]
  left[,  ymin := c(0, head(ymax, -1))]
  right[, ymin := c(0, head(ymax, -1))]

  out <- rbind(left, right)
  out[, group_label := grp_name]
  out
}

bar_data <- rbindlist(lapply(
  c("Asian American", "Latino"),
  function(g) make_bar_data(wide[group_label == g], g)
))

# ── Label data ────────────────────────────────────────────────────────────────

label_data <- bar_data[, .(
  y_mid = (ymin + ymax) / 2,
  prop  = prop,
  x     = x,
  direction = direction
), by = .(group_label, type_label)]

label_data[, hjust := fifelse(x == 1, 1.08, -0.08)]
# Only label if slice is wide enough to be readable
label_data[, label := fifelse(prop >= 0.025, type_label, "")]

# ── Plot ──────────────────────────────────────────────────────────────────────

p <- ggplot() +
  # Ribbons
  geom_polygon(
    data = ribbon_data,
    aes(x = x, y = y, group = interaction(group_label, ribbon_id),
        fill = direction),
    alpha = 0.45, color = NA
  ) +
  # Bars (left: Pre-1981)
  geom_rect(
    data = bar_data[x == 1],
    aes(xmin = 0.88, xmax = 1.0, ymin = ymin, ymax = ymax,
        fill = direction),
    color = "white", linewidth = 0.3
  ) +
  # Bars (right: Post-1980)
  geom_rect(
    data = bar_data[x == 2],
    aes(xmin = 2.0, xmax = 2.12, ymin = ymin, ymax = ymax,
        fill = direction),
    color = "white", linewidth = 0.3
  ) +
  # Left labels
  geom_text(
    data = label_data[x == 1 & label != ""],
    aes(x = 0.87, y = y_mid, label = label),
    hjust = 1, size = 2.6, lineheight = 0.9
  ) +
  # Right labels
  geom_text(
    data = label_data[x == 2 & label != ""],
    aes(x = 2.13, y = y_mid, label = label),
    hjust = 0, size = 2.6, lineheight = 0.9
  ) +
  # Period axis labels
  annotate("text", x = 0.94, y = -0.04, label = "Pre-1981",
           size = 3.2, fontface = "bold", hjust = 0.5) +
  annotate("text", x = 2.06, y = -0.04, label = "Post-1980",
           size = 3.2, fontface = "bold", hjust = 0.5) +
  scale_fill_manual(
    values = COLORS,
    name   = "Proportion change"
  ) +
  scale_y_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(-0.06, 1.01),
    breaks = seq(0, 1, 0.25)
  ) +
  scale_x_continuous(limits = c(-0.5, 3.4)) +
  coord_cartesian(clip = "off") +
  facet_wrap(~group_label, ncol = 2) +
  labs(
    y     = "Proportion",
    x     = NULL,
    title = "Organizational type composition of panethnic organizations: Pre-1981 vs. Post-1980",
    subtitle = "Panethnic orgs only (direct name + reclassified + ground truth). Color = direction of share change after 1980.\nAsian pre-1981 n=137; Asian post-1980 n=864; Latino pre-1981 n=300; Latino post-1980 n=1,269."
  ) +
  theme_minimal(base_size = 11) +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.x        = element_blank(),
    axis.ticks.x       = element_blank(),
    strip.text         = element_text(face = "bold", size = 12),
    legend.position    = "bottom",
    legend.title       = element_text(face = "bold"),
    plot.title         = element_text(face = "bold"),
    plot.margin        = margin(10, 60, 10, 60)
  )

ggsave(OUT_FIGURE, p, width = 16, height = 8, dpi = 180)
cat("Figure saved:", OUT_FIGURE, "\n")
cat("Table saved: ", OUT_TABLE, "\n")
cat("\nProportion changes (top movers):\n")
wide[, abs_change := abs(change)]
setorder(wide, group_label, -abs_change)
wide[, abs_change := NULL]
print(wide[, .(group_label, type_label, pre = round(`Pre-1981`, 3),
               post = round(`Post-1980`, 3), change = round(change, 3),
               direction)])
