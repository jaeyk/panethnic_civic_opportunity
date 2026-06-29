#!/usr/bin/env Rscript
# Visualize population share vs org share and civic opportunity index by urbanicity
# for panethnic Asian American and Latino organizations.
#
# Story: Asian American and Latino population growth is increasingly happening
# outside urban cores (suburban, rural), but organizational infrastructure and
# civic opportunity remain concentrated in urban areas — a mismatch that this
# figure makes visible.
#
# Left panel:  dumbbell plot — population share vs org share by urbanicity
# Right panel: civic opportunity index (avg of 4 dimensions) by urbanicity
# Rows:        Asian American | Latino

library(data.table)
library(ggplot2)
library(jsonlite)
library(scales)

# ── Paths ─────────────────────────────────────────────────────────────────────
ORG_FILE  <- "processed_data/org_enriched/org_civic_enriched.csv"
POP_FILE  <- "processed_data/population/census_county_2020_pl_total_asian_latino.json"
RUCC_FILE <- "raw_data/County_Classifications.csv"
OUT_FIG   <- "outputs/figures/population_org_urbanicity_mismatch.png"
OUT_TABLE <- "outputs/analysis/population_org_urbanicity_mismatch.csv"

# ── Urbanicity classification (same rules as org_civic_enriched) ──────────────
rucc_to_urban <- function(rucc) {
  fcase(
    rucc == 1,          "Urban",
    rucc %in% c(2, 3), "Suburban",
    rucc >= 4,          "Rural"
  )
}

URBAN_LEVELS <- c("Urban", "Suburban", "Rural")

# ── 1. Population by urbanicity (2020 Census + RUCC) ─────────────────────────
j     <- fromJSON(POP_FILE)
pop   <- as.data.table(j[-1, ]); setnames(pop, j[1, ])
pop[, fips   := as.integer(paste0(state, county))]
pop[, asian  := as.numeric(P1_006N)]
pop[, latino := as.numeric(P2_002N)]
pop   <- pop[, .(fips, asian, latino)]

rucc  <- fread(RUCC_FILE)[, .(fips = FIPStxt, rucc = RuralUrbanContinuumCode2013)]
pop   <- merge(pop, rucc, by = "fips", all.x = TRUE)
pop[, urbanicity := rucc_to_urban(rucc)]
pop   <- pop[!is.na(urbanicity)]

pop_by_urban <- pop[, .(asian = sum(asian, na.rm = TRUE),
                         latino = sum(latino, na.rm = TRUE)),
                    by = urbanicity]
pop_by_urban[, asian_share  := asian  / sum(asian)]
pop_by_urban[, latino_share := latino / sum(latino)]

# Reshape to long
pop_long <- rbind(
  pop_by_urban[, .(urbanicity, group = "Asian American", pop_share = asian_share)],
  pop_by_urban[, .(urbanicity, group = "Latino",         pop_share = latino_share)]
)

# ── 2. Org share by urbanicity (panethnic only) ────────────────────────────────
orgs <- fread(ORG_FILE)
pan  <- orgs[
  detection_strategy %in% c("direct_RE", "indirect_RE", "ground_truth") &
    panethnic_group %in% c("asian", "latino") &
    urbanicity %in% c("urban", "suburban", "rural")
]
pan[, urbanicity  := fifelse(urbanicity == "urban", "Urban",
                    fifelse(urbanicity == "suburban", "Suburban", "Rural"))]
pan[, group_label := fifelse(panethnic_group == "asian", "Asian American", "Latino")]

org_totals <- pan[, .N, by = .(group_label, urbanicity)]
org_totals[, org_share := N / sum(N), by = group_label]
setnames(org_totals, "group_label", "group")

# ── 3. Civic opportunity index by urbanicity (panethnic only) ─────────────────
# Index = average of the four _final binary dimensions
pan[, civ_index := rowMeans(
  cbind(membership_final, volunteer_final, events_final, civic_action_final),
  na.rm = TRUE
)]

civ_by_urban <- pan[, .(
  civ_index = mean(civ_index, na.rm = TRUE),
  n         = .N
), by = .(group_label, urbanicity)]
setnames(civ_by_urban, "group_label", "group")
civ_by_urban[, group      := factor(group, levels = c("Asian American", "Latino"))]
civ_by_urban[, urbanicity := factor(urbanicity, levels = URBAN_LEVELS)]

# ── 4. Merge and tidy ─────────────────────────────────────────────────────────
dumbbell <- merge(pop_long, org_totals[, .(group, urbanicity, org_share)],
                  by = c("group", "urbanicity"))
dumbbell[, urbanicity := factor(urbanicity, levels = URBAN_LEVELS)]
dumbbell[, group      := factor(group, levels = c("Asian American", "Latino"))]

fwrite(merge(dumbbell, civ_by_urban[, .(group, urbanicity, civ_index)],
             by = c("group", "urbanicity")), OUT_TABLE)
cat("Table saved:", OUT_TABLE, "\n")

# ── 5. Build plot ─────────────────────────────────────────────────────────────

# Reshape dumbbell to long for proper legend
dumb_long <- rbind(
  dumbbell[, .(group, urbanicity, share = pop_share, measure = "Population")],
  dumbbell[, .(group, urbanicity, share = org_share, measure = "Organizations")]
)
dumb_long[, measure := factor(measure, levels = c("Population", "Organizations"))]

# Panel A: dumbbell — population share vs org share
p_dumb <- ggplot(dumbbell, aes(y = urbanicity)) +
  geom_segment(
    aes(x = org_share, xend = pop_share, yend = urbanicity),
    color = "grey60", linewidth = 1.2
  ) +
  geom_point(
    data = dumb_long,
    aes(x = share, shape = measure, fill = measure),
    size = 4, color = "grey20", stroke = 1.2
  ) +
  scale_shape_manual(values = c("Population" = 21, "Organizations" = 16),
                     name = NULL) +
  scale_fill_manual(values  = c("Population" = "white", "Organizations" = "grey20"),
                    name = NULL) +
  scale_x_continuous(labels = percent_format(accuracy = 1),
                     limits = c(0, 1), expand = expansion(mult = c(0.02, 0.05))) +
  scale_y_discrete(limits = rev(URBAN_LEVELS)) +
  facet_wrap(~group, ncol = 1) +
  labs(
    x     = "Share of group total (%)",
    y     = NULL,
    title = "Population vs. organizational presence"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    strip.text         = element_text(face = "bold", size = 13),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.y        = element_text(size = 12),
    plot.title         = element_text(face = "bold", size = 13),
    legend.position    = "bottom",
    legend.text        = element_text(size = 12),
    plot.margin        = margin(6, 10, 6, 6)
  )

# Panel B: civic opportunity index
p_civ <- ggplot(civ_by_urban, aes(x = civ_index, y = urbanicity)) +
  geom_col(fill = "grey40", width = 0.55) +
  geom_text(aes(label = sprintf("%.2f", civ_index)),
            hjust = -0.15, size = 3.8, color = "grey20") +
  scale_x_continuous(limits = c(0, 0.55),
                     expand = expansion(mult = c(0.02, 0.05))) +
  scale_y_discrete(limits = rev(URBAN_LEVELS)) +
  facet_wrap(~group, ncol = 1) +
  labs(
    x     = "Civic opportunity index (0–1)",
    y     = NULL,
    title = "Civic opportunity index"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    strip.text         = element_text(face = "bold", size = 13),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.y        = element_blank(),
    plot.title         = element_text(face = "bold", size = 13),
    legend.position    = "bottom",
    plot.margin        = margin(6, 10, 6, 2)
  )

# ── Combine with patchwork ────────────────────────────────────────────────────
library(patchwork)

p_combined <- p_dumb + p_civ +
  plot_layout(widths = c(1.6, 1)) +
  plot_annotation(
    title    = "Panethnic organizations and civic opportunity are more urban than the populations they serve",
    subtitle = "Panethnic Asian American and Latino organizations only; 2020 Census population",
    caption  = paste0(
      "Urbanicity defined by USDA Rural-Urban Continuum Codes (RUCC 2013): ",
      "Urban = RUCC 1 (large metro); Suburban = RUCC 2–3 (smaller metro); Rural = RUCC 4–9 (non-metro).\n",
      "Civic opportunity index = average of membership, volunteering, events, and civic and political action indicators."
    ),
    theme = theme(
      plot.title    = element_text(face = "bold", size = 15),
      plot.subtitle = element_text(size = 12),
      plot.caption  = element_text(size = 9, color = "grey30", hjust = 0,
                                   margin = margin(t = 6))
    )
  )

ggsave(OUT_FIG, p_combined, width = 13, height = 7, dpi = 180)
cat("Figure saved:", OUT_FIG, "\n")

cat("\nSummary table:\n")
print(merge(dumbbell, civ_by_urban[, .(group, urbanicity, civ_index)],
            by = c("group", "urbanicity"))[
  order(group, urbanicity),
  .(group, urbanicity, pop_share = round(pop_share, 3),
    org_share = round(org_share, 3), civ_index = round(civ_index, 3))
])
