#!/usr/bin/env Rscript
# Visualize population share vs org share by urbanicity for panethnic orgs.
#
# Story: Asian American and Latino population growth is increasingly happening
# outside urban cores (suburban, rural), but organizational infrastructure
# remains concentrated in urban areas — a mismatch this figure makes visible.
#
# Dumbbell plot: population share vs org share by urbanicity
# Rows: Asian American | Latino

library(data.table)
library(ggplot2)
library(ggrepel)
library(jsonlite)
library(scales)

# ── Paths ─────────────────────────────────────────────────────────────────────
ORG_FILE <- "processed_data/org_enriched/org_civic_enriched.csv"
POP_FILE <- "processed_data/population/census_county_2020_pl_total_asian_latino.json"
RUCC_FILE <- "raw_data/County_Classifications.csv"
OUT_FIG <- "outputs/figures/population_org_urbanicity_mismatch.png"
OUT_TABLE <- "outputs/analysis/population_org_urbanicity_mismatch.csv"

# ── Urbanicity classification (same rules as org_civic_enriched) ──────────────
rucc_to_urban <- function(rucc) {
  fcase(
    rucc == 1, "Urban",
    rucc %in% c(2, 3), "Suburban",
    rucc >= 4, "Rural"
  )
}

URBAN_LEVELS <- c("Urban", "Suburban", "Rural")

# ── 1. Population by urbanicity (2020 Census + RUCC) ─────────────────────────
j <- fromJSON(POP_FILE)
pop <- as.data.table(j[-1, ])
setnames(pop, j[1, ])
pop[, fips := as.integer(paste0(state, county))]
pop[, asian := as.numeric(P1_006N)]
pop[, latino := as.numeric(P2_002N)]
pop <- pop[, .(fips, asian, latino)]

rucc <- fread(RUCC_FILE)[, .(fips = FIPStxt, rucc = RuralUrbanContinuumCode2013)]
pop <- merge(pop, rucc, by = "fips", all.x = TRUE)
pop[, urbanicity := rucc_to_urban(rucc)]
pop <- pop[!is.na(urbanicity)]

pop_by_urban <- pop[, .(
  asian = sum(asian, na.rm = TRUE),
  latino = sum(latino, na.rm = TRUE)
),
by = urbanicity
]
pop_by_urban[, asian_share := asian / sum(asian)]
pop_by_urban[, latino_share := latino / sum(latino)]

# Reshape to long
pop_long <- rbind(
  pop_by_urban[, .(urbanicity, group = "Asian American", pop_share = asian_share)],
  pop_by_urban[, .(urbanicity, group = "Latino", pop_share = latino_share)]
)

# ── 2. Org share by urbanicity (panethnic only) ────────────────────────────────
orgs <- fread(ORG_FILE)
pan <- orgs[
  detection_method %in% c("both", "RE", "ML", "ground_truth") &
    panethnic_group %in% c("asian", "latino") &
    urbanicity %in% c("urban", "suburban", "rural")
]
pan[, urbanicity := fifelse(
  urbanicity == "urban", "Urban",
  fifelse(urbanicity == "suburban", "Suburban", "Rural")
)]
pan[, group_label := fifelse(panethnic_group == "asian", "Asian American", "Latino")]

org_totals <- pan[, .N, by = .(group_label, urbanicity)]
org_totals[, org_share := N / sum(N), by = group_label]
setnames(org_totals, "group_label", "group")

# ── 3. Civic opportunity org share by urbanicity (civic_any == 1) ─────────────
civ_orgs <- pan[civic_any == 1]
civ_totals <- civ_orgs[, .N, by = .(group_label, urbanicity)]
civ_totals[, civ_org_share := N / sum(N), by = group_label]
setnames(civ_totals, "group_label", "group")

# ── 4. Merge and tidy ─────────────────────────────────────────────────────────
dumbbell <- merge(pop_long, org_totals[, .(group, urbanicity, org_share)],
  by = c("group", "urbanicity")
)
dumbbell[, urbanicity := factor(urbanicity, levels = URBAN_LEVELS)]
dumbbell[, group := factor(group, levels = c("Asian American", "Latino"))]

dumbbell_civ <- merge(pop_long, civ_totals[, .(group, urbanicity, civ_org_share)],
  by = c("group", "urbanicity")
)
dumbbell_civ[, urbanicity := factor(urbanicity, levels = URBAN_LEVELS)]
dumbbell_civ[, group := factor(group, levels = c("Asian American", "Latino"))]

out_tbl <- merge(
  dumbbell,
  dumbbell_civ[, .(group, urbanicity, civ_org_share)],
  by = c("group", "urbanicity")
)
fwrite(out_tbl, OUT_TABLE)
cat("Table saved:", OUT_TABLE, "\n")

# ── 5. Build plot ─────────────────────────────────────────────────────────────

# Merge all three shares; segment spans full range per row
combined <- merge(
  dumbbell[, .(group, urbanicity, pop_share, org_share)],
  dumbbell_civ[, .(group, urbanicity, civ_org_share)],
  by = c("group", "urbanicity")
)
combined[, seg_lo := pmin(pop_share, org_share, civ_org_share)]
combined[, seg_hi := pmax(pop_share, org_share, civ_org_share)]
combined[, urbanicity := factor(urbanicity, levels = URBAN_LEVELS)]
combined[, group := factor(group, levels = c("Asian American", "Latino"))]

MEASURES <- c("Population", "All organizations", "Civic opportunity organizations")
combined_long <- rbind(
  combined[, .(group, urbanicity, share = pop_share,     measure = "Population")],
  combined[, .(group, urbanicity, share = org_share,     measure = "All organizations")],
  combined[, .(group, urbanicity, share = civ_org_share, measure = "Civic opportunity organizations")]
)
combined_long[, measure := factor(measure, levels = MEASURES)]

p_combined <- ggplot(combined, aes(y = urbanicity)) +
  geom_segment(
    aes(x = seg_lo, xend = seg_hi, yend = urbanicity),
    color = "grey60", linewidth = 1.2
  ) +
  geom_point(
    data  = combined_long,
    aes(x = share, shape = measure, fill = measure),
    size = 5, color = "grey20", stroke = 1.2
  ) +
  geom_text_repel(
    data  = combined_long[measure == "Population"],
    aes(x = share, label = percent(share, accuracy = 1)),
    nudge_y = 0.32, direction = "y", min.segment.length = 0,
    segment.color = "grey50", segment.size = 0.4,
    size = 4.2, color = "grey30", box.padding = 0.1
  ) +
  geom_text_repel(
    data  = combined_long[measure == "All organizations"],
    aes(x = share, label = percent(share, accuracy = 1)),
    nudge_y = -0.32, direction = "y", min.segment.length = 0,
    segment.color = "grey50", segment.size = 0.4,
    size = 4.2, color = "grey20", box.padding = 0.1
  ) +
  geom_text_repel(
    data  = combined_long[measure == "Civic opportunity organizations"],
    aes(x = share, label = percent(share, accuracy = 1)),
    nudge_y = 0.62, direction = "y", min.segment.length = 0,
    segment.color = "grey50", segment.size = 0.4,
    size = 4.2, color = "grey20", box.padding = 0.1
  ) +
  scale_shape_manual(
    values = c("Population" = 21, "All organizations" = 16, "Civic opportunity organizations" = 18),
    name = NULL
  ) +
  scale_fill_manual(
    values = c("Population" = "white", "All organizations" = "grey20", "Civic opportunity organizations" = "grey20"),
    name = NULL
  ) +
  scale_x_continuous(
    labels = percent_format(accuracy = 1),
    limits = c(0, 1), expand = expansion(mult = c(0.02, 0.08))
  ) +
  scale_y_discrete(limits = rev(URBAN_LEVELS)) +
  facet_wrap(~group, ncol = 1) +
  labs(
    x       = "Share of group total (%)",
    y       = NULL,
    title   = "Panethnic organizations are more urban than the populations they serve",
    caption = paste0(
      "Urbanicity defined by USDA Rural-Urban Continuum Codes (RUCC 2013).\n",
      "Urban = RUCC 1 (large metro); Suburban = RUCC 2–3 (smaller metro); Rural = RUCC 4–9 (non-metro).\n",
      "Civic opportunity organizations = organizations with any of: membership, volunteering, events, or civic and political action."
    )
  ) +
  theme_minimal(base_size = 15) +
  theme(
    strip.text         = element_text(face = "bold", size = 15),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank(),
    axis.text.y        = element_text(size = 14),
    axis.text.x        = element_text(size = 13),
    axis.title.x       = element_text(size = 14),
    plot.title         = element_text(face = "bold", size = 15),
    legend.position    = "bottom",
    legend.text        = element_text(size = 13),
    plot.caption       = element_text(size = 10, color = "grey30", hjust = 0, margin = margin(t = 6)),
    plot.margin        = margin(8, 12, 8, 8)
  )

ggsave(OUT_FIG, p_combined, width = 10, height = 8, dpi = 180)
cat("Figure saved:", OUT_FIG, "\n")

cat("\nSummary table:\n")
print(out_tbl[
  order(group, urbanicity),
  .(group, urbanicity,
    pop_share     = round(pop_share, 3),
    org_share     = round(org_share, 3),
    civ_org_share = round(civ_org_share, 3)
  )
])
