#!/usr/bin/env Rscript
# Identify panethnic Asian American and Latino legal organizations from
# org_civic_enriched.csv and report median IRS incorporation year by group.
#
# Panethnic definition: detection_method in {RE, both, ML, ground_truth}
# Legal definition: IRS name contains legal keywords across ALL org_types.
#   Included: legal, law (whole word), lawyer, attorney, bar assoc(iation)
#   Excluded: counsel/counseling — too many mental health counseling false positives
#             law enforcement / peace officer — not legal professional orgs
#
# Subtypes:
#   Professional associations — bar associations, lawyers associations,
#     law student associations, foundations (n = 101: 60 Asian American, 41 Latino)
#   Legal services & advocacy — legal defense funds, legal resource centers,
#     law caucuses, law alliances (n = 10: 5 Asian American, 5 Latino)
#
# Key findings (median IRS incorporation year):
#   Professional associations: Asian American 2011 (IQR 2003-2015)
#                              Latino         2008 (IQR 1996-2015)
#   Legal services & advocacy: Asian American 1994 (IQR 1978-1998)
#                              Latino         1975 (IQR 1972-2018)
#   Legal services orgs are substantially older than professional associations,
#   consistent with their roots in civil rights-era advocacy.
#
# Caveat on fnd_yr: this field reflects the IRS registration date of the current
#   legal entity, not the organization's true founding year. Organizations that
#   reorganized or merged under a new legal name will show a later date. For
#   example, Asian Americans Advancing Justice-Asian Law Caucus shows fnd_yr=1998
#   but was actually founded in 1972. The true founding gap between legal services
#   orgs and professional associations is likely larger than the IRS data suggest.

library(data.table)

ORG_FILE <- "processed_data/org_enriched/org_civic_enriched.csv"
OUT_FILE <- "outputs/analysis/panethnic_orgs_summary.csv"
OUT_LIST_FILE <- "outputs/analysis/panethnic_legal_orgs_list.csv"

# "bar assoc" targets bar associations; plain \bbar\b too broad
# "counsel" excluded — catches mental health counseling orgs
# "law enforcement|peace officer" excluded — not legal professional orgs
LEGAL_PATTERN <- "legal|\\blaw\\b|lawyer|attorney|bar assoc"
EXCLUDE_PATTERN <- "law enforcement|peace officer"

# ── Load and filter ───────────────────────────────────────────────────────────
orgs <- fread(ORG_FILE)

pan <- orgs[
  detection_method %in% c("RE", "both", "ML", "ground_truth") &
    panethnic_group %in% c("asian", "latino", "both") &
    grepl(LEGAL_PATTERN, irs_name_raw, ignore.case = TRUE, perl = TRUE) &
    !grepl(EXCLUDE_PATTERN, irs_name_raw, ignore.case = TRUE, perl = TRUE)
]

# ── Subtype classification (first match wins) ─────────────────────────────────
# Order matters: bar assoc checked before "legal educational" to correctly
# classify bar association education funds as Professional; foundations
# (including law/legal foundations) classified as Professional; default
# is Professional so ambiguous names (e.g. law centers that may be academic)
# don't silently land in Legal services.
pan[, subtype := fcase(
  grepl("law student|law school", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Professional associations",
  grepl("lawyer|lawyers|abogado", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Professional associations",
  grepl("bar assoc", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Professional associations",
  grepl("foundation", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Professional associations",
  grepl("legal defense", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Legal services & advocacy",
  grepl("centro legal|legal resource|law caucus|law alliance|law fund|auxilio legal|legal theory", irs_name_raw, ignore.case = TRUE, perl = TRUE),
  "Legal services & advocacy",
  default = "Professional associations"
)]

cat("Panethnic legal organizations identified:", nrow(pan), "\n")
cat("\nBreakdown by subtype:\n")
print(pan[, .N, by = subtype][order(-N)])
cat("\nBreakdown by subtype and group:\n")
print(dcast(pan[, .N, by = .(subtype, panethnic_group)],
  subtype ~ panethnic_group,
  value.var = "N", fill = 0L
)[order(-asian)])

cat("\nFull list:\n")
print(pan[
  order(subtype, panethnic_group, irs_name_raw),
  .(subtype, panethnic_group, irs_name_raw)
], nrows = Inf)

# ── Median IRS incorporation year ─────────────────────────────────────────────
# fnd_yr == 0 means IRS record has no founding year; exclude from median
pan_yr <- pan[fnd_yr > 0]
pan_yr[, fnd_yr := as.numeric(fnd_yr)]

cat(
  "\nOrgs with valid incorporation year:", nrow(pan_yr),
  "(excluded", nrow(pan) - nrow(pan_yr), "with fnd_yr == 0)\n"
)

summary_tbl <- pan_yr[, .(
  n_orgs        = .N,
  median_fnd_yr = median(fnd_yr),
  p25_fnd_yr    = quantile(fnd_yr, 0.25),
  p75_fnd_yr    = quantile(fnd_yr, 0.75)
), by = panethnic_group][order(panethnic_group)]

cat("\nMedian IRS incorporation year by group:\n")
print(summary_tbl)

# By subtype x group
subtype_tbl <- pan_yr[, .(
  n_orgs        = .N,
  median_fnd_yr = median(fnd_yr),
  p25_fnd_yr    = quantile(fnd_yr, 0.25),
  p75_fnd_yr    = quantile(fnd_yr, 0.75)
), by = .(subtype, panethnic_group)][order(subtype, panethnic_group)]

cat("\nMedian IRS incorporation year by subtype and group:\n")
print(subtype_tbl)

# Overall (asian + latino combined, excluding "both")
overall <- pan_yr[panethnic_group %in% c("asian", "latino"), .(
  panethnic_group = "overall",
  n_orgs          = .N,
  median_fnd_yr   = median(fnd_yr),
  p25_fnd_yr      = quantile(fnd_yr, 0.25),
  p75_fnd_yr      = quantile(fnd_yr, 0.75)
)]

summary_tbl <- rbind(summary_tbl, overall)

fwrite(summary_tbl, OUT_FILE)
cat("\nSummary saved:", OUT_FILE, "\n")

fwrite(
  pan[
    order(subtype, panethnic_group, irs_name_raw),
    .(subtype, panethnic_group, org_type, irs_name_raw, ein, fnd_yr)
  ],
  OUT_LIST_FILE
)
cat("Full list saved:", OUT_LIST_FILE, "\n")
