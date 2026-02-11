#!/usr/bin/env Rscript

suppressPackageStartupMessages({ library(data.table) })

parse_args <- function(args) {
  cfg <- list(
    irs_mbf = "raw_data/irs_data/irs_mbf.csv",
    irs_urls = "raw_data/irs_data/irs_urls.csv",
    out_file = "outputs/org_matching/potential_asian_latino_orgs.csv"
  )
  if (length(args) == 0) return(cfg)
  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    val <- args[[i + 1L]]
    if (!key %in% names(cfg)) stop(sprintf("Unknown argument: --%s", key))
    cfg[[key]] <- val
    i <- i + 2L
  }
  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(cfg$out_file), recursive = TRUE, showWarnings = FALSE)

  mbf <- fread(cfg$irs_mbf, select = c("ein", "name", "state", "city"), encoding = "UTF-8")
  urls <- fread(cfg$irs_urls, select = c("ein", "preferred_link", "irs_url", "first_link"), encoding = "UTF-8")

  mbf[, ein := normalize_ein(ein)]
  urls[, ein := normalize_ein(ein)]

  dt <- merge(mbf, urls, by = "ein", all.x = TRUE)
  dt[, preferred_link := fifelse(!is.na(preferred_link) & preferred_link != "", preferred_link,
                          fifelse(!is.na(irs_url) & irs_url != "", irs_url, first_link))]

  nm <- tolower(dt$name)
  direct_pattern <- "\\b(asian|asian american|asian pacific|aapi|latino|latina|latinx|hispanic|la raza|centro)\\b"
  ethnic_pattern <- "\\b(chinese|chinatown|japanese|filipino|korean|vietnamese|cambodian|thai|hmong|samoan|pacific islander|mexican|chicano|chicana|puerto rican|cuban|salvadoran|guatemalan|dominican|colombian|peruvian|spanish)\\b"

  dt[, direct_flag := grepl(direct_pattern, nm, perl = TRUE)]
  dt[, ethnic_flag := grepl(ethnic_pattern, nm, perl = TRUE)]
  dt[, candidate_type := fifelse(direct_flag, "direct_panethnic", fifelse(ethnic_flag, "ethnic_named", NA_character_))]

  out <- dt[!is.na(candidate_type) & !is.na(preferred_link) & preferred_link != "", .(
    ein,
    irs_name_raw = name,
    irs_state = state,
    irs_city = city,
    preferred_link,
    candidate_type,
    direct_flag,
    ethnic_flag
  )]

  out <- unique(out, by = "ein")
  fwrite(out, cfg$out_file)
  message(sprintf("Wrote %s potential orgs to %s", format(nrow(out), big.mark = ","), cfg$out_file))
}

main()
