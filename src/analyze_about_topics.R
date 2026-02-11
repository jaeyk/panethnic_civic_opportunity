#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    candidates = "outputs/org_matching/similar_org_candidates.csv",
    about_pages = "outputs/org_matching/candidate_about_pages.csv",
    out_dir = "outputs/topic_analysis"
  )

  if (length(args) == 0) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% names(cfg)) {
      cfg[[key]] <- val
    } else {
      stop(sprintf("Unknown argument: --%s", key))
    }
    i <- i + 2L
  }

  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))

make_pattern <- function(terms) {
  paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", tolower(terms))), collapse = "|")
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  cand <- fread(cfg$candidates, encoding = "UTF-8")
  about <- fread(cfg$about_pages, encoding = "UTF-8")

  cand[, ein := normalize_ein(ein)]
  about[, ein := normalize_ein(ein)]

  if (!"about_page_text" %in% names(about)) {
    stop("about_pages file must contain about_page_text")
  }

  dt <- merge(
    cand[, .(ein, irs_name_raw, candidate_type, irs_state, irs_city, preferred_link)],
    about[, .(ein, about_page_text)],
    by = "ein",
    all = TRUE
  )

  dt[, text_clean := tolower(trimws(as.character(about_page_text)))]
  dt[is.na(text_clean), text_clean := ""]
  dt[, scrape_ok := nchar(text_clean) > 0 & !grepl("^scrape_error:|does not have about page|is broken|is flat|php error", text_clean)]

  safety_terms <- c(
    "snap", "wic", "medicaid", "medicare", "chip", "tanf", "ssi", "ssdi",
    "rental assistance", "housing voucher", "section 8", "food stamps", "food assistance",
    "public benefits", "benefit enrollment", "cash assistance", "utility assistance",
    "senior care", "elder care", "in-home care", "caregiver support", "long term care"
  )

  democracy_terms <- c(
    "democracy", "democratic", "organizing", "community organizing", "grassroots", "grassroot",
    "power building", "build power", "collective action", "civic engagement", "civic participation",
    "leadership development", "movement building", "public action", "base building", "advocacy campaign"
  )

  safety_pattern <- make_pattern(safety_terms)
  democracy_pattern <- make_pattern(democracy_terms)

  dt[, safety_net_mention := grepl(safety_pattern, text_clean, perl = TRUE)]
  dt[, democracy_mention := grepl(democracy_pattern, text_clean, perl = TRUE)]

  dt[, n_safety_terms := lengths(regmatches(text_clean, gregexpr(safety_pattern, text_clean, perl = TRUE)))]
  dt[, n_democracy_terms := lengths(regmatches(text_clean, gregexpr(democracy_pattern, text_clean, perl = TRUE)))]

  summary_overall <- dt[, .(
    org_n = .N,
    scraped_ok_n = sum(scrape_ok, na.rm = TRUE),
    safety_net_n = sum(safety_net_mention & scrape_ok, na.rm = TRUE),
    democracy_n = sum(democracy_mention & scrape_ok, na.rm = TRUE),
    both_n = sum(safety_net_mention & democracy_mention & scrape_ok, na.rm = TRUE),
    safety_net_rate = round(mean(safety_net_mention[scrape_ok], na.rm = TRUE), 4),
    democracy_rate = round(mean(democracy_mention[scrape_ok], na.rm = TRUE), 4)
  )]

  summary_by_type <- dt[, .(
    org_n = .N,
    scraped_ok_n = sum(scrape_ok, na.rm = TRUE),
    safety_net_n = sum(safety_net_mention & scrape_ok, na.rm = TRUE),
    democracy_n = sum(democracy_mention & scrape_ok, na.rm = TRUE),
    both_n = sum(safety_net_mention & democracy_mention & scrape_ok, na.rm = TRUE)
  ), by = .(candidate_type)]

  flagged <- dt[scrape_ok == TRUE & (safety_net_mention | democracy_mention), .(
    ein, irs_name_raw, candidate_type, irs_state, irs_city, preferred_link,
    safety_net_mention, democracy_mention, n_safety_terms, n_democracy_terms
  )]

  fwrite(summary_overall, file.path(cfg$out_dir, "about_topic_summary_overall.csv"))
  fwrite(summary_by_type, file.path(cfg$out_dir, "about_topic_summary_by_candidate_type.csv"))
  fwrite(flagged, file.path(cfg$out_dir, "about_topic_flagged_orgs.csv"))
  fwrite(dt, file.path(cfg$out_dir, "about_topic_scored_all.csv"))

  message(sprintf("Done. Scraped usable pages: %s | safety-net mentions: %s | democracy mentions: %s",
                  format(summary_overall$scraped_ok_n[[1]], big.mark = ","),
                  format(summary_overall$safety_net_n[[1]], big.mark = ","),
                  format(summary_overall$democracy_n[[1]], big.mark = ",")))
}

main()
