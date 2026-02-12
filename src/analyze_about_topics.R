#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    candidates = "processed_data/org_matching/similar_org_candidates.csv",
    about_pages = "processed_data/org_matching/candidate_about_pages.csv",
    safety_dict = "misc/safety_net_dictionary.csv",
    out_dir = "processed_data/topic_analysis"
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

grepl_in_chunks_progress <- function(pattern, x, chunk_size = 50000L, label = "scan") {
  n <- length(x)
  if (n == 0L) return(logical(0))
  out <- rep(FALSE, n)
  starts <- seq.int(1L, n, by = chunk_size)
  pb <- txtProgressBar(min = 0, max = n, style = 3)
  done <- 0L
  t0 <- Sys.time()
  for (s in starts) {
    e <- min(s + chunk_size - 1L, n)
    out[s:e] <- grepl(pattern, x[s:e], perl = TRUE)
    done <- e
    setTxtProgressBar(pb, done)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    rate <- done / pmax(elapsed, 1e-6)
    eta <- (n - done) / pmax(rate, 1e-6)
    message(sprintf("  %s: %s/%s (%.1f%%) | elapsed %.1fs | ETA %.1fs",
                    label, done, n, 100 * done / n, elapsed, eta))
  }
  close(pb)
  out
}

read_safety_dictionary <- function(path) {
  if (!file.exists(path)) {
    stop(sprintf("Safety-net dictionary not found: %s", path))
  }
  dict <- fread(path, encoding = "UTF-8")
  req <- c("program_group", "term")
  if (!all(req %in% names(dict))) {
    stop("safety_dict must include columns: program_group, term")
  }
  if (!"state" %in% names(dict)) dict[, state := ""]
  dict[, state := toupper(trimws(as.character(state)))]
  dict[, term := trimws(tolower(as.character(term)))]
  dict <- dict[!is.na(term) & term != ""]
  unique(dict, by = c("program_group", "term", "state"))
}

score_safety_programs <- function(dt, dict) {
  if (nrow(dt) == 0L) return(list(any = logical(0), counts = data.table(), matched = data.table()))
  out_any <- rep(FALSE, nrow(dt))

  # Program-level counts
  counts <- data.table(program_group = sort(unique(dict$program_group)))
  counts[, mention_n := 0L]
  pb <- txtProgressBar(min = 0, max = nrow(counts), style = 3)
  t0 <- Sys.time()

  for (i in seq_len(nrow(counts))) {
    pg <- counts$program_group[[i]]
    dsub <- dict[program_group == pg]
    terms_all <- unique(dsub[state == "" | is.na(state), term])

    flag <- rep(FALSE, nrow(dt))
    if (length(terms_all) > 0L) {
      pat <- make_pattern(terms_all)
      flag <- flag | grepl(pat, dt$text_clean, perl = TRUE)
    }

    states <- unique(dsub[state != "" & !is.na(state), state])
    if (length(states) > 0L) {
      for (st in states) {
        terms_st <- dsub[state == st, unique(term)]
        if (length(terms_st) == 0L) next
        idx <- which(dt$irs_state == st)
        if (length(idx) == 0L) next
        pat_st <- make_pattern(terms_st)
        flag[idx] <- flag[idx] | grepl(pat_st, dt$text_clean[idx], perl = TRUE)
      }
    }

    counts[program_group == pg, mention_n := sum(flag & dt$scrape_ok, na.rm = TRUE)]
    out_any <- out_any | flag
    setTxtProgressBar(pb, i)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    rate <- i / pmax(elapsed, 1e-6)
    eta <- (nrow(counts) - i) / pmax(rate, 1e-6)
    message(sprintf("  safety_program_scan: %s/%s (%.1f%%) | elapsed %.1fs | ETA %.1fs",
                    i, nrow(counts), 100 * i / nrow(counts), elapsed, eta))
  }
  close(pb)

  list(any = out_any, counts = counts)
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  cand <- fread(cfg$candidates, encoding = "UTF-8")
  about <- if (file.exists(cfg$about_pages)) fread(cfg$about_pages, encoding = "UTF-8") else data.table()

  if (!"ein" %in% names(cand)) stop("candidates file must contain ein")
  if (!"ein" %in% names(about)) about[, ein := NA_character_]
  cand[, ein := normalize_ein(ein)]
  about[, ein := normalize_ein(ein)]

  if (!"about_page_text" %in% names(about)) about[, about_page_text := NA_character_]

  dt <- merge(
    cand[, .(ein, irs_name_raw, candidate_type, irs_state, irs_city, preferred_link)],
    about[, .(ein, about_page_text)],
    by = "ein",
    all = TRUE
  )

  dt[, text_clean := tolower(trimws(as.character(about_page_text)))]
  dt[is.na(text_clean), text_clean := ""]
  dt[, irs_state := toupper(trimws(as.character(irs_state)))]
  dt[, scrape_ok := nchar(text_clean) > 0 & !grepl("^scrape_error:|does not have about page|is broken|is flat|php error", text_clean)]

  safety_dict <- read_safety_dictionary(cfg$safety_dict)
  message(sprintf("Scoring safety-net mentions with dictionary terms (%s rows)...", format(nrow(safety_dict), big.mark = ",")))
  scored <- score_safety_programs(dt, safety_dict)
  dt[, safety_net_mention := scored$any]

  democracy_terms <- c(
    "democracy", "democratic", "organizing", "community organizing", "grassroots", "grassroot",
    "power building", "build power", "collective action", "civic engagement", "civic participation",
    "leadership development", "movement building", "public action", "base building", "advocacy campaign"
  )

  democracy_pattern <- make_pattern(democracy_terms)

  message("Scanning democracy/organizing terms...")
  dt[, democracy_mention := grepl_in_chunks_progress(democracy_pattern, text_clean, label = "democracy_scan")]
  dt[, n_democracy_terms := lengths(regmatches(text_clean, gregexpr(democracy_pattern, text_clean, perl = TRUE)))]
  dt[, n_safety_terms := as.integer(safety_net_mention)]

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
  fwrite(scored$counts[order(-mention_n)], file.path(cfg$out_dir, "about_topic_safety_program_counts.csv"))
  fwrite(flagged, file.path(cfg$out_dir, "about_topic_flagged_orgs.csv"))
  fwrite(dt, file.path(cfg$out_dir, "about_topic_scored_all.csv"))

  message(sprintf("Done. Scraped usable pages: %s | safety-net mentions: %s | democracy mentions: %s",
                  format(summary_overall$scraped_ok_n[[1]], big.mark = ","),
                  format(summary_overall$safety_net_n[[1]], big.mark = ","),
                  format(summary_overall$democracy_n[[1]], big.mark = ",")))
}

main()
