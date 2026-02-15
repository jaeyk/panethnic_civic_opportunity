#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    input = "processed_data/org_enriched/org_civic_enriched.csv",
    dictionary = "misc/safety_net_dictionary.csv",
    output = "processed_data/org_enriched/org_civic_enriched.csv"
  )

  if (length(args) == 0L) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]
    if (!key %in% names(cfg)) stop(sprintf("Unknown argument: --%s", key))
    cfg[[key]] <- val
    i <- i + 2L
  }
  cfg
}

normalize_term <- function(x) {
  y <- trimws(tolower(as.character(x)))
  y <- gsub("\\s+", " ", y)
  y
}

make_pattern <- function(terms) {
  paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", normalize_term(terms))), collapse = "|")
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))

  dt <- fread(cfg$input, encoding = "UTF-8")
  req <- c("about_page_text", "irs_state")
  miss <- req[!req %in% names(dt)]
  if (length(miss) > 0L) {
    stop(sprintf("Input file is missing required columns: %s", paste(miss, collapse = ", ")))
  }

  dict <- fread(cfg$dictionary, encoding = "UTF-8")
  if (!all(c("term", "state") %in% names(dict))) {
    stop("Dictionary must include columns: term, state")
  }

  dict[, term := normalize_term(term)]
  dict <- dict[!is.na(term) & term != ""]
  dict[, state := toupper(trimws(as.character(state)))]

  dt[, text_clean := normalize_term(about_page_text)]
  dt[is.na(text_clean), text_clean := ""]
  dt[, irs_state := toupper(trimws(as.character(irs_state)))]

  safety <- rep(FALSE, nrow(dt))

  # Terms that apply to all states
  all_terms <- unique(dict[state == "" | is.na(state), term])
  if (length(all_terms) > 0L) {
    safety <- safety | grepl(make_pattern(all_terms), dt$text_clean, perl = TRUE)
  }

  # State-specific aliases
  st_list <- unique(dict[state != "" & !is.na(state), state])
  if (length(st_list) > 0L) {
    for (st in st_list) {
      idx <- which(dt$irs_state == st)
      if (length(idx) == 0L) next
      st_terms <- unique(dict[state == st, term])
      if (length(st_terms) == 0L) next
      safety[idx] <- safety[idx] | grepl(make_pattern(st_terms), dt$text_clean[idx], perl = TRUE)
    }
  }

  dt[, safety_net := as.integer(safety)]
  dt[, text_clean := NULL]

  fwrite(dt, cfg$output)

  cat(sprintf("Rows processed: %s\n", format(nrow(dt), big.mark = ",")))
  cat(sprintf("Safety-net flagged: %s (%.2f%%)\n",
              format(sum(dt$safety_net, na.rm = TRUE), big.mark = ","),
              100 * mean(dt$safety_net, na.rm = TRUE)))
  cat(sprintf("Wrote: %s\n", cfg$output))
}

main()
