#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    scored_input = "processed_data/topic_analysis/about_topic_scored_all.csv",
    dict_input = "misc/safety_net_dictionary.csv",
    out_csv = "outputs/analysis/safety_net_dictionary_candidates.csv",
    max_docs = 200000L,
    min_mentions = 8L
  )

  if (length(args) == 0L) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]
    if (!key %in% names(cfg)) stop(sprintf("Unknown argument: --%s", key))
    if (key %in% c("max_docs", "min_mentions")) {
      cfg[[key]] <- as.integer(val)
    } else {
      cfg[[key]] <- val
    }
    i <- i + 2L
  }
  cfg
}

normalize_term <- function(x) {
  y <- tolower(trimws(as.character(x)))
  y <- gsub("[^a-z0-9\\s\\-]", " ", y)
  y <- gsub("\\s+", " ", y)
  y
}

guess_program_group <- function(term) {
  if (grepl("\\b(snap|food stamps|calfresh|link card|ebt|nutrition)\\b", term, perl = TRUE)) return("SNAP/Nutrition")
  if (grepl("\\b(wic|women infants and children)\\b", term, perl = TRUE)) return("WIC")
  if (grepl("\\b(medicaid|medi-cal|apple health|soonercare|tenncare|masshealth|badgercare|husky|ahcccs)\\b", term, perl = TRUE)) return("Medicaid")
  if (grepl("\\b(chip|children s health insurance)\\b", term, perl = TRUE)) return("CHIP")
  if (grepl("\\b(tanf|cash assistance|temporary assistance)\\b", term, perl = TRUE)) return("Cash Assistance")
  if (grepl("\\b(section 8|housing choice voucher|public housing|rental assistance|housing assistance|eviction|homeless|shelter)\\b", term, perl = TRUE)) return("Housing Assistance")
  if (grepl("\\b(liheap|utility assistance|energy assistance)\\b", term, perl = TRUE)) return("Utility Assistance")
  if (grepl("\\b(food pantry|food bank|meal program|nutrition)\\b", term, perl = TRUE)) return("Nutrition Assistance")
  if (grepl("\\b(caregiver|elder care|senior care|long term care|in-home care)\\b", term, perl = TRUE)) return("Senior Care")
  if (grepl("\\b(legal aid|case management|benefits navigation|emergency assistance)\\b", term, perl = TRUE)) return("Cross-cutting Support")
  "Unassigned"
}

extract_terms <- function(txt) {
  if (is.na(txt) || txt == "") return(character(0))

  patterns <- c(
    "\\b[a-z][a-z\\-']*(?:\\s+[a-z][a-z\\-']*){0,4}\\s+(?:assistance|aid|benefits?|voucher|subsid(?:y|ies)|insurance|allowance|support)\\b",
    "\\b(?:section\\s*8|housing choice voucher|public housing|rental assistance|housing assistance|food pantry|food bank|cash assistance|temporary assistance for needy families|benefits navigation|legal aid|case management|emergency assistance|medicaid|chip|snap|wic|liheap|tanf|medi-cal|apple health|soonercare|tenncare|masshealth|badgercare|husky health|ahcccs)\\b",
    "\\b[a-z][a-z\\-']*(?:\\s+[a-z][a-z\\-']*){0,3}\\s+(?:program|services?)\\b"
  )

  out <- character(0)
  for (p in patterns) {
    m <- gregexpr(p, txt, perl = TRUE)
    r <- regmatches(txt, m)[[1]]
    if (length(r) > 0L && !(length(r) == 1L && r[1] == "-1")) {
      out <- c(out, r)
    }
  }
  unique(normalize_term(out))
}

is_useful_candidate <- function(term) {
  positive_anchor <- grepl(
    "\\b(medicaid|medi-cal|apple health|soonercare|tenncare|masshealth|badgercare|husky health|ahcccs|snap|food stamps|calfresh|ebt|wic|chip|tanf|liheap|food pantry|food bank|housing voucher|section 8|public housing|rental assistance|housing assistance|utility assistance|energy assistance|legal aid|case management|benefits navigation|emergency assistance|eviction|homeless|shelter|caregiver|in-home care|long term care|nutrition assistance)\\b",
    term, perl = TRUE
  )

  boilerplate <- grepl(
    "\\b(terms of service|program service|total support|public support|professional fundraising|value of services|employee benefits|goods or services|excess benefit|senate|amicus brief|armed services|recognition|this program|the program|support)\\b",
    term, perl = TRUE
  )

  positive_anchor && !boilerplate
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(cfg$out_csv), recursive = TRUE, showWarnings = FALSE)

  scored <- fread(cfg$scored_input, encoding = "UTF-8", select = c("ein", "irs_state", "text_clean", "scrape_ok"))
  scored[, scrape_ok := as.logical(scrape_ok)]
  scored <- scored[scrape_ok == TRUE & !is.na(text_clean) & nchar(text_clean) > 0]
  if (nrow(scored) > cfg$max_docs) scored <- scored[1:cfg$max_docs]
  scored[, ein := sprintf("%09s", gsub("[^0-9]", "", as.character(ein)))]

  dict <- fread(cfg$dict_input, encoding = "UTF-8")
  existing <- unique(normalize_term(dict$term))
  existing <- existing[existing != "" & !is.na(existing)]

  cue_pat <- "\\b(assistance|benefits?|voucher|medicaid|snap|wic|chip|tanf|liheap|housing|rental|food pantry|food bank|utility|case management|legal aid|shelter|homeless|caregiver|in-home care)\\b"
  scored <- scored[grepl(cue_pat, text_clean, perl = TRUE)]

  message(sprintf("Mining candidate terms from %s documents...", format(nrow(scored), big.mark = ",")))

  all_terms <- vector("list", nrow(scored))
  pb <- txtProgressBar(min = 0, max = nrow(scored), style = 3)
  for (i in seq_len(nrow(scored))) {
    all_terms[[i]] <- extract_terms(scored$text_clean[[i]])
    if (i %% 1000L == 0L || i == nrow(scored)) setTxtProgressBar(pb, i)
  }
  close(pb)

  out <- rbindlist(lapply(seq_along(all_terms), function(i) {
    t <- all_terms[[i]]
    if (length(t) == 0L) return(NULL)
    data.table(ein = scored$ein[[i]], irs_state = scored$irs_state[[i]], term = t)
  }), fill = TRUE)

  if (nrow(out) == 0L) {
    fwrite(data.table(), cfg$out_csv)
    message("No candidate terms found.")
    return(invisible(NULL))
  }

  out <- out[!term %in% existing]
  out <- out[nchar(term) >= 4 & nchar(term) <= 80]
  out <- out[vapply(term, is_useful_candidate, logical(1))]

  summary <- out[, .(
    mention_n = .N,
    org_n = uniqueN(ein),
    sample_state = first(irs_state)
  ), by = term][order(-mention_n, -org_n)]

  summary <- summary[mention_n >= cfg$min_mentions]
  summary[, suggested_program_group := vapply(term, guess_program_group, character(1))]
  setcolorder(summary, c("suggested_program_group", "term", "mention_n", "org_n", "sample_state"))

  fwrite(summary, cfg$out_csv)
  message(sprintf("Wrote %s candidate terms to %s", format(nrow(summary), big.mark = ","), cfg$out_csv))
}

main()
