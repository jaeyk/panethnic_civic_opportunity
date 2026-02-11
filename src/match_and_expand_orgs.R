#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(stringdist)
})

parse_args <- function(args) {
  defaults <- list(
    org_dir = "raw_data/org_data_ground_truth",
    irs_mbf = "raw_data/irs_data/irs_mbf.csv",
    irs_urls = "raw_data/irs_data/irs_urls.csv",
    irs_url_checks = "raw_data/irs_data/irs_url_checks.csv",
    out_dir = "processed_data/org_matching",
    max_org_rows = NA_integer_,
    max_irs_rows = NA_integer_,
    matching_method = "linkorgs",
    linkorgs_algorithm = "bipartite",
    fallback_to_fuzzy = TRUE,
    match_threshold = 0.90,
    target_match_rate = 0.90,
    scrape_about = FALSE,
    scrape_scope = "all_candidates",
    scrape_limit = 500L
  )

  if (length(args) == 0) return(defaults)

  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (!startsWith(key, "--")) {
      stop(sprintf("Unexpected argument: %s", key))
    }
    key <- sub("^--", "", key)
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% c("org_dir", "irs_mbf", "irs_urls", "irs_url_checks", "out_dir", "matching_method", "linkorgs_algorithm", "scrape_scope")) {
      defaults[[key]] <- val
    } else if (key %in% c("match_threshold", "target_match_rate")) {
      defaults[[key]] <- as.numeric(val)
    } else if (key %in% c("max_org_rows", "max_irs_rows")) {
      defaults[[key]] <- as.integer(val)
    } else if (key == "fallback_to_fuzzy") {
      defaults[[key]] <- tolower(val) %in% c("1", "true", "t", "yes", "y")
    } else if (key == "scrape_about") {
      defaults[[key]] <- tolower(val) %in% c("1", "true", "t", "yes", "y")
    } else if (key == "scrape_limit") {
      defaults[[key]] <- as.integer(val)
    } else {
      stop(sprintf("Unknown argument: --%s", key))
    }
    i <- i + 2L
  }

  defaults
}

normalize_name <- function(x) {
  x <- tolower(x)
  x <- gsub("[^a-z0-9 ]", " ", x)
  x <- gsub("\\b(incorporated|inc|llc|corp|corporation|co|company|foundation|assoc|association|society|organization|org|the)\\b", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

first_token <- function(x) {
  out <- sub(" .*", "", x)
  out[out == ""] <- NA_character_
  out
}

second_token <- function(x) {
  has_space <- grepl(" ", x, fixed = TRUE)
  out <- ifelse(has_space, sub("^[^ ]+ +", "", x), "")
  out <- sub(" .*", "", out)
  out[out == ""] <- NA_character_
  out
}

compute_similarity <- function(a, b) {
  jw <- 1 - stringdist(a, b, method = "jw", p = 0.1)
  lv <- 1 - (stringdist(a, b, method = "lv") / pmax(nchar(a), nchar(b), 1))
  pmax(jw, lv)
}

prepare_candidates <- function(org_row, irs_dt, fallback_n = 25000L) {
  st <- org_row[["state_norm"]]
  t1 <- org_row[["tok1"]]
  t2 <- org_row[["tok2"]]
  nm <- org_row[["org_name_norm"]]

  candidates <- irs_dt[irs_state == st & tok1 == t1]
  if (nrow(candidates) == 0L && !is.na(t2)) candidates <- irs_dt[irs_state == st & tok2 == t2]
  if (nrow(candidates) == 0L) candidates <- irs_dt[irs_state == st]
  if (nrow(candidates) == 0L) candidates <- irs_dt[tok1 == t1]
  if (nrow(candidates) == 0L && !is.na(t2)) candidates <- irs_dt[tok2 == t2]

  if (nrow(candidates) == 0L) {
    seed <- substr(nm, 1, 1)
    candidates <- irs_dt[substr(irs_name_norm, 1, 1) == seed]
  }

  if (nrow(candidates) == 0L) {
    candidates <- irs_dt[1:min(nrow(irs_dt), fallback_n)]
  }

  candidates
}

load_org_data <- function(org_dir, max_org_rows = NA_integer_) {
  asian_path <- file.path(org_dir, "asian_org.csv")
  latino_path <- file.path(org_dir, "latino_org.csv")

  if (!file.exists(asian_path) || !file.exists(latino_path)) {
    stop("Expected asian_org.csv and latino_org.csv in org_dir")
  }

  asian <- fread(asian_path, encoding = "UTF-8")
  latino <- fread(latino_path, encoding = "UTF-8")

  if (!"Name" %in% names(asian) || !"Name" %in% names(latino)) {
    stop("Org ground truth files must include a Name column")
  }

  asian[, source_group := "asian"]
  latino[, source_group := "latino"]

  org <- rbindlist(list(asian, latino), fill = TRUE)
  org[, org_id := .I]
  org[, org_name_raw := Name]
  org[, org_name_norm := normalize_name(org_name_raw)]
  org[, state_norm := toupper(trimws(States))]
  org[, tok1 := first_token(org_name_norm)]
  org[, tok2 := second_token(org_name_norm)]
  if (!is.na(max_org_rows) && max_org_rows > 0L && nrow(org) > max_org_rows) {
    org <- org[1:max_org_rows]
  }
  org
}

load_irs_data <- function(irs_mbf, irs_urls, irs_url_checks, max_irs_rows = NA_integer_) {
  nrows_opt <- if (!is.na(max_irs_rows) && max_irs_rows > 0L) max_irs_rows else Inf
  mbf <- fread(irs_mbf, select = c("ein", "name", "state", "city"), encoding = "UTF-8", nrows = nrows_opt)
  setnames(mbf, c("name", "state", "city"), c("irs_name_raw", "irs_state", "irs_city"))

  mbf[, ein := sprintf("%09s", gsub("[^0-9]", "", as.character(ein)))]
  mbf[, irs_name_norm := normalize_name(irs_name_raw)]
  mbf[, irs_state := toupper(trimws(irs_state))]
  mbf[, tok1 := first_token(irs_name_norm)]
  mbf[, tok2 := second_token(irs_name_norm)]

  urls <- fread(irs_urls, select = c("ein", "taxpayer_name", "preferred_link", "irs_url", "first_link"), encoding = "UTF-8", nrows = nrows_opt)
  urls[, ein := sprintf("%09s", gsub("[^0-9]", "", as.character(ein)))]

  checks <- fread(irs_url_checks, select = c("ein", "url_check", "preferred_link", "irs_url", "first_link"), encoding = "UTF-8", nrows = nrows_opt)
  checks[, ein := sprintf("%09s", gsub("[^0-9]", "", as.character(ein)))]

  setkey(mbf, ein)
  setkey(urls, ein)
  setkey(checks, ein)

  irs <- unique(mbf[urls], by = "ein")

  if (nrow(checks) > 0) {
    irs <- checks[irs]
  }

  irs[, preferred_link := fifelse(!is.na(preferred_link) & preferred_link != "", preferred_link,
                           fifelse(!is.na(i.preferred_link) & i.preferred_link != "", i.preferred_link,
                           fifelse(!is.na(irs_url) & irs_url != "", irs_url,
                           fifelse(!is.na(i.irs_url) & i.irs_url != "", i.irs_url,
                           fifelse(!is.na(first_link) & first_link != "", first_link, i.first_link)))))]

  irs[, c("i.preferred_link", "i.irs_url", "i.first_link") := NULL]
  if (!is.na(max_irs_rows) && max_irs_rows > 0L && nrow(irs) > max_irs_rows) {
    irs <- irs[1:max_irs_rows]
  }
  irs
}

best_match_one_fuzzy <- function(org_row, irs_dt, fallback_n = 25000L) {
  nm <- org_row[["org_name_norm"]]
  candidates <- prepare_candidates(org_row, irs_dt, fallback_n = fallback_n)
  scores <- compute_similarity(nm, candidates$irs_name_norm)
  idx <- which.max(scores)

  data.table(
    org_id = org_row[["org_id"]],
    ein = candidates$ein[[idx]],
    match_score = as.numeric(scores[[idx]]),
    candidate_n = nrow(candidates),
    matching_method = "fuzzy"
  )
}

extract_linkorgs_pairs <- function(link_res, by_x = "org_name_norm", by_y = "irs_name_norm") {
  if (is.null(link_res)) return(data.table())
  dt <- as.data.table(link_res)
  if (nrow(dt) == 0L) return(data.table())

  x_candidates <- c(by_x, "orgnames_x", "x", "name_x", "left", "x_name")
  y_candidates <- c(by_y, "orgnames_y", "y", "name_y", "right", "y_name")
  x_col <- x_candidates[x_candidates %in% names(dt)][1]
  y_col <- y_candidates[y_candidates %in% names(dt)][1]

  if (is.na(x_col) || is.na(y_col)) return(data.table())

  num_cols <- names(dt)[vapply(dt, is.numeric, logical(1))]
  score_candidates <- num_cols[grepl("prob|score|link|sim|dist", num_cols, ignore.case = TRUE)]
  score_col <- score_candidates[1]

  if (is.na(score_col)) {
    dt[, score_proxy := NA_real_]
    score_col <- "score_proxy"
  }

  out <- dt[, .(
    org_name_norm = get(x_col),
    irs_name_norm = get(y_col),
    link_score = suppressWarnings(as.numeric(get(score_col)))
  )]
  out
}

best_match_one_linkorgs <- function(org_row, irs_dt, linkorgs_algorithm = "bipartite", fallback_n = 25000L, fallback_to_fuzzy = TRUE) {
  nm <- org_row[["org_name_norm"]]
  candidates <- prepare_candidates(org_row, irs_dt, fallback_n = fallback_n)

  if (!requireNamespace("LinkOrgs", quietly = TRUE)) {
    if (fallback_to_fuzzy) return(best_match_one_fuzzy(org_row, irs_dt, fallback_n = fallback_n))
    stop("matching_method=linkorgs requested but LinkOrgs package is not installed.")
  }

  x <- data.frame(org_name_norm = nm, stringsAsFactors = FALSE)
  y <- data.frame(irs_name_norm = unique(candidates$irs_name_norm), stringsAsFactors = FALSE)

  link_res <- tryCatch(
    {
      LinkOrgs::LinkOrgs(
        x = x,
        y = y,
        by.x = "org_name_norm",
        by.y = "irs_name_norm",
        algorithm = linkorgs_algorithm
      )
    },
    error = function(e) NULL
  )

  pairs <- extract_linkorgs_pairs(link_res, by_x = "org_name_norm", by_y = "irs_name_norm")
  if (!all(c("org_name_norm", "irs_name_norm") %in% names(pairs)) || nrow(pairs) == 0L) {
    if (fallback_to_fuzzy) return(best_match_one_fuzzy(org_row, irs_dt, fallback_n = fallback_n))
    return(data.table(
      org_id = org_row[["org_id"]],
      ein = NA_character_,
      match_score = NA_real_,
      candidate_n = nrow(candidates),
      matching_method = "linkorgs"
    ))
  }
  pairs <- pairs[org_name_norm == nm]

  if (nrow(pairs) == 0L) {
    if (fallback_to_fuzzy) return(best_match_one_fuzzy(org_row, irs_dt, fallback_n = fallback_n))
    return(data.table(
      org_id = org_row[["org_id"]],
      ein = NA_character_,
      match_score = NA_real_,
      candidate_n = nrow(candidates),
      matching_method = "linkorgs"
    ))
  }

  if (all(is.na(pairs$link_score))) {
    pairs[, link_score := compute_similarity(nm, irs_name_norm)]
  }

  top_name <- pairs[which.max(link_score), irs_name_norm]
  cand2 <- candidates[irs_name_norm == top_name]
  if (nrow(cand2) == 0L) {
    if (fallback_to_fuzzy) return(best_match_one_fuzzy(org_row, irs_dt, fallback_n = fallback_n))
    return(data.table(
      org_id = org_row[["org_id"]],
      ein = NA_character_,
      match_score = NA_real_,
      candidate_n = nrow(candidates),
      matching_method = "linkorgs"
    ))
  }

  cand2[, sim2 := compute_similarity(nm, irs_name_norm)]
  chosen <- cand2[which.max(sim2)]
  top_score <- pairs[irs_name_norm == chosen$irs_name_norm[[1]], max(link_score, na.rm = TRUE)]
  if (!is.finite(top_score)) top_score <- chosen$sim2[[1]]

  data.table(
    org_id = org_row[["org_id"]],
    ein = chosen$ein[[1]],
    match_score = as.numeric(top_score),
    candidate_n = nrow(candidates),
    matching_method = "linkorgs"
  )
}

perform_matching <- function(org_dt, irs_dt, matching_method, linkorgs_algorithm, fallback_to_fuzzy, match_threshold, target_match_rate) {
  out <- rbindlist(lapply(seq_len(nrow(org_dt)), function(i) {
    if (tolower(matching_method) == "linkorgs") {
      best_match_one_linkorgs(
        org_row = org_dt[i],
        irs_dt = irs_dt,
        linkorgs_algorithm = linkorgs_algorithm,
        fallback_to_fuzzy = fallback_to_fuzzy
      )
    } else {
      best_match_one_fuzzy(org_row = org_dt[i], irs_dt = irs_dt)
    }
  }), use.names = TRUE, fill = TRUE)

  matched <- merge(org_dt, out, by = "org_id", all.x = TRUE)
  matched <- merge(matched, irs_dt[, .(ein, irs_name_raw, irs_name_norm, irs_state, irs_city, preferred_link, url_check)], by = "ein", all.x = TRUE)

  matched[, is_match := !is.na(match_score) & match_score >= match_threshold]

  match_rate <- mean(matched$is_match)
  status <- if (is.nan(match_rate)) FALSE else match_rate >= target_match_rate

  summary_dt <- data.table(
    org_n = nrow(matched),
    matched_n = sum(matched$is_match, na.rm = TRUE),
    unmatched_n = sum(!matched$is_match, na.rm = TRUE),
    match_rate = round(match_rate, 4),
    matching_method = matching_method,
    linkorgs_algorithm = linkorgs_algorithm,
    match_threshold = match_threshold,
    target_match_rate = target_match_rate,
    target_met = status
  )

  list(matched = matched, summary = summary_dt)
}

find_similar_org_candidates <- function(irs_dt, matched_dt) {
  seed_eins <- unique(matched_dt[is_match == TRUE & !is.na(ein), ein])
  pool <- irs_dt[!ein %in% seed_eins]

  if (nrow(pool) == 0L) {
    return(data.table())
  }

  pool[, name_norm := normalize_name(irs_name_raw)]

  direct_terms <- c(
    "asian", "asian american", "asian pacific", "aapi", "latino", "latina", "latinx", "hispanic", "la raza", "centro"
  )

  ethnic_terms <- c(
    "chinese", "chinatown", "japanese", "filipino", "korean", "vietnamese", "cambodian", "thai", "hmong", "samoan", "pacific islander",
    "mexican", "chicano", "chicana", "puerto rican", "cuban", "salvadoran", "guatemalan", "dominican", "colombian", "peruvian", "spanish"
  )

  seed_tokens <- unique(unlist(strsplit(paste(matched_dt$org_name_norm[matched_dt$is_match == TRUE], collapse = " "), " +")))
  stop_tokens <- c("and", "of", "the", "center", "community", "services", "association", "society", "coalition", "alliance", "network", "foundation", "project", "inc", "org")
  seed_tokens <- setdiff(seed_tokens[nchar(seed_tokens) >= 4], stop_tokens)

  direct_pattern <- paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", direct_terms)), collapse = "|")
  ethnic_pattern <- paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", ethnic_terms)), collapse = "|")

  pool[, direct_flag := grepl(direct_pattern, name_norm, ignore.case = TRUE)]
  pool[, ethnic_flag := grepl(ethnic_pattern, name_norm, ignore.case = TRUE)]

  if (length(seed_tokens) > 0) {
    token_pattern <- paste(sprintf("\\b%s\\b", gsub("([.|(){}+*?^$\\[\\]\\\\])", "\\\\\\1", seed_tokens)), collapse = "|")
    pool[, seed_token_flag := grepl(token_pattern, name_norm, ignore.case = TRUE)]
  } else {
    pool[, seed_token_flag := FALSE]
  }

  pool[, candidate_type := fifelse(direct_flag, "direct_panethnic",
                            fifelse(ethnic_flag, "ethnic_named", fifelse(seed_token_flag, "unique_or_neighbor", NA_character_)))]

  candidates <- pool[!is.na(candidate_type), .(
    ein, irs_name_raw, irs_state, irs_city, preferred_link, url_check,
    candidate_type, direct_flag, ethnic_flag, seed_token_flag
  )]

  unique(candidates)
}

scrape_about_pages <- function(candidate_dt, matched_dt, out_dir, scrape_scope, scrape_limit) {
  source("func/get_about_pages.R")

  scope <- tolower(scrape_scope)

  cand_all <- candidate_dt[!is.na(preferred_link) & preferred_link != "", .(
    ein, irs_name_raw, preferred_link, candidate_type
  )]

  cand_non_direct <- candidate_dt[
    candidate_type %in% c("ethnic_named", "unique_or_neighbor") &
      !is.na(preferred_link) & preferred_link != "",
    .(ein, irs_name_raw, preferred_link, candidate_type)
  ]

  seed_matched <- matched_dt[
    is_match == TRUE & !is.na(preferred_link) & preferred_link != "",
    .(ein, irs_name_raw, preferred_link)
  ]
  if (nrow(seed_matched) > 0L) seed_matched[, candidate_type := "seed_matched"]

  if (scope == "all_candidates") {
    to_scrape <- cand_all
  } else if (scope == "non_direct_only") {
    to_scrape <- cand_non_direct
  } else if (scope == "all_potential") {
    to_scrape <- rbindlist(list(cand_all, seed_matched), fill = TRUE)
  } else {
    stop("scrape_scope must be one of: all_candidates, non_direct_only, all_potential")
  }

  to_scrape <- unique(to_scrape, by = "ein")

  if (nrow(to_scrape) == 0L) {
    fwrite(data.table(), file.path(out_dir, "candidate_about_pages.csv"))
    return(invisible(NULL))
  }

  if (!is.na(scrape_limit) && scrape_limit > 0 && nrow(to_scrape) > scrape_limit) {
    to_scrape <- to_scrape[1:scrape_limit]
  }

  safe_get <- function(url) {
    tryCatch({
      out <- get_about_page_content(url)
      if (!"about_page" %in% names(out)) return(NA_character_)
      as.character(out$about_page[[1]])
    }, error = function(e) {
      paste("SCRAPE_ERROR:", conditionMessage(e))
    })
  }

  to_scrape[, about_page_text := vapply(preferred_link, safe_get, character(1))]
  fwrite(to_scrape, file.path(out_dir, "candidate_about_pages.csv"))
}

main <- function() {
  args <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(args$out_dir, recursive = TRUE, showWarnings = FALSE)

  message("Loading org and IRS data...")
  org <- load_org_data(args$org_dir, max_org_rows = args$max_org_rows)
  irs <- load_irs_data(args$irs_mbf, args$irs_urls, args$irs_url_checks, max_irs_rows = args$max_irs_rows)

  message("Running organization linkage (LinkOrgs-style with blocking)...")
  matched_res <- perform_matching(
    org_dt = org,
    irs_dt = irs,
    matching_method = args$matching_method,
    linkorgs_algorithm = args$linkorgs_algorithm,
    fallback_to_fuzzy = args$fallback_to_fuzzy,
    match_threshold = args$match_threshold,
    target_match_rate = args$target_match_rate
  )
  matched <- matched_res$matched
  summary_dt <- matched_res$summary

  message("Finding additional similar organizations by naming...")
  candidates <- find_similar_org_candidates(irs, matched)

  fwrite(summary_dt, file.path(args$out_dir, "match_summary.csv"))
  fwrite(matched, file.path(args$out_dir, "org_to_irs_matches.csv"))
  fwrite(matched[is_match == FALSE], file.path(args$out_dir, "org_to_irs_unmatched.csv"))
  fwrite(candidates, file.path(args$out_dir, "similar_org_candidates.csv"))

  if (isTRUE(args$scrape_about)) {
    message(sprintf("Scraping about pages (scope=%s)...", args$scrape_scope))
    scrape_about_pages(candidates, matched, args$out_dir, args$scrape_scope, args$scrape_limit)
  }

  message(sprintf("Done. Match rate: %.2f%% (target: %.2f%%)",
                  100 * summary_dt$match_rate[[1]],
                  100 * summary_dt$target_match_rate[[1]]))
}

main()
