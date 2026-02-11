#!/usr/bin/env Rscript

suppressPackageStartupMessages({ library(data.table) })
source("func/get_about_pages.R")

parse_args <- function(args) {
  cfg <- list(
    candidates = "outputs/org_matching/potential_asian_latino_orgs.csv",
    out_file = "outputs/org_matching/candidate_about_pages.csv",
    start_index = 1L,
    end_index = NA_integer_,
    batch_size = 100L,
    overwrite = FALSE
  )
  if (length(args) == 0) return(cfg)
  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    val <- args[[i + 1L]]
    if (key %in% c("candidates", "out_file")) cfg[[key]] <- val
    else if (key %in% c("start_index", "end_index", "batch_size")) cfg[[key]] <- as.integer(val)
    else if (key == "overwrite") cfg[[key]] <- tolower(val) %in% c("1","true","t","yes","y")
    else stop(sprintf("Unknown argument: --%s", key))
    i <- i + 2L
  }
  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))

safe_get_about <- function(url) {
  tryCatch({
    out <- get_about_page_content(url)
    if (!"about_page" %in% names(out)) return(NA_character_)
    txt <- as.character(out$about_page[[1]])
    txt <- gsub("[\r\n\t]+", " ", txt)
    txt <- gsub("\\s+", " ", txt)
    trimws(txt)
  }, error = function(e) {
    paste("SCRAPE_ERROR:", conditionMessage(e))
  })
}

shorten <- function(x, n = 80L) {
  x <- as.character(x)
  ifelse(nchar(x) > n, paste0(substr(x, 1, n - 3L), "..."), x)
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(dirname(cfg$out_file), recursive = TRUE, showWarnings = FALSE)

  cand <- fread(cfg$candidates, encoding = "UTF-8")
  if (!all(c("ein","preferred_link") %in% names(cand))) stop("candidates file must include ein and preferred_link")
  cand[, ein := normalize_ein(ein)]
  cand <- cand[!is.na(preferred_link) & preferred_link != ""]

  if (nrow(cand) == 0L) {
    fwrite(data.table(), cfg$out_file)
    message("No candidates with URLs.")
    return(invisible(NULL))
  }

  if (!is.na(cfg$end_index) && cfg$end_index > 0L) {
    cand <- cand[cfg$start_index:min(cfg$end_index, .N)]
  } else {
    cand <- cand[cfg$start_index:.N]
  }

  done <- data.table()
  if (file.exists(cfg$out_file) && !cfg$overwrite) {
    done <- fread(cfg$out_file, encoding = "UTF-8")
    if ("ein" %in% names(done)) done[, ein := normalize_ein(ein)]
  }

  if (nrow(done) > 0 && "ein" %in% names(done)) cand <- cand[!ein %in% done$ein]
  if (nrow(cand) == 0L) {
    message("No remaining candidates to scrape.")
    return(invisible(NULL))
  }

  n <- nrow(cand)
  chunks <- split(seq_len(n), ceiling(seq_len(n) / cfg$batch_size))
  pb <- txtProgressBar(min = 0, max = n, style = 3)
  completed <- 0L

  for (k in seq_along(chunks)) {
    idx <- chunks[[k]]
    part <- copy(cand[idx])
    scraped <- character(nrow(part))

    for (j in seq_len(nrow(part))) {
      ein_j <- part$ein[[j]]
      org_j <- shorten(part$irs_name_raw[[j]], 90L)
      url_j <- shorten(part$preferred_link[[j]], 100L)
      message(sprintf("Scraping [%s/%s] EIN=%s | %s | %s", completed + 1L, n, ein_j, org_j, url_j))
      scraped[[j]] <- safe_get_about(part$preferred_link[[j]])
      completed <- completed + 1L
      setTxtProgressBar(pb, completed)
    }

    part[, about_page_text := scraped]

    append_mode <- file.exists(cfg$out_file) && !cfg$overwrite
    if (append_mode) fwrite(part, cfg$out_file, append = TRUE, quote = TRUE)
    else {
      fwrite(part, cfg$out_file, quote = TRUE)
      cfg$overwrite <- FALSE
    }

    ok_n <- sum(!grepl("^SCRAPE_ERROR:", part$about_page_text) & !is.na(part$about_page_text) & nchar(part$about_page_text) > 0)
    message(sprintf("Completed batch %s/%s (rows %s-%s). Successful pages in batch: %s", k, length(chunks), min(idx), max(idx), ok_n))
  }
  close(pb)

  message(sprintf("Done scraping %s rows.", n))
}

main()
