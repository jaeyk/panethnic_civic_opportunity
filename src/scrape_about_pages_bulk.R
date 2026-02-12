#!/usr/bin/env Rscript

suppressPackageStartupMessages({ library(data.table) })
source("func/get_about_pages.R")

parse_args <- function(args) {
  cfg <- list(
    candidates = "processed_data/org_matching/potential_asian_latino_orgs.csv",
    out_file = "processed_data/org_matching/candidate_about_pages.csv",
    start_index = 1L,
    end_index = NA_integer_,
    batch_size = 100L,
    overwrite = FALSE,
    timeout_thres = 30L,
    retries = 2L,
    retry_wait_sec = 2L,
    min_request_sec = 0L,
    early_fail_check_n = 50L
  )
  if (length(args) == 0) return(cfg)
  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    val <- args[[i + 1L]]
    if (key %in% c("candidates", "out_file")) cfg[[key]] <- val
    else if (key %in% c("start_index", "end_index", "batch_size", "timeout_thres", "retries", "retry_wait_sec", "min_request_sec", "early_fail_check_n")) cfg[[key]] <- as.integer(val)
    else if (key == "overwrite") cfg[[key]] <- tolower(val) %in% c("1","true","t","yes","y")
    else stop(sprintf("Unknown argument: --%s", key))
    i <- i + 2L
  }
  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))

classify_status <- function(txt) {
  t <- tolower(trimws(as.character(txt)))
  if (is.na(t) || t == "") return("empty")
  if (grepl("^scrape_error:", t)) return("error")
  if (grepl("does not have about page|is broken|is flat|php error", t)) return("no_about")
  "success"
}

safe_get_about <- function(url, timeout_thres = 30L, retries = 2L, retry_wait_sec = 2L, min_request_sec = 0L) {
  attempts <- max(1L, as.integer(retries) + 1L)
  last_txt <- NA_character_
  last_status <- "error"
  elapsed_total <- 0

  for (a in seq_len(attempts)) {
    t0 <- Sys.time()
    txt <- tryCatch({
      out <- get_about_page_content(url, timeout_thres = timeout_thres)
      if (!"about_page" %in% names(out)) return(NA_character_)
      y <- as.character(out$about_page[[1]])
      y <- gsub("[\r\n\t]+", " ", y)
      y <- gsub("\\s+", " ", y)
      trimws(y)
    }, error = function(e) {
      paste("SCRAPE_ERROR:", conditionMessage(e))
    })
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    elapsed_total <- elapsed_total + elapsed
    if (min_request_sec > 0 && elapsed < min_request_sec) Sys.sleep(min_request_sec - elapsed)

    st <- classify_status(txt)
    last_txt <- txt
    last_status <- st

    is_transient_error <- st == "error" && grepl("timeout|timed out|resolve host|connection|ssl", tolower(txt))
    if (!is_transient_error || a == attempts) break
    if (retry_wait_sec > 0) Sys.sleep(retry_wait_sec)
  }

  list(
    about_page_text = last_txt,
    scrape_status = last_status,
    request_sec = round(elapsed_total, 2),
    attempts = attempts
  )
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
  ok_total <- 0L
  err_total <- 0L
  no_about_total <- 0L

  for (k in seq_along(chunks)) {
    idx <- chunks[[k]]
    part <- copy(cand[idx])
    scraped <- character(nrow(part))
    statuses <- character(nrow(part))
    req_sec <- numeric(nrow(part))
    tries <- integer(nrow(part))

    for (j in seq_len(nrow(part))) {
      ein_j <- part$ein[[j]]
      org_j <- shorten(part$irs_name_raw[[j]], 90L)
      url_j <- shorten(part$preferred_link[[j]], 100L)
      message(sprintf("Scraping [%s/%s] EIN=%s | %s | %s", completed + 1L, n, ein_j, org_j, url_j))
      res <- safe_get_about(
        part$preferred_link[[j]],
        timeout_thres = cfg$timeout_thres,
        retries = cfg$retries,
        retry_wait_sec = cfg$retry_wait_sec,
        min_request_sec = cfg$min_request_sec
      )
      scraped[[j]] <- res$about_page_text
      statuses[[j]] <- res$scrape_status
      req_sec[[j]] <- res$request_sec
      tries[[j]] <- res$attempts
      completed <- completed + 1L
      setTxtProgressBar(pb, completed)

      if (res$scrape_status == "success") ok_total <- ok_total + 1L
      else if (res$scrape_status == "no_about") no_about_total <- no_about_total + 1L
      else err_total <- err_total + 1L

      message(sprintf(
        "  status=%s | req=%.1fs | ok=%s err=%s no_about=%s",
        res$scrape_status, res$request_sec,
        format(ok_total, big.mark = ","), format(err_total, big.mark = ","), format(no_about_total, big.mark = ",")
      ))
    }

    part[, about_page_text := scraped]
    part[, scrape_status := statuses]
    part[, request_sec := req_sec]
    part[, attempts := tries]

    append_mode <- file.exists(cfg$out_file) && !cfg$overwrite
    if (append_mode) fwrite(part, cfg$out_file, append = TRUE, quote = TRUE)
    else {
      fwrite(part, cfg$out_file, quote = TRUE)
      cfg$overwrite <- FALSE
    }

    ok_n <- sum(part$scrape_status == "success", na.rm = TRUE)
    err_n <- sum(part$scrape_status == "error", na.rm = TRUE)
    no_about_n <- sum(part$scrape_status == "no_about", na.rm = TRUE)
    message(sprintf("Completed batch %s/%s (rows %s-%s). success=%s error=%s no_about=%s", k, length(chunks), min(idx), max(idx), ok_n, err_n, no_about_n))

    # Guardrail: if early slice is almost all errors, abort instead of silently finishing quickly.
    if (completed >= cfg$early_fail_check_n && ok_total == 0L) {
      err_rate <- err_total / completed
      if (err_rate >= 0.98) {
        close(pb)
        stop(sprintf(
          "Early stop: %s/%s first requests are errors (%.1f%%). Scraping appears to be failing fast.",
          err_total, completed, 100 * err_rate
        ))
      }
    }
  }
  close(pb)

  message(sprintf(
    "Done scraping %s rows. success=%s error=%s no_about=%s",
    n, format(ok_total, big.mark = ","), format(err_total, big.mark = ","), format(no_about_total, big.mark = ",")
  ))
}

main()
