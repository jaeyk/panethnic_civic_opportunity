#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
})

parse_args <- function(args) {
  cfg <- list(
    matches = "processed_data/org_matching/org_to_irs_matches.csv",
    candidates = "processed_data/org_matching/similar_org_candidates.csv",
    about_pages = "processed_data/org_matching/candidate_about_pages.csv",
    reclassifications = "processed_data/org_matching/panethnic_constituency_reclass.csv",
    irs_org_activities = "raw_data/irs_data/irs_org_activities.csv",
    irs_nonweb_activities = "raw_data/irs_data/irs_nonweb_activities.csv",
    predictions = "raw_data/web_data/predictions.csv",
    out_dir = "processed_data/org_enriched",
    include_uncertain = FALSE
  )

  if (length(args) == 0) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% names(cfg)) {
      if (key == "include_uncertain") {
        cfg[[key]] <- tolower(val) %in% c("1", "true", "t", "yes", "y")
      } else {
        cfg[[key]] <- val
      }
    } else {
      stop(sprintf("Unknown argument: --%s", key))
    }
    i <- i + 2L
  }
  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))
normalize_fips <- function(x) {
  y <- gsub("[^0-9]", "", as.character(x))
  y[y == ""] <- NA_character_
  sprintf("%05s", y)
}

to_flag <- function(x) {
  y <- trimws(as.character(x))
  y[y %in% c("", "NA", "NaN", "NULL", "null")] <- NA_character_
  out <- suppressWarnings(as.numeric(y))
  out[!is.na(out)] <- ifelse(out[!is.na(out)] > 0, 1, 0)
  out
}

regex_detect <- function(x, patterns) {
  if (length(patterns) == 0) return(rep(FALSE, length(x)))
  p <- paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", patterns)), collapse = "|")
  grepl(p, tolower(x), perl = TRUE)
}

detect_panethnic_service_scope <- function(about_text) {
  txt <- tolower(as.character(about_text))
  txt[is.na(txt)] <- ""

  action_pat <- "(serve|serves|serving|support|supports|supporting|provide|provides|providing|advocate|advocates|advocating|organize|organizes|organizing|empower|empowers|empowering)"
  connector_pat <- "(.{0,80}\\b(for|to|with|among|within|across|in)\\b.{0,80})?"

  asian_target_pat <- "(asian\\s+american[s]?|aapi|api|asian[s]?|pacific\\s+islander[s]?|apida)"
  latino_target_pat <- "(latino[s]?|latina[s]?|latinx|hispanic[s]?|latine)"

  asian_scope <- grepl(
    paste0("\\b", action_pat, "\\b", connector_pat, ".{0,80}\\b", asian_target_pat, "\\b"),
    txt, perl = TRUE
  ) | grepl(
    paste0("\\b", asian_target_pat, "\\b.{0,80}\\b(community|communities|famil(y|ies)|residents|youth|people)\\b"),
    txt, perl = TRUE
  )

  latino_scope <- grepl(
    paste0("\\b", action_pat, "\\b", connector_pat, ".{0,80}\\b", latino_target_pat, "\\b"),
    txt, perl = TRUE
  ) | grepl(
    paste0("\\b", latino_target_pat, "\\b.{0,80}\\b(community|communities|famil(y|ies)|residents|youth|people)\\b"),
    txt, perl = TRUE
  )

  ifelse(asian_scope & latino_scope, "both",
  ifelse(asian_scope, "asian",
  ifelse(latino_scope, "latino", "uncertain")))
}

infer_panethnic_group <- function(name, about_text = NA_character_, service_scope_group = NULL) {
  if (is.null(service_scope_group)) {
    service_scope_group <- detect_panethnic_service_scope(about_text)
  }

  txt <- tolower(paste(name, about_text))

  asian_strong <- c(
    "asian", "asian american", "aapi", "asian pacific", "pacific islander",
    "chinese", "chinatown", "japanese", "korean", "filipino", "vietnamese",
    "hmong", "cambodian", "thai", "samoan", "little tokyo", "tokyo",
    "international district", "southeast asia", "on lok", "kai ming", "wu yee",
    "south cove", "maitri", "kalihi", "kokua", "tofa"
  )

  latino_strong <- c(
    "latino", "latina", "latinx", "hispanic", "hispanas", "hispanics", "la raza",
    "latin american", "latinoamericana", "mexican", "chicano", "chicana",
    "puerto rican", "cuban", "dominican", "salvadoran", "guatemalan", "colombian",
    "peruvian", "central american", "alianza", "asociacion", "tepeyac",
    "lulac", "aspira", "la casa", "la familia", "comunidades latinas",
    "chicanos por la causa", "bienestar", "cesar e chavez", "cesar chavez", "bomberos"
  )

  # Broad but weak indicators that should only matter when no strong signals exist.
  asian_weak <- c("api", "a a p i")
  latino_weak <- c("raza", "latinas", "spanish")

  a_strong <- regex_detect(txt, asian_strong)
  l_strong <- regex_detect(txt, latino_strong)
  a_weak <- regex_detect(txt, asian_weak)
  l_weak <- regex_detect(txt, latino_weak)

  a_score <- as.integer(a_strong) * 2L + as.integer(a_weak)
  l_score <- as.integer(l_strong) * 2L + as.integer(l_weak)

  out <- ifelse(service_scope_group %in% c("asian", "latino", "both"), service_scope_group,
         ifelse(a_score == 0 & l_score == 0, "uncertain",
         ifelse(a_score > l_score, "asian",
         ifelse(l_score > a_score, "latino", "both"))))
  out
}

load_seed_matches <- function(path) {
  dt <- fread(path, encoding = "UTF-8")
  if (!all(c("ein", "is_match", "source_group", "org_name_raw") %in% names(dt))) {
    stop("Seed match file is missing required columns: ein, is_match, source_group, org_name_raw")
  }

  dt <- dt[is_match == TRUE]
  dt[, ein := normalize_ein(ein)]
  dt[, origin := "seed_match"]
  dt[, panethnic_group := fifelse(source_group == "asian", "asian", "latino")]

  keep <- c("ein", "origin", "panethnic_group", "org_name_raw", "irs_name_raw", "irs_state", "irs_city", "preferred_link", "match_score")
  keep <- keep[keep %in% names(dt)]
  unique(dt[, ..keep])
}

load_candidate_matches <- function(candidates_path, about_path, include_uncertain) {
  cand <- fread(candidates_path, encoding = "UTF-8")
  if (!all(c("ein", "candidate_type", "irs_name_raw") %in% names(cand))) {
    stop("Candidate file is missing required columns: ein, candidate_type, irs_name_raw")
  }
  cand[, ein := normalize_ein(ein)]

  about_path_use <- about_path
  if (!file.exists(about_path_use)) {
    alt_about <- sub("candidate_about_pages\\.csv$", "candidate_about_pages_browser.csv", about_path_use)
    if (file.exists(alt_about)) {
      about_path_use <- alt_about
      message(sprintf("About-page file not found at %s; using %s", about_path, alt_about))
    }
  }

  if (file.exists(about_path_use)) {
    about <- fread(about_path_use, encoding = "UTF-8")
    if (all(c("ein", "about_page_text") %in% names(about))) {
      about[, ein := normalize_ein(ein)]
      cand <- merge(cand, unique(about[, .(ein, about_page_text)]), by = "ein", all.x = TRUE)
    }
  }
  if (!"about_page_text" %in% names(cand)) cand[, about_page_text := NA_character_]

  cand[, service_scope_group := detect_panethnic_service_scope(about_page_text)]
  cand[, inferred_group := infer_panethnic_group(irs_name_raw, about_page_text, service_scope_group)]
  cand[, origin := "expanded_candidate"]
  cand <- cand[candidate_type %in% c("direct_panethnic", "ethnic_named", "unique_or_neighbor")]

  if (!include_uncertain) {
    cand <- cand[inferred_group %in% c("asian", "latino", "both")]
  }

  cand[, panethnic_group := inferred_group]
  keep <- c(
    "ein", "origin", "panethnic_group", "irs_name_raw", "irs_state", "irs_city",
    "preferred_link", "candidate_type", "service_scope_group", "about_page_text",
    "inferred_group"
  )
  keep <- keep[keep %in% names(cand)]
  unique(cand[, ..keep])
}

apply_reclassifications <- function(cand, reclass_path) {
  if (!file.exists(reclass_path)) return(cand)

  rc <- fread(reclass_path, encoding = "UTF-8")
  if (!"ein" %in% names(rc)) return(cand)
  rc[, ein := normalize_ein(ein)]

  if ("reclass_group" %in% names(rc)) {
    rc[, reclass_group := tolower(trimws(as.character(reclass_group)))]
  } else if ("panethnic_group" %in% names(rc)) {
    rc[, reclass_group := tolower(trimws(as.character(panethnic_group)))]
  } else {
    return(cand)
  }

  rc <- rc[reclass_group %in% c("asian", "latino", "both")]
  if (nrow(rc) == 0L) return(cand)

  keep <- c("ein", "reclass_group", "reclass_confidence", "reclass_evidence_sentence")
  keep <- keep[keep %in% names(rc)]
  rc <- unique(rc[, ..keep], by = "ein")

  cand <- merge(cand, rc, by = "ein", all.x = TRUE)
  cand[, inferred_group_base := inferred_group]
  cand[, reclass_applied := as.integer(FALSE)]
  cand[
    candidate_type == "ethnic_named" & reclass_group %in% c("asian", "latino", "both"),
    `:=`(
      inferred_group = reclass_group,
      panethnic_group = reclass_group,
      reclass_applied = as.integer(TRUE)
    )
  ]
  cand
}

load_civic_features <- function(org_act_path, nonweb_path, pred_path) {
  org <- fread(org_act_path, encoding = "UTF-8")
  nonweb <- fread(nonweb_path, encoding = "UTF-8")
  pred <- fread(pred_path, encoding = "UTF-8")

  org[, ein := normalize_ein(ein)]
  nonweb[, ein := normalize_ein(ein)]
  if ("fips" %in% names(nonweb)) {
    nonweb[, irs_county_fips := normalize_fips(fips)]
  } else {
    nonweb[, irs_county_fips := NA_character_]
  }
  pred[, ein := normalize_ein(ein)]

  civic_cols <- c("take_action", "volunteer", "advocacy", "services", "membership", "events", "chapters", "board", "press", "donations", "resources")
  civic_cols <- civic_cols[civic_cols %in% names(org)]
  for (cc in civic_cols) org[, (cc) := to_flag(get(cc))]

  nonweb[, volunteer_nonweb := as.integer(!is.na(volunteer_text) & trimws(volunteer_text) != "")]
  nonweb[, member_nonweb := as.integer(!is.na(member_text) & trimws(member_text) != "")]

  pred_keep <- c("ein", "predicted", "group_predicted", "irs_class", "fnd_yr", "lat", "lng")
  pred_keep <- pred_keep[pred_keep %in% names(pred)]
  pred <- pred[, ..pred_keep]

  list(org = org, nonweb = nonweb[, .(ein, volunteer_nonweb, member_nonweb, irs_county_fips)], pred = pred)
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  message("Loading seed matches and expanded candidates...")
  seed <- load_seed_matches(cfg$matches)
  cand <- load_candidate_matches(cfg$candidates, cfg$about_pages, cfg$include_uncertain)
  cand <- apply_reclassifications(cand, cfg$reclassifications)

  universe <- rbindlist(list(seed, cand), fill = TRUE)
  universe <- unique(universe, by = c("ein", "origin", "panethnic_group"))

  message("Joining civic opportunity features and org type predictions...")
  fx <- load_civic_features(cfg$irs_org_activities, cfg$irs_nonweb_activities, cfg$predictions)

  setkey(universe, ein)
  setkey(fx$org, ein)
  setkey(fx$nonweb, ein)
  setkey(fx$pred, ein)

  out <- fx$org[universe]
  out <- fx$nonweb[out]
  out <- fx$pred[out]

  if (!"membership" %in% names(out)) out[, membership := NA_real_]
  if (!"volunteer" %in% names(out)) out[, volunteer := NA_real_]
  if (!"events" %in% names(out)) out[, events := NA_real_]
  if (!"take_action" %in% names(out)) out[, take_action := NA_real_]

  out[, membership_final := fifelse(!is.na(membership), membership, member_nonweb)]
  out[, volunteer_final := fifelse(!is.na(volunteer), volunteer, volunteer_nonweb)]
  out[, events_final := events]
  out[, civic_action_final := take_action]

  out[, civic_any := as.integer(
    pmax(
      fifelse(is.na(membership_final), 0, membership_final),
      fifelse(is.na(volunteer_final), 0, volunteer_final),
      fifelse(is.na(events_final), 0, events_final),
      fifelse(is.na(civic_action_final), 0, civic_action_final),
      fifelse(is.na(advocacy), 0, advocacy),
      fifelse(is.na(services), 0, services)
    ) > 0
  )]

  out[, org_type := fifelse(!is.na(predicted) & predicted != "", predicted,
                     fifelse(!is.na(group_predicted) & group_predicted != "", group_predicted, irs_class))]

  summary_by_group <- out[, .(
    org_n = uniqueN(ein),
    civic_any_rate = round(mean(civic_any, na.rm = TRUE), 4),
    membership_rate = round(mean(membership_final, na.rm = TRUE), 4),
    volunteer_rate = round(mean(volunteer_final, na.rm = TRUE), 4),
    events_rate = round(mean(events_final, na.rm = TRUE), 4),
    civic_action_rate = round(mean(civic_action_final, na.rm = TRUE), 4)
  ), by = .(panethnic_group, origin)]

  fwrite(out, file.path(cfg$out_dir, "org_civic_enriched.csv"))
  fwrite(summary_by_group, file.path(cfg$out_dir, "org_civic_enriched_summary.csv"))

  message(sprintf("Done. Enriched rows: %s", format(nrow(out), big.mark = ",")))
}

main()
