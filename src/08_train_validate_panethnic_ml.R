#!/usr/bin/env Rscript

# =============================================================================
# LAST RUN PERFORMANCE  (2026-06-26)
#
# Task 1 — Asian vs. Latino Group Classifier  |  5-fold CV
# Input  : 818 ground-truth orgs  (299 Asian, 519 Latino)
#   Model         Accuracy  Bal. Acc  Macro-F1  AUC
#   xgboost       0.965     0.956     0.961     0.996   ← best
#   ranger        0.961     0.948     0.957     0.991
#   superlearner  0.951     0.935     0.946     0.995
#   glmnet        0.763     0.676     0.681     0.967
#   Best model  : xgboost
#   Pass ML filter (conf≥0.70, margin≥0.15): 10,764 / 12,677 candidates
#
# Task 2 — Panethnic vs. Ethnic Classifier  |  5-fold CV
# Positive (panethnic=1): 818 ground-truth orgs
# Negative (panethnic=0): 2,454 ethnic-named IRS candidates not reclassified
#   Ensemble (avg glmnet+ranger+xgb) | 5-fold CV
#   Accuracy  Bal. Acc  Macro-F1  AUC
#   0.966     0.941     0.954     0.992
#   Scored labels: panethnic=1,870  ethnic=8,976  uncertain=1,831
#   Output → processed_data/ml_validation/candidate_panethnic_predictions.csv
# =============================================================================

suppressPackageStartupMessages({
  library(data.table)
  library(Matrix)
  library(glmnet)
  library(ranger)
  library(xgboost)
  library(SuperLearner)
  library(ggplot2)
})

parse_args <- function(args) {
  cfg <- list(
    asian_input = "raw_data/org_data_ground_truth/asian_org.csv",
    latino_input = "raw_data/org_data_ground_truth/latino_org.csv",
    matches_input = "processed_data/org_matching/org_to_irs_matches.csv",
    about_input = "processed_data/org_matching/candidate_about_pages.csv",
    candidates_input = "processed_data/org_matching/potential_asian_latino_orgs.csv",
    out_dir = "processed_data/ml_validation",
    folds = 5L,
    seed = 42L,
    min_df = 2L,
    max_features = 3000L,
    confidence_threshold = 0.70,
    margin_threshold = 0.15
  )

  if (length(args) == 0) return(cfg)

  i <- 1L
  while (i <= length(args)) {
    key <- sub("^--", "", args[[i]])
    if (i == length(args)) stop(sprintf("Missing value for --%s", key))
    val <- args[[i + 1L]]

    if (key %in% c("asian_input", "latino_input", "matches_input", "about_input", "candidates_input", "out_dir")) cfg[[key]] <- val
    else if (key %in% c("folds", "seed", "min_df", "max_features")) cfg[[key]] <- as.integer(val)
    else if (key %in% c("confidence_threshold", "margin_threshold")) cfg[[key]] <- as.numeric(val)
    else stop(sprintf("Unknown argument: --%s", key))

    i <- i + 2L
  }

  cfg
}

normalize_ein <- function(x) sprintf("%09s", gsub("[^0-9]", "", as.character(x)))

clean_text <- function(x) {
  x <- tolower(as.character(x))
  x <- gsub("[^a-z0-9 ]", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

tokenize <- function(text) {
  if (is.na(text) || text == "") return(character(0))
  t <- unlist(strsplit(clean_text(text), " +"))
  t <- t[nchar(t) >= 2]
  stopwords <- c("the","and","for","of","to","in","a","an","on","at","by","with","from","is","are","be","inc","llc","org","organization","community","center")
  t[!t %in% stopwords]
}

build_vocab <- function(texts, min_df = 2L, max_features = 3000L) {
  df <- new.env(parent = emptyenv())
  for (txt in texts) {
    toks <- unique(tokenize(txt))
    for (tk in toks) df[[tk]] <- if (!exists(tk, envir = df, inherits = FALSE)) 1L else df[[tk]] + 1L
  }
  keys <- ls(df)
  vals <- vapply(keys, function(k) get(k, envir = df, inherits = FALSE), integer(1))
  dt <- data.table(term = keys, df = vals)
  dt <- dt[df >= min_df][order(-df)]
  if (nrow(dt) > max_features) dt <- dt[1:max_features]
  dt$term
}

build_dtm <- function(texts, vocab, idf = NULL) {
  term_index <- setNames(seq_along(vocab), vocab)
  i_idx <- integer(0)
  j_idx <- integer(0)
  x_val <- numeric(0)

  for (r in seq_along(texts)) {
    toks <- tokenize(texts[[r]])
    toks <- toks[toks %in% vocab]
    if (length(toks) == 0) next
    tab <- table(toks)
    cols <- term_index[names(tab)]
    vals <- as.numeric(tab)

    i_idx <- c(i_idx, rep.int(r, length(cols)))
    j_idx <- c(j_idx, as.integer(cols))
    x_val <- c(x_val, vals)
  }

  X <- sparseMatrix(i = i_idx, j = j_idx, x = x_val, dims = c(length(texts), length(vocab)))

  # tf-idf transform
  rs <- rowSums(X)
  rs[rs == 0] <- 1
  X <- Diagonal(x = 1 / rs) %*% X

  if (is.null(idf)) {
    df <- colSums(X > 0)
    idf <- log((nrow(X) + 1) / (df + 1)) + 1
  }

  X <- X %*% Diagonal(x = idf)
  list(X = X, idf = idf)
}

stratified_folds <- function(y, k = 5L, seed = 42L) {
  set.seed(seed)
  idx1 <- which(y == 1L)
  idx0 <- which(y == 0L)
  idx1 <- sample(idx1)
  idx0 <- sample(idx0)

  f <- integer(length(y))
  f[idx1] <- rep(1:k, length.out = length(idx1))
  f[idx0] <- rep(1:k, length.out = length(idx0))
  f
}

roc_auc_fast <- function(y, p) {
  n1 <- sum(y == 1)
  n0 <- sum(y == 0)
  if (n1 == 0 || n0 == 0) return(NA_real_)
  r <- rank(p)
  (sum(r[y == 1]) - n1 * (n1 + 1) / 2) / (n1 * n0)
}

metrics_binary <- function(y_true, p_asian, threshold = 0.5) {
  y_hat <- as.integer(p_asian >= threshold)
  acc <- mean(y_hat == y_true)

  tp1 <- sum(y_hat == 1 & y_true == 1)
  fp1 <- sum(y_hat == 1 & y_true == 0)
  fn1 <- sum(y_hat == 0 & y_true == 1)

  tp0 <- sum(y_hat == 0 & y_true == 0)
  fp0 <- sum(y_hat == 0 & y_true == 1)
  fn0 <- sum(y_hat == 1 & y_true == 0)

  p1 <- ifelse(tp1 + fp1 == 0, NA, tp1 / (tp1 + fp1))
  r1 <- ifelse(tp1 + fn1 == 0, NA, tp1 / (tp1 + fn1))
  f1_1 <- ifelse(is.na(p1) || is.na(r1) || p1 + r1 == 0, NA, 2 * p1 * r1 / (p1 + r1))

  p0 <- ifelse(tp0 + fp0 == 0, NA, tp0 / (tp0 + fp0))
  r0 <- ifelse(tp0 + fn0 == 0, NA, tp0 / (tp0 + fn0))
  f1_0 <- ifelse(is.na(p0) || is.na(r0) || p0 + r0 == 0, NA, 2 * p0 * r0 / (p0 + r0))

  macro_f1         <- mean(c(f1_0, f1_1), na.rm = TRUE)
  balanced_accuracy <- mean(c(r1, r0),   na.rm = TRUE)
  eps <- 1e-8
  ll  <- -mean(y_true * log(pmax(pmin(p_asian, 1 - eps), eps)) +
               (1 - y_true) * log(pmax(pmin(1 - p_asian, 1 - eps), eps)))
  auc <- roc_auc_fast(y_true, p_asian)

  data.table(accuracy = acc, balanced_accuracy = balanced_accuracy,
             macro_f1 = macro_f1, log_loss = ll, auc = auc)
}

fit_predict_glmnet <- function(Xtr, ytr, Xte) {
  m <- cv.glmnet(Xtr, ytr, family = "binomial", type.measure = "auc", nfolds = 5)
  as.numeric(predict(m, newx = Xte, s = "lambda.1se", type = "response"))
}

fit_predict_ranger <- function(Xtr, ytr, Xte) {
  Xtr_df <- as.data.frame(as.matrix(Xtr))
  Xte_df <- as.data.frame(as.matrix(Xte))
  yfac <- factor(ifelse(ytr == 1, "asian", "latino"), levels = c("latino", "asian"))
  m <- ranger::ranger(y = yfac, x = Xtr_df, probability = TRUE, num.trees = 400, respect.unordered.factors = TRUE)
  pr <- predict(m, data = Xte_df)$predictions
  as.numeric(pr[, "asian"])
}

fit_predict_xgb <- function(Xtr, ytr, Xte) {
  dtr <- xgboost::xgb.DMatrix(data = Xtr, label = ytr)
  dte <- xgboost::xgb.DMatrix(data = Xte)
  m <- xgboost::xgb.train(
    params = list(objective = "binary:logistic", eval_metric = "logloss", eta = 0.1, max_depth = 6, subsample = 0.8, colsample_bytree = 0.8),
    data = dtr,
    nrounds = 180,
    verbose = 0
  )
  as.numeric(predict(m, dte))
}

choose_sl_library <- function() {
  wrappers <- tryCatch(SuperLearner::listWrappers("SL"), error = function(e) NULL)
  if (is.null(wrappers)) wrappers <- tryCatch(SuperLearner::listWrappers(), error = function(e) character(0))
  if (is.list(wrappers) && "SL" %in% names(wrappers)) wrappers <- wrappers$SL
  wrappers <- as.character(wrappers)
  target <- c("SL.glmnet", "SL.ranger", "SL.xgboost")
  libs <- intersect(target, wrappers)
  if (length(libs) == 0L) libs <- c("SL.mean")
  libs
}

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)
  total_steps <- as.integer(cfg$folds + 10L)
  pb <- txtProgressBar(min = 0, max = total_steps, style = 3)
  step <- 0L
  t0 <- Sys.time()
  tick <- function(label) {
    step <<- step + 1L
    setTxtProgressBar(pb, step)
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
    rate <- step / pmax(elapsed, 1e-6)
    eta <- (total_steps - step) / pmax(rate, 1e-6)
    message(sprintf("  %s | step %s/%s | elapsed %.1fs | ETA %.1fs", label, step, total_steps, elapsed, eta))
  }
  message("Phase 01d (SuperLearner): loading data...")

  asian <- fread(cfg$asian_input, encoding = "UTF-8")
  latino <- fread(cfg$latino_input, encoding = "UTF-8")
  if (!"Name" %in% names(asian) || !"Name" %in% names(latino)) stop("Ground truth files must contain Name")

  asian[, true_group := "asian"]
  latino[, true_group := "latino"]
  gt <- rbindlist(list(asian, latino), fill = TRUE)
  gt[, org_name := Name]

  # Optional enrichment with scraped about text for matched rows.
  if (file.exists(cfg$matches_input) && file.exists(cfg$about_input)) {
    mt <- fread(cfg$matches_input, encoding = "UTF-8")
    ab <- fread(cfg$about_input, encoding = "UTF-8")
    if (all(c("org_name_raw", "ein", "is_match") %in% names(mt)) && all(c("ein", "about_page_text") %in% names(ab))) {
      mt <- mt[is_match == TRUE]
      mt[, ein := normalize_ein(ein)]
      ab[, ein := normalize_ein(ein)]
      tmp <- merge(mt[, .(org_name_raw, ein)], ab[, .(ein, about_page_text)], by = "ein", all.x = TRUE)
      tmp <- tmp[!is.na(about_page_text) & about_page_text != ""]
      if (nrow(tmp) > 0) {
        tmp <- tmp[, .(about_page_text = about_page_text[1]), by = org_name_raw]
        setnames(tmp, "org_name_raw", "org_name")
        gt <- merge(gt, tmp, by = "org_name", all.x = TRUE)
      }
    }
  }
  if (!"about_page_text" %in% names(gt)) gt[, about_page_text := NA_character_]
  tick("Ground-truth + about-page join complete")

  gt[, text := clean_text(paste(org_name, about_page_text))]
  gt[nchar(text) == 0, text := clean_text(org_name)]

  y <- as.integer(gt$true_group == "asian")
  folds <- stratified_folds(y, k = cfg$folds, seed = cfg$seed)

  vocab <- build_vocab(gt$text, min_df = cfg$min_df, max_features = cfg$max_features)
  dtm_train <- build_dtm(gt$text, vocab)
  X <- dtm_train$X
  tick("Text features built")

  models <- c("glmnet", "ranger", "xgboost")
  oof <- data.table(row_id = seq_len(nrow(gt)), y = y, fold = folds)
  for (m in models) oof[, (paste0("p_", m)) := NA_real_]

  for (f in seq_len(cfg$folds)) {
    tr <- which(folds != f)
    te <- which(folds == f)

    Xtr <- X[tr, ]
    Xte <- X[te, ]
    ytr <- y[tr]

    # Each model is wrapped with tryCatch to keep pipeline robust.
    p_glm <- tryCatch(fit_predict_glmnet(Xtr, ytr, Xte), error = function(e) rep(mean(ytr), length(te)))
    p_rf <- tryCatch(fit_predict_ranger(Xtr, ytr, Xte), error = function(e) rep(mean(ytr), length(te)))
    p_xgb <- tryCatch(fit_predict_xgb(Xtr, ytr, Xte), error = function(e) rep(mean(ytr), length(te)))

    oof[te, p_glmnet := p_glm]
    oof[te, p_ranger := p_rf]
    oof[te, p_xgboost := p_xgb]
    tick(sprintf("Cross-validation fold %s/%s complete", f, cfg$folds))
  }

  # Stacked super learner (meta logistic over OOF preds)
  meta <- glm(y ~ p_glmnet + p_ranger + p_xgboost, data = oof, family = binomial())
  oof[, p_super := as.numeric(predict(meta, newdata = oof, type = "response"))]

  metric_rows <- list()
  # True SuperLearner ensemble (ecpolley/SuperLearner) via cross-validated predictions.
  sl_lib <- choose_sl_library()
  Xdf <- as.data.frame(as.matrix(X))
  sl_cv <- tryCatch({
    SuperLearner::CV.SuperLearner(
      Y = y,
      X = Xdf,
      family = binomial(),
      SL.library = sl_lib,
      cvControl = list(V = cfg$folds, stratifyCV = TRUE),
      method = "method.NNLS",
      verbose = FALSE
    )
  }, error = function(e) NULL)

  if (!is.null(sl_cv) && !is.null(sl_cv$SL.predict)) {
    oof[, p_superlearner := as.numeric(sl_cv$SL.predict)]
  } else {
    # Robust fallback: average base learners.
    oof[, p_superlearner := rowMeans(.SD, na.rm = TRUE), .SDcols = c("p_glmnet", "p_ranger", "p_xgboost")]
  }
  tick("SuperLearner CV complete")

  for (m in c(models, "superlearner")) {
    pm <- paste0("p_", m)
    mm <- metrics_binary(oof$y, oof[[pm]])
    mm[, model := m]
    metric_rows[[length(metric_rows) + 1]] <- mm
  }
  metrics <- rbindlist(metric_rows, fill = TRUE)[,
    .(model, accuracy, balanced_accuracy, macro_f1, auc, log_loss)]

  # Pick best by macro_f1, then accuracy, then auc.
  setorder(metrics, -macro_f1, -accuracy, -auc, log_loss)
  best_model <- metrics$model[1]

  # Train full models.
  p_full_glm <- tryCatch(fit_predict_glmnet(X, y, X), error = function(e) rep(mean(y), nrow(X)))
  tick("Full glmnet fit complete")
  p_full_rf <- tryCatch(fit_predict_ranger(X, y, X), error = function(e) rep(mean(y), nrow(X)))
  tick("Full ranger fit complete")
  p_full_xgb <- tryCatch(fit_predict_xgb(X, y, X), error = function(e) rep(mean(y), nrow(X)))
  tick("Full xgboost fit complete")
  sl_full <- tryCatch({
    SuperLearner::SuperLearner(
      Y = y,
      X = Xdf,
      family = binomial(),
      SL.library = sl_lib,
      method = "method.NNLS",
      verbose = FALSE
    )
  }, error = function(e) NULL)
  tick("Full SuperLearner fit complete")

  # Candidate scoring.
  cand <- fread(cfg$candidates_input, encoding = "UTF-8")
  if (!all(c("ein", "irs_name_raw") %in% names(cand))) stop("candidates_input must include ein and irs_name_raw")

  about <- if (file.exists(cfg$about_input)) fread(cfg$about_input, encoding = "UTF-8") else data.table()
  if (!"ein" %in% names(about)) about[, ein := NA_character_]
  if (!"about_page_text" %in% names(about)) about[, about_page_text := NA_character_]

  cand[, ein := normalize_ein(ein)]
  about[, ein := normalize_ein(ein)]

  cand <- merge(cand, about[, .(ein, about_page_text)], by = "ein", all.x = TRUE)
  cand[, text := clean_text(paste(irs_name_raw, about_page_text))]
  cand[nchar(text) == 0, text := clean_text(irs_name_raw)]
  tick("Candidate + about-page join complete")

  dtm_cand <- build_dtm(cand$text, vocab, idf = dtm_train$idf)
  Xc <- dtm_cand$X
  tick("Candidate text features built")

  pc_glm <- tryCatch(fit_predict_glmnet(X, y, Xc), error = function(e) rep(mean(y), nrow(Xc)))
  pc_rf <- tryCatch(fit_predict_ranger(X, y, Xc), error = function(e) rep(mean(y), nrow(Xc)))
  pc_xgb <- tryCatch(fit_predict_xgb(X, y, Xc), error = function(e) rep(mean(y), nrow(Xc)))

  cand[, p_glmnet := pc_glm]
  cand[, p_ranger := pc_rf]
  cand[, p_xgboost := pc_xgb]
  if (!is.null(sl_full)) {
    Xc_df <- as.data.frame(as.matrix(Xc))
    cand[, p_superlearner := as.numeric(predict(sl_full, newdata = Xc_df)$pred)]
  } else {
    cand[, p_superlearner := rowMeans(.SD, na.rm = TRUE), .SDcols = c("p_glmnet", "p_ranger", "p_xgboost")]
  }

  sel_col <- paste0("p_", best_model)
  cand[, p_asian := get(sel_col)]
  cand[, p_latino := 1 - p_asian]
  cand[, pred_group := fifelse(p_asian >= 0.5, "asian", "latino")]
  cand[, confidence := pmax(p_asian, p_latino)]
  cand[, margin := abs(p_asian - p_latino)]

  cand[, pass_ml_filter := confidence >= cfg$confidence_threshold & margin >= cfg$margin_threshold]

  fwrite(oof, file.path(cfg$out_dir, "cv_oof_predictions.csv"))
  fwrite(metrics, file.path(cfg$out_dir, "cv_model_metrics.csv"))
  fwrite(data.table(best_model = best_model, confidence_threshold = cfg$confidence_threshold, margin_threshold = cfg$margin_threshold), file.path(cfg$out_dir, "model_selection.csv"))
  fwrite(cand, file.path(cfg$out_dir, "candidate_predictions_with_ml.csv"))
  fwrite(cand[pass_ml_filter == TRUE], file.path(cfg$out_dir, "candidate_predictions_pass_ml_filter.csv"))
  fwrite(cand[pass_ml_filter == FALSE], file.path(cfg$out_dir, "candidate_predictions_fail_ml_filter.csv"))

  # === Task 2: Panethnic vs. Ethnic binary classifier ===
  # Positive (panethnic = 1): all 818 ground-truth orgs (known panethnic)
  # Negative (panethnic = 0): ethnic-named IRS candidates not reclassified
  #   by 05_reclassify_panethnic_constituency, used as a proxy for ethnic orgs
  message("Building panethnic vs. ethnic classifier...")

  ethnic_neg <- cand[!is.na(candidate_type) & candidate_type == "ethnic_named"]

  if (file.exists(cfg$matches_input)) {
    mt2 <- fread(cfg$matches_input, encoding = "UTF-8")
    if (all(c("ein", "is_match") %in% names(mt2))) {
      matched_eins <- normalize_ein(mt2[is_match == TRUE, ein])
      ethnic_neg   <- ethnic_neg[!ein %in% matched_eins]
    }
  }
  reclass_path <- "processed_data/org_matching/panethnic_constituency_reclass.csv"
  if (file.exists(reclass_path)) {
    rc  <- fread(reclass_path, encoding = "UTF-8")
    rc[, ein := normalize_ein(ein)]
    if ("reclass_group" %in% names(rc)) {
      pan_eins   <- rc[reclass_group %in% c("asian", "latino", "both"), ein]
      ethnic_neg <- ethnic_neg[!ein %in% pan_eins]
    }
  }

  if (nrow(ethnic_neg) > 0L) {
    n_pos    <- nrow(gt)
    n_neg    <- min(nrow(ethnic_neg), n_pos * 3L)
    set.seed(cfg$seed)
    eth_samp <- ethnic_neg[sample(.N, n_neg)]

    pan_eth  <- rbindlist(list(
      data.table(text = gt$text,       y_pan = 1L),
      data.table(text = eth_samp$text, y_pan = 0L)
    ), use.names = TRUE)
    pan_eth[is.na(text) | text == "", text := "unknown"]

    vocab_pan  <- build_vocab(pan_eth$text, min_df = cfg$min_df,
                              max_features = cfg$max_features)
    dtm_pan    <- build_dtm(pan_eth$text, vocab_pan)
    X_pan      <- dtm_pan$X
    y_pan      <- pan_eth$y_pan
    folds_pan  <- stratified_folds(y_pan, k = cfg$folds, seed = cfg$seed)

    oof_pan <- data.table(
      row_id = seq_len(length(y_pan)), y = y_pan, fold = folds_pan,
      p_glmnet = NA_real_, p_ranger = NA_real_, p_xgboost = NA_real_
    )
    for (f in seq_len(cfg$folds)) {
      tr_p <- which(folds_pan != f); te_p <- which(folds_pan == f)
      ytr_p <- y_pan[tr_p]
      oof_pan[te_p, p_glmnet  := tryCatch(
        fit_predict_glmnet(X_pan[tr_p, ], ytr_p, X_pan[te_p, ]),
        error = function(e) rep(mean(ytr_p), length(te_p)))]
      oof_pan[te_p, p_ranger  := tryCatch(
        fit_predict_ranger(X_pan[tr_p, ], ytr_p, X_pan[te_p, ]),
        error = function(e) rep(mean(ytr_p), length(te_p)))]
      oof_pan[te_p, p_xgboost := tryCatch(
        fit_predict_xgb(X_pan[tr_p, ],   ytr_p, X_pan[te_p, ]),
        error = function(e) rep(mean(ytr_p), length(te_p)))]
    }
    oof_pan[, p_ensemble := rowMeans(.SD, na.rm = TRUE),
            .SDcols = c("p_glmnet", "p_ranger", "p_xgboost")]
    m_pan <- metrics_binary(oof_pan$y, oof_pan$p_ensemble)
    m_pan[, model := "panethnic_ensemble"]

    dtm_cand_pan <- build_dtm(cand$text, vocab_pan, idf = dtm_pan$idf)
    Xc_pan       <- dtm_cand_pan$X
    pc_pan_glm   <- tryCatch(fit_predict_glmnet(X_pan, y_pan, Xc_pan),
                             error = function(e) rep(mean(y_pan), nrow(Xc_pan)))
    pc_pan_rf    <- tryCatch(fit_predict_ranger(X_pan, y_pan, Xc_pan),
                             error = function(e) rep(mean(y_pan), nrow(Xc_pan)))
    pc_pan_xgb   <- tryCatch(fit_predict_xgb(X_pan, y_pan, Xc_pan),
                             error = function(e) rep(mean(y_pan), nrow(Xc_pan)))
    cand[, p_panethnic := rowMeans(
      cbind(pc_pan_glm, pc_pan_rf, pc_pan_xgb), na.rm = TRUE)]
    cand[, ml_label := fifelse(
      p_panethnic >= cfg$confidence_threshold, "panethnic",
      fifelse(p_panethnic <= (1 - cfg$confidence_threshold), "ethnic",
              "uncertain"))]

    fwrite(
      cand[, .(ein, irs_name_raw, candidate_type, p_panethnic, ml_label)],
      file.path(cfg$out_dir, "candidate_panethnic_predictions.csv"))
    fwrite(m_pan, file.path(cfg$out_dir, "panethnic_classifier_cv_metrics.csv"))

    message(sprintf(
      "Panethnic classifier CV (n=%s pos=%s neg=%s): accuracy=%.3f bal_acc=%.3f macro_f1=%.3f auc=%.3f",
      nrow(pan_eth), sum(y_pan), sum(1L - y_pan),
      m_pan$accuracy, m_pan$balanced_accuracy, m_pan$macro_f1, m_pan$auc))
    message(sprintf(
      "Panethnic labels: panethnic=%s ethnic=%s uncertain=%s",
      sum(cand$ml_label == "panethnic", na.rm = TRUE),
      sum(cand$ml_label == "ethnic",    na.rm = TRUE),
      sum(cand$ml_label == "uncertain", na.rm = TRUE)))
  } else {
    message("No ethnic_named candidates found; skipping panethnic classifier.")
  }
  tick("Panethnic vs. Ethnic classifier complete")

  # Figure: model performance comparison.
  mlong <- melt(metrics, id.vars = "model", measure.vars = c("accuracy", "macro_f1", "auc"), variable.name = "metric", value.name = "value")
  p <- ggplot(mlong, aes(x = model, y = value, fill = model)) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ metric, nrow = 1) +
    ylim(0, 1) +
    theme_minimal(base_size = 12) +
    labs(title = "Cross-Validated Model Performance", x = "Model", y = "Score")
  ggsave(file.path(cfg$out_dir, "cv_model_performance.png"), p, width = 10, height = 4.5, dpi = 220)
  tick("Outputs and performance figure saved")
  close(pb)

  message(sprintf("Done. Best group model: %s | Passed ML filter: %s/%s",
                  best_model,
                  format(sum(cand$pass_ml_filter), big.mark = ","),
                  format(nrow(cand), big.mark = ",")))
  message("Group classifier CV metrics (copy into header):")
  print(metrics[, .(model, accuracy, balanced_accuracy, macro_f1, auc)])
}

main()
