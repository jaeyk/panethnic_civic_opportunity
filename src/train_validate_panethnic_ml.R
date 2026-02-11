#!/usr/bin/env Rscript

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
    matches_input = "outputs/org_matching/org_to_irs_matches.csv",
    about_input = "outputs/org_matching/candidate_about_pages.csv",
    candidates_input = "outputs/org_matching/potential_asian_latino_orgs.csv",
    out_dir = "outputs/ml_validation",
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

  macro_f1 <- mean(c(f1_0, f1_1), na.rm = TRUE)
  eps <- 1e-8
  ll <- -mean(y_true * log(pmax(pmin(p_asian, 1 - eps), eps)) + (1 - y_true) * log(pmax(pmin(1 - p_asian, 1 - eps), eps)))
  auc <- roc_auc_fast(y_true, p_asian)

  data.table(accuracy = acc, macro_f1 = macro_f1, log_loss = ll, auc = auc)
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

  gt[, text := clean_text(paste(org_name, about_page_text))]
  gt[nchar(text) == 0, text := clean_text(org_name)]

  y <- as.integer(gt$true_group == "asian")
  folds <- stratified_folds(y, k = cfg$folds, seed = cfg$seed)

  vocab <- build_vocab(gt$text, min_df = cfg$min_df, max_features = cfg$max_features)
  dtm_train <- build_dtm(gt$text, vocab)
  X <- dtm_train$X

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

  for (m in c(models, "superlearner")) {
    pm <- paste0("p_", m)
    mm <- metrics_binary(oof$y, oof[[pm]])
    mm[, model := m]
    metric_rows[[length(metric_rows) + 1]] <- mm
  }
  metrics <- rbindlist(metric_rows, fill = TRUE)[, .(model, accuracy, macro_f1, auc, log_loss)]

  # Pick best by macro_f1, then accuracy, then auc.
  setorder(metrics, -macro_f1, -accuracy, -auc, log_loss)
  best_model <- metrics$model[1]

  # Train full models.
  p_full_glm <- tryCatch(fit_predict_glmnet(X, y, X), error = function(e) rep(mean(y), nrow(X)))
  p_full_rf <- tryCatch(fit_predict_ranger(X, y, X), error = function(e) rep(mean(y), nrow(X)))
  p_full_xgb <- tryCatch(fit_predict_xgb(X, y, X), error = function(e) rep(mean(y), nrow(X)))
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

  dtm_cand <- build_dtm(cand$text, vocab, idf = dtm_train$idf)
  Xc <- dtm_cand$X

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

  # Figure: model performance comparison.
  mlong <- melt(metrics, id.vars = "model", measure.vars = c("accuracy", "macro_f1", "auc"), variable.name = "metric", value.name = "value")
  p <- ggplot(mlong, aes(x = model, y = value, fill = model)) +
    geom_col(show.legend = FALSE) +
    facet_wrap(~ metric, nrow = 1) +
    ylim(0, 1) +
    theme_minimal(base_size = 12) +
    labs(title = "Cross-Validated Model Performance", x = "Model", y = "Score")
  ggsave(file.path(cfg$out_dir, "cv_model_performance.png"), p, width = 10, height = 4.5, dpi = 220)

  message(sprintf("Done. Best model: %s | Passed ML filter: %s/%s", best_model, format(sum(cand$pass_ml_filter), big.mark = ","), format(nrow(cand), big.mark = ",")))
}

main()
