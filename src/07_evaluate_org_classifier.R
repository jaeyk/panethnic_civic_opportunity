#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
})

parse_args <- function(args) {
  cfg <- list(
    asian_input = "raw_data/org_data_ground_truth/asian_org.csv",
    latino_input = "raw_data/org_data_ground_truth/latino_org.csv",
    out_dir = "outputs/evaluation"
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

regex_detect <- function(x, patterns) {
  if (length(patterns) == 0) return(rep(FALSE, length(x)))
  p <- paste(sprintf("\\b%s\\b", gsub(" +", "\\\\s+", patterns)), collapse = "|")
  grepl(p, tolower(x), perl = TRUE)
}

infer_panethnic_group <- function(name_text) {
  txt <- tolower(trimws(as.character(name_text)))

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

  asian_weak <- c("api", "a a p i")
  latino_weak <- c("raza", "latinas", "spanish")

  a_strong <- regex_detect(txt, asian_strong)
  l_strong <- regex_detect(txt, latino_strong)
  a_weak <- regex_detect(txt, asian_weak)
  l_weak <- regex_detect(txt, latino_weak)

  a_score <- as.integer(a_strong) * 2L + as.integer(a_weak)
  l_score <- as.integer(l_strong) * 2L + as.integer(l_weak)

  ifelse(a_score == 0 & l_score == 0, "uncertain",
         ifelse(a_score > l_score, "asian",
                ifelse(l_score > a_score, "latino", "both")))
}

safe_div <- function(a, b) ifelse(b == 0, NA_real_, a / b)

main <- function() {
  cfg <- parse_args(commandArgs(trailingOnly = TRUE))
  dir.create(cfg$out_dir, recursive = TRUE, showWarnings = FALSE)

  asian <- fread(cfg$asian_input, encoding = "UTF-8")
  latino <- fread(cfg$latino_input, encoding = "UTF-8")

  if (!"Name" %in% names(asian) || !"Name" %in% names(latino)) {
    stop("Both ground-truth files must have a Name column")
  }

  asian[, true_label := "asian"]
  latino[, true_label := "latino"]

  dt <- rbindlist(list(asian, latino), fill = TRUE)
  dt[, org_name := Name]
  dt[, pred_label := infer_panethnic_group(org_name)]

  dt[, pred_binary := fifelse(pred_label %in% c("asian", "latino"), pred_label, "other")]

  confusion <- dt[, .N, by = .(true_label, pred_binary)]
  all_true <- c("asian", "latino")
  all_pred <- c("asian", "latino", "other")
  confusion <- CJ(true_label = all_true, pred_binary = all_pred)[confusion, on = .(true_label, pred_binary)]
  confusion[is.na(N), N := 0L]

  accuracy <- dt[, mean(pred_label == true_label)]

  metric_rows <- lapply(c("asian", "latino"), function(lbl) {
    tp <- dt[true_label == lbl & pred_label == lbl, .N]
    fp <- dt[true_label != lbl & pred_label == lbl, .N]
    fn <- dt[true_label == lbl & pred_label != lbl, .N]

    precision <- safe_div(tp, tp + fp)
    recall <- safe_div(tp, tp + fn)
    f1 <- ifelse(is.na(precision) || is.na(recall) || (precision + recall) == 0, NA_real_, 2 * precision * recall / (precision + recall))

    data.table(
      label = lbl,
      precision = precision,
      recall = recall,
      f1 = f1,
      support = dt[true_label == lbl, .N]
    )
  })

  metrics <- rbindlist(metric_rows)
  overall <- data.table(
    metric = c("accuracy", "macro_f1"),
    value = c(accuracy, mean(metrics$f1, na.rm = TRUE))
  )

  fwrite(dt, file.path(cfg$out_dir, "org_classifier_predictions_ground_truth.csv"))
  fwrite(confusion, file.path(cfg$out_dir, "org_classifier_confusion_matrix.csv"))
  fwrite(metrics, file.path(cfg$out_dir, "org_classifier_metrics_by_label.csv"))
  fwrite(overall, file.path(cfg$out_dir, "org_classifier_metrics_overall.csv"))

  heat <- copy(confusion)
  heat[, true_label := factor(true_label, levels = c("asian", "latino"))]
  heat[, pred_binary := factor(pred_binary, levels = c("asian", "latino", "other"))]

  p <- ggplot(heat, aes(x = pred_binary, y = true_label, fill = N)) +
    geom_tile(color = "white") +
    geom_text(aes(label = N), size = 4) +
    scale_fill_gradient(low = "#f1eef6", high = "#045a8d") +
    labs(
      title = "Org Classifier Performance on Ground Truth",
      subtitle = sprintf("Accuracy = %.1f%% | Macro-F1 = %.1f%%", 100 * accuracy, 100 * mean(metrics$f1, na.rm = TRUE)),
      x = "Predicted label",
      y = "True label",
      fill = "Count"
    ) +
    theme_minimal(base_size = 12)

  ggsave(file.path(cfg$out_dir, "org_classifier_accuracy.png"), p, width = 8, height = 5, dpi = 220)

  cat(sprintf("Saved evaluation outputs to %s\n", cfg$out_dir))
  cat(sprintf("Accuracy: %.4f\n", accuracy))
}

main()
