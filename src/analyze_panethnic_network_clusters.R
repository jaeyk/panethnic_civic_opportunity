#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(data.table)
  library(ggplot2)
  library(igraph)
})

cfg <- list(
  input = "outputs/analysis/panethnic_share_by_category_decade.csv",
  out_edges = "outputs/analysis/panethnic_network_edges.csv",
  out_nodes = "outputs/analysis/panethnic_network_nodes_clusters.csv",
  out_fig = "outputs/figures/panethnic_category_network_clusters.png",
  min_overlap = 4L,
  corr_threshold = 0.55
)

dir.create(dirname(cfg$out_edges), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(cfg$out_fig), recursive = TRUE, showWarnings = FALSE)

dt <- fread(cfg$input, encoding = "UTF-8")
req <- c("group", "org_type", "decade", "panethnic_share")
miss <- req[!req %in% names(dt)]
if (length(miss) > 0L) stop(sprintf("Missing required columns: %s", paste(miss, collapse = ", ")))

dt[, org_type := as.character(org_type)]
dt[, group := as.character(group)]
dt[, decade := as.integer(decade)]

build_network_for_group <- function(dg, grp_label, min_overlap = 4L, corr_threshold = 0.55) {
  cats <- sort(unique(dg$org_type))
  if (length(cats) < 3L) return(NULL)

  # Wide matrix: rows categories, cols decades
  wide <- dcast(dg, org_type ~ decade, value.var = "panethnic_share", fill = NA_real_)
  rownames(wide) <- wide$org_type
  mat <- as.matrix(wide[, -1])
  rownames(mat) <- wide$org_type

  edges <- data.table()
  for (i in seq_len(nrow(mat) - 1L)) {
    for (j in (i + 1L):nrow(mat)) {
      xi <- mat[i, ]
      xj <- mat[j, ]
      keep <- is.finite(xi) & is.finite(xj)
      n_ov <- sum(keep)
      if (n_ov < min_overlap) next
      r <- suppressWarnings(cor(xi[keep], xj[keep]))
      if (!is.finite(r)) next
      if (r >= corr_threshold) {
        edges <- rbind(
          edges,
          data.table(from = rownames(mat)[i], to = rownames(mat)[j], corr = as.numeric(r), overlap_n = n_ov),
          fill = TRUE
        )
      }
    }
  }

  if (nrow(edges) == 0L) {
    nodes <- data.table(group = grp_label, org_type = cats, cluster = NA_integer_, degree = 0, strength = 0)
    return(list(edges = data.table(), nodes = nodes, layout = data.table()))
  }

  g <- graph_from_data_frame(edges[, .(from, to)], directed = FALSE, vertices = data.table(name = cats))
  edge_key <- edges[, .(from, to, corr)]
  edge_key[, k := ifelse(from < to, paste(from, to, sep = "||"), paste(to, from, sep = "||"))]
  setkey(edge_key, k)
  e_df <- as.data.table(as_data_frame(g, what = "edges"))
  e_df[, k := ifelse(from < to, paste(from, to, sep = "||"), paste(to, from, sep = "||"))]
  e_df <- edge_key[e_df, on = "k"]
  E(g)$weight <- e_df$corr

  # Community detection over similarity graph.
  clu <- cluster_louvain(g, weights = E(g)$weight)
  V(g)$cluster <- membership(clu)[V(g)$name]
  V(g)$degree <- degree(g)
  V(g)$strength <- strength(g, weights = E(g)$weight)

  lay <- layout_with_fr(g, weights = E(g)$weight)
  lay_dt <- data.table(
    group = grp_label,
    org_type = V(g)$name,
    x = lay[, 1],
    y = lay[, 2],
    cluster = as.integer(V(g)$cluster),
    degree = as.numeric(V(g)$degree),
    strength = as.numeric(V(g)$strength)
  )

  edges_plot <- as.data.table(as_data_frame(g, what = "edges"))
  setnames(edges_plot, c("from", "to", "weight"), c("from", "to", "corr"))
  edges_plot[, group := grp_label]
  edges_plot <- merge(edges_plot, lay_dt[, .(org_type, x_from = x, y_from = y)], by.x = "from", by.y = "org_type", all.x = TRUE)
  edges_plot <- merge(edges_plot, lay_dt[, .(org_type, x_to = x, y_to = y)], by.x = "to", by.y = "org_type", all.x = TRUE)

  list(
    edges = edges_plot[, .(group, from, to, corr, x_from, y_from, x_to, y_to)],
    nodes = lay_dt[, .(group, org_type, cluster, degree, strength)],
    layout = lay_dt
  )
}

parts <- lapply(split(dt, by = "group", keep.by = TRUE), function(dg) {
  grp <- unique(dg$group)[1]
  build_network_for_group(dg, grp, min_overlap = cfg$min_overlap, corr_threshold = cfg$corr_threshold)
})
parts <- parts[!vapply(parts, is.null, logical(1))]

edges_all <- rbindlist(lapply(parts, `[[`, "edges"), fill = TRUE)
nodes_all <- rbindlist(lapply(parts, `[[`, "nodes"), fill = TRUE)
layout_all <- rbindlist(lapply(parts, `[[`, "layout"), fill = TRUE)

fwrite(edges_all[, .(group, from, to, corr)], cfg$out_edges)
fwrite(nodes_all[order(group, cluster, -strength)], cfg$out_nodes)

if (nrow(layout_all) > 0L) {
  p <- ggplot() +
    geom_segment(
      data = edges_all,
      aes(x = x_from, y = y_from, xend = x_to, yend = y_to, alpha = corr),
      color = "#8A8A8A", linewidth = 0.6
    ) +
    geom_point(
      data = layout_all,
      aes(x = x, y = y, fill = factor(cluster), size = pmax(1, strength)),
      shape = 21, color = "#1F1F1F", stroke = 0.5
    ) +
    geom_text(
      data = layout_all,
      aes(x = x, y = y, label = org_type),
      nudge_y = 0.02,
      size = 3.0,
      check_overlap = TRUE
    ) +
    facet_wrap(~group, nrow = 1, scales = "free") +
    scale_alpha(range = c(0.25, 0.9), guide = "none") +
    scale_size_continuous(range = c(2.2, 6), guide = "none") +
    labs(
      title = "Category Network Clusters from Decade Trend Similarity",
      subtitle = sprintf("Edges: correlation >= %.2f over at least %d shared decades", cfg$corr_threshold, cfg$min_overlap),
      x = NULL,
      y = NULL,
      fill = "Cluster"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      panel.grid = element_blank(),
      axis.text = element_blank(),
      axis.title = element_blank(),
      axis.ticks = element_blank(),
      legend.position = "bottom"
    )

  ggsave(cfg$out_fig, p, width = 12, height = 7, dpi = 220)
}

cat(sprintf("Input rows: %s\n", format(nrow(dt), big.mark = ",")))
cat(sprintf("Network edges written: %s\n", format(nrow(edges_all), big.mark = ",")))
cat(sprintf("Network nodes written: %s\n", format(nrow(nodes_all), big.mark = ",")))
cat(sprintf("Wrote edges: %s\n", cfg$out_edges))
cat(sprintf("Wrote nodes: %s\n", cfg$out_nodes))
cat(sprintf("Wrote figure: %s\n", cfg$out_fig))
