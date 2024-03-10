formatter_json_glue <- function(..., .logcall = sys.call(), .topcall = sys.call(-1),
                                .topenv = parent.frame()) {
  logger::fail_on_missing_package("jsonlite")
  dots <- list(...)
  if (is.null(nms <- names(dots))) nms <- rep("", length(dots))
  if (any(!nzchar(nms))) {
    nms[!nzchar(nms)] <- paste0("arg", seq_along(dots))[!nzchar(nms)]
  }
  names(dots) <- nms
  eval(as.character(jsonlite::toJSON(
    lapply(dots, function(dot) tryCatch(glue::glue(dot, .envir = .topenv), error = function(ign) dot)),
    auto_unbox = TRUE)),
    envir = .topenv)
}

# create concatenated list of categories
gen_categories_df <- function(data) {
  data <- dplyr::select(data, id, starts_with("category"))
  data_long <- tidyr::pivot_longer(
    data,
    cols = starts_with("category"),
    names_to = "category_index",
    values_to = "category_value"
  ) |>
    dplyr::filter(category_value != "")

  data_sum <- data_long |>
    group_by(id) |>
    summarize(category = glue::glue_collapse(category_value, ", ", last = " and ")) |>
    ungroup()
  return(data_sum)
}