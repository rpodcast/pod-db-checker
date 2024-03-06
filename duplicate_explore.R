# load packages ----
library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)
library(tidyr)
library(anytime)
library(logger)
library(s3fs)

podcast_dup_df <- readRDS("db_export/podcast_dup_df.rds")

record_group_counts <- podcast_dup_df |>
  group_by(record_group) |>
  tally(sort = TRUE) |>
  ungroup()

record_group_analysis <- podcast_dup_df |>
  tibble::as_tibble() |>
  nest(.by = record_group) |>
  mutate(
    metrics = purrr::map(data, ~{
      tibble::tibble(
        n_records = nrow(.x),
        n_distinct_podcastGuid = length(unique(.x$podcastGuid)),
        n_distinct_title = length(unique(.x$title)),
        n_distinct_chash = length(unique(.x$chash)),
        n_distinct_description = length(unique(.x$description)),
        n_distinct_episode_count = length(unique(.x$episodeCount)),
        n_distinct_imageUrl = length(unique(.x$imageUrl))
      )
    })
  ) |>
  unnest_wider(col = metrics) |>
  select(-data)
