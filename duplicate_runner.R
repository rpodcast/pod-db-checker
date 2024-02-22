# load packages ----
library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)
library(tidyr)
library(anytime)
library(logger)
library(s3fs)

source("R/utils.R")

# establish file and db connections ----
db_url <- "https://public.podcastindex.org/podcastindex_feeds.db.tgz"
db_tgz_file <- fs::path_file(db_url)
db_file <- fs::path_ext_remove(db_tgz_file)
db_tmp_dir <- fs::path_temp("dbdir")
if (!fs::dir_exists(db_tmp_dir)) fs::dir_create(db_tmp_dir)
log_dir <- "logs"
log_file <- paste0("pdblog_", format(Sys.time(), "%Y-%m-%d"), ".log")
s3_bucket_path <- "s3://podcast20-projects/"

s3_file_system(
  aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  endpoint = Sys.getenv("ENDPOINT"),
  region_name = Sys.getenv("AWS_REGION")
)

# initialize logger options ----
# - first logger for console
logger::log_threshold("INFO")

# - second logger for JSON file
logger::log_threshold("INFO", index = 2)
logger::log_appender(logger::appender_file(here::here(log_dir, log_file)), index = 2)
logger::log_formatter(formatter_json_glue, index = 2)
logger::log_layout(
  logger::layout_json_parser(
    fields = c("time", "level", "ns", "topenv", "fn", "user")
  ),
  index = 2
)

# download and extract podcast database ----
logger::log_info("Downloading podcast database")
curl::curl_download(db_url, destfile = fs::path(db_tmp_dir, db_tgz_file), quiet = TRUE)

logger::log_info("Extracting podcast database file")
archive::archive_extract(fs::path(db_tmp_dir, db_tgz_file), dir = db_tmp_dir)
db_file_size <- fs::file_size(fs::path(db_tmp_dir, db_file)) |> unname()
logger::log_info('DB size: {db_file_size}')

# initialize database connection
con <- DBI::dbConnect(
  RSQLite::SQLite(),
  fs::path(db_tmp_dir, db_file)
)
podcasts_db <- tbl(con, "podcasts")

# remove records with missing chash value
logger::log_info('Removing records with missing chash values')
podcasts_filtered_db <- podcasts_db |>
  filter(chash != "")

# create initial de-duplication pairs
logger::log_info("Create de-duplication pairs")
db_pairs <- reclin2::pair_blocking(
  podcasts_filtered_db, 
  on = c("title", "chash"), 
  deduplication = TRUE
)

logger::log_info("Pairs dataset includes {nrow(db_pairs)} rows")

# compare pairs on url, newestEnclosureUrl, and imageUrl
logger::log_info("Comparing pairs")
reclin2::compare_pairs(
  db_pairs,
  on = c("url", "newestEnclosureUrl", "imageUrl"),
  default_comparator = reclin2::cmp_jarowinkler(),
  inplace = TRUE
)

# select pairs with threshold at or above 0.95
threshold_value <- 0.95
logger::log_info("Select pairs with threshold above {threshold_value}")

reclin2::select_threshold(
  db_pairs, 
  variable = "threshold_select",
  score = "url", 
  threshold = threshold_value, 
  inplace = TRUE
)

# Perform de-duplication
logger::log_info("Performing deduplication")

db_dedup <- reclin2::deduplicate_equivalence(
  db_pairs,
  variable = "record_group",
  selection = "threshold_select"
)

# Obtain record groups with at least 2 duplicated podcast entries

logger::log_info("Generating duplicate record group counts")

record_group_counts <- db_dedup |>
  group_by(record_group) |>
  tally(sort = TRUE) |>
  ungroup() |>
  filter(n > 1)

logger::log_info("Number of record groups with 2 or more podcast entries: {nrow(record_group_counts)}")

# filter duplication set to only contain the groups with two or more records
group_ids <- pull(record_group_counts, record_group)

podcast_dup_df <- filter(db_dedup, record_group %in% group_ids)

logger::log_info("Number of flagged duplicate entries in database: {nrow(podcast_dup_df)}")

# create parquet version of duplicate data and send to s3
logger::log_info("Creating parquet version of duplicate dataset")
arrow::write_parquet(podcast_dup_df, fs::path(db_tmp_dir, "podcast_dup_df.parquet"))

logger::log_info("Sending database parquet file to object storage")
s3_file_copy(
  path = fs::path(db_tmp_dir, "podcast_dup_df.parquet"),
  new_path = paste0(s3_bucket_path, fs::path("exports", "podcast_dup_df.parquet")),
  ACL = "public-read",
  overwrite = TRUE
)

# create rds file of duplicate data and send to s3
logger::log_info("Creating rds version of duplicate dataset")
saveRDS(podcast_dup_df, fs::path(db_tmp_dir, "podcast_dup_df.rds"))

logger::log_info("Sending database rds file to object storage")
s3_file_copy(
  path = fs::path(db_tmp_dir, "podcast_dup_df.rds"),
  new_path = paste0(s3_bucket_path, fs::path("exports", "podcast_dup_df.rds")),
  ACL = "public-read",
  overwrite = TRUE
)

# create duplicates analysis metrics data frame
logger::log_info("Deriving podcast duplicate analytics")
analysis_metrics_df <- podcast_dup_df |>
  tibble::as_tibble() |>
  mutate(newestItemPubdate = na_if(newestItemPubdate, 0),
         oldestItemPubdate = na_if(oldestItemPubdate, 0),
         title = na_if(title, ""),
         lastUpdate = na_if(lastUpdate, 0),
         createdOn = na_if(createdOn, 0)) |>
  mutate(newestItemPubdate_p = anytime(newestItemPubdate),
         oldestItemPubdate_p = anytime(oldestItemPubdate),
         createdOn_p = anytime(createdOn)) |>
  mutate(pub_timespan_days = lubridate::interval(oldestItemPubdate_p, newestItemPubdate_p) / lubridate::ddays(1)) |>
  mutate(created_timespan_days = lubridate::interval(createdOn_p, Sys.time()) / lubridate::ddays(1)) |>
  nest(.by = record_group) |>
  mutate(
    metrics = purrr::map(data, ~{
      tibble::tibble(
        n_records = nrow(.x),
        group_title = unique(.x$title),
        n_distinct_podcastGuid = length(unique(.x$podcastGuid)),
        n_distinct_title = length(unique(.x$title)),
        n_distinct_chash = length(unique(.x$chash)),
        n_distinct_description = length(unique(.x$description)),
        n_distinct_episode_count = length(unique(.x$episodeCount)),
        n_distinct_imageUrl = length(unique(.x$imageUrl)),
        med_newestEnclosureDuration = median(.x$newestEnclosureDuration, na.rm = TRUE),
        med_created_timespan_days = median(.x$created_timespan_days, na.rm = TRUE),
        med_pub_timespan_days = median(.x$pub_timespan_days)
        #newestEnclosureDuration_list = list(.x$newestEnclosureDuration),
        #created_timespan_days_list = list(.x$created_timespan_days),
        #pub_timespan_days_list = list(.x$pub_timespan_days)
      )
    })
  ) |>
  unnest_wider(col = metrics) |>
  select(-data)

# create rds file of analysis metrics and send to s3
logger::log_info("Creating rds version of analysis metrics dataset")
saveRDS(analysis_metrics_df, fs::path(db_tmp_dir, "analysis_metrics_df.rds"))

logger::log_info("Sending analysis metrics rds file to object storage")
s3_file_copy(
  path = fs::path(db_tmp_dir, "analysis_metrics_df.rds"),
  new_path = paste0(s3_bucket_path, fs::path("exports", "analysis_metrics_df.rds")),
  ACL = "public-read",
  overwrite = TRUE
)

# copy log to object storage
s3_file_copy(
  path = fs::path(log_dir, log_file),
  new_path = paste0(s3_bucket_path, fs::path(log_dir, log_file)),
  ACL = "public-read",
  overwrite = TRUE
)
