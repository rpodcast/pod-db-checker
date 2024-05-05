# load packages ----
library(DBI)
library(RSQLite)
library(dplyr)
library(dbplyr)
library(pointblank)
library(logger)
library(s3fs)

source("R/utils.R")

# establish file and db connections ----
db_url <- "https://public.podcastindex.org/podcastindex_feeds.db.tgz"
db_tgz_file <- fs::path_file(db_url)
db_file <- fs::path_ext_remove(db_tgz_file)
db_tmp_dir <- fs::path_temp("dbdir")
if (!fs::dir_exists(db_tmp_dir)) fs::dir_create(db_tmp_dir)
report_tmp_dir <- fs::path_temp("reportdir")
if (!fs::dir_exists(report_tmp_dir)) fs::dir_create(report_tmp_dir)
log_dir <- "logs"
log_file <- paste0("pointblanklog_", format(Sys.time(), "%Y-%m-%d"), ".log")
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

# database cleaning: Create itunesIdText as text variable
logger::log_info("Create itunesIdText variable as text")
itunes_add_q <- dbSendStatement(con, "ALTER TABLE podcasts ADD  COLUMN itunesIdText text")
dbClearResult(itunes_add_q)
itunes_update_q <- dbSendStatement(con, "UPDATE podcasts SET itunesIdText = CAST(itunesId AS text)")
dbClearResult(itunes_update_q)

podcasts_db <- tbl(con, "podcasts")

# create pointblank agent
logger::log_info("Initializing pointblank agent")
agent_1 <-
  create_agent(
    tbl = podcasts_db,
    tbl_name = "podcasts",
    label = "Podcasts Agent 1"
  )

# add rules to agent
agent_2_prep <- 
  agent_1 |>
    col_vals_not_null(
      columns = podcastGuid,
      preconditions = function(x) dplyr::mutate(x, podcastGuid = na_if(podcastGuid, "")),
      active = TRUE,
      step_id = "step-nonmissing-podcastguid",
      label = "Non-missing podcast guid"
    ) |>
    rows_distinct(
      columns = vars(podcastGuid),
      preconditions = function(x) {
        x |>
          dplyr::mutate(podcastGuid = na_if(podcastGuid, "")) |>
          dplyr::filter(!is.na(podcastGuid))
      },
      active = TRUE,
      step_id = "step-unique-podcastguid",
      label = "Unique podcast guid values"
    ) |>
    col_vals_not_null(
      columns = chash,
      preconditions = function(x) dplyr::mutate(x, chash = na_if(chash, "")),
      active = TRUE,
      step_id = "step-nonmissing-chash",
      label = "Non-missing content hash"
    ) |>
    col_vals_not_null(
      columns = newestEnclosureDuration,
      preconditions = function(x) dplyr::mutate(x, newestEnclosureDuration = na_if(newestEnclosureDuration, "")),
      active = TRUE,
      step_id = "step-nonmissing-newestEnclosureDuration",
      label = "Non-missing newest enclosure duration"
    ) |>
    col_vals_in_set(
      columns = explicit,
      set = c(0, 1),
      active = TRUE,
      step_id = "step-valid-explicit",
      label = "Valid explicit values"
    ) |>
    col_vals_gte(
      columns = lastHttpStatus,
      value = 0,
      active = TRUE,
      step_id = "step-valid-lastHttpStatus",
      label = "Valid last HTTP status values"
    ) |>
    col_vals_gte(
      columns = newestEnclosureDuration,
      value = 0,
      active = TRUE,
      step_id = "step-valid-newestEnclosureDuration",
      label = "Valid newest enclosure duration"
    ) |>
    col_vals_between(
      columns = updateFrequency,
      left = 0,
      right = 9,
      active = TRUE,
      step_id = "step-range-updateFrequency",
      label = "Update frequency between 0 and 9"
    ) |>
    rows_distinct(
      columns = vars(itunesIdText),
      preconditions = function(x) {
        x |>
          dplyr::mutate(itunesIdText = na_if(itunesIdText, "")) |>
          dplyr::filter(!is.na(itunesIdText))
      },
      active = TRUE,
      step_id = "step-unique-itunesId",
      label = "Unique iTunes ID"
    ) |>
    rows_distinct(
      columns = vars(chash, host),
      preconditions = function(x) {
        x |>
          dplyr::mutate(chash = na_if(chash, "")) |>
          dplyr::filter(!is.na(chash))
      },
      step_id = "step-dup-chash-host",
      label = "Duplicate chash in host",
      active = TRUE
    ) |>
    rows_distinct(
      columns = vars(title, imageUrl),
      preconditions = function(x) {
        x |>
          dplyr::mutate(chash = na_if(chash, "")) |>
          dplyr::filter(!is.na(chash))
      },
      step_id = "step-dup-title-imageUrl",
      label = "Duplicate title and imageUrl",
      active = TRUE
    ) |>
    rows_distinct(
      columns = vars(chash, title, imageUrl),
      preconditions = function(x) {
        x |>
          dplyr::mutate(chash = na_if(chash, "")) |>
          dplyr::mutate(title = na_if(title, "")) |>
          dplyr::mutate(imageUrl = na_if(imageUrl, "")) |>
          dplyr::filter(!is.na(chash))
      },
      step_id = "step-dup-chash-title-imageUrl",
      label = "Duplicate chash, title, and imageUrl",
      active = TRUE
    )

# generate poinblank report object
# - specify huge number for get_first_n to ensure
#   each step data extract has all rows available
logger::log_info("Performing pointblank agent interrogation")
agent_2_rep <- interrogate(agent_2_prep, get_first_n = 1e10)

# obtain report source data frame
logger::log_info("Obtain report data frame")
report_df <- get_agent_report(agent_2_rep, display_table = FALSE)

# write object to tmp disk and upload to object storage
logger::log_info("Writing pointblank agent execution result to temporary file")
x_write_disk(
  agent_2_rep,
  filename = "podcastdb_pointblank_object",
  keep_extracts = TRUE,
  path = report_tmp_dir
)

logger::log_info("Sending pointblank agent result file to object storage")
s3_file_copy(
  path = fs::path(report_tmp_dir, "podcastdb_pointblank_object"),
  new_path = paste0(s3_bucket_path, fs::path("exports", "podcastdb_pointblank_object")),
  ACL = "public-read",
  overwrite = TRUE
)

# write pointblank plan metadata to object storage
agent_meta_df <- agent_2_rep$validation_set |>
  select(i, i_o, step_id, label, brief, sha1, assertion_type, active)

saveRDS(agent_meta_df, fs::path(db_tmp_dir, "agent_meta_df.rds"))

logger::log_info("Sending agent meta rds file to object storage")
s3_file_copy(
  path = fs::path(db_tmp_dir, "agent_meta_df.rds"),
  new_path = paste0(s3_bucket_path, fs::path("exports", "agent_meta_df.rds")),
  ACL = "public-read",
  overwrite = TRUE
)

# write individual extracts to object storage
# obtain the step_id values for failed assessments
failed_i_values <- agent_2_rep$validation_set |>
  dplyr::filter(!all_passed) |>
  dplyr::pull(i)

failed_step_id_values <- agent_2_rep$validation_set |>
  dplyr::filter(!all_passed) |>
  dplyr::pull(step_id)

# create rds and parquet versions of extracts
# copy to object storage
logger::log_info("Writing extracts of failed pointblank assessments to object storage")
purrr::map2(failed_i_values, failed_step_id_values, ~{
  extract_df <- get_data_extracts(agent_2_rep, .x)
  saveRDS(extract_df, fs::path(db_tmp_dir, paste0(.y, ".rds")))
  s3_file_copy(
    path = fs::path(db_tmp_dir, paste0(.y, ".rds")),
    new_path = paste0(s3_bucket_path, fs::path("exports", paste0(.y, ".rds"))),
    ACL = "public-read",
    overwrite = TRUE
  )

  arrow::write_parquet(extract_df, fs::path(db_tmp_dir, paste0(.y, ".parquet")))
  s3_file_copy(
    path = fs::path(db_tmp_dir, paste0(.y, ".parquet")),
    new_path = paste0(s3_bucket_path, fs::path("exports", paste0(.y, ".parquet"))),
    ACL = "public-read",
    overwrite = TRUE
  )
})

# copy log to object storage
s3_file_copy(
  path = fs::path(log_dir, log_file),
  new_path = paste0(s3_bucket_path, fs::path(log_dir, log_file)),
  ACL = "public-read",
  overwrite = TRUE
)