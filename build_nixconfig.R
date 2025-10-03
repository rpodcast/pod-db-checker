#https://gist.github.com/b-rodrigues/d427703e76a112847616c864551d96a1
library(rix)

rix(
  #date = "2025-04-16",
  r_ver = "4.5.1",
  project_path = getwd(),
  r_pkgs = c(
    "DBI",
    "RSQLite",
    "dplyr",
    "dbplyr",
    "tidyr",
    "anytime",
    "logger",
    "pointblank",
    "purrr",
    "archive",
    "reclin2",
    "arrow",
    "s3fs",
    "lubridate"
  ),
  ide = "none",
  system_pkgs = c("air-formatter"),
  overwrite = TRUE
)
