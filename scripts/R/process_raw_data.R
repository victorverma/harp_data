# here::i_am("../../scripts/process_raw_harp_data.R")
setwd("~/research/harp_data/")

# Load packages -----------------------------------------------------------

library(foreach)
# library(here)
library(lubridate)
library(reticulate)
library(tidyverse)
library(tools)
use_condaenv("env/")
# use_condaenv(here("conda/env"))
drms <- import("drms")

# Process the data --------------------------------------------------------

# The command line arguments should be
# - the file with the keywords
# - the subdirectory where downloaded data has been saved
# - the RData file the processed data should be put in,
# in that order. The keywords file should be the R script in data/raw/harp/ that
# creates a character vector called keywords containing the keywords. The
# subdirectory is assumed to be in data/raw/harp/. The RData file is created in
# data/processed/harp/. If the current script is being run interactively, it
# should be sourced so that the scan() call works properly
if (interactive()) {
  cat("\nEnter keywords file, subdirectory with data files, and output file\n")
  command_args <- scan(what = character(), nmax = 3)
} else {
  command_args <- commandArgs(trailingOnly = TRUE)
}
stopifnot(length(command_args) == 3)
keywords_file <- command_args[1]
subdir <- command_args[2]
output_file <- command_args[3]

source(here(str_glue("data/raw/harp/{keywords_file}")))
# See the comments in data/raw/harp/download_data.R for information on the use
# of this object
cli <- drms$Client()
spec_expr <- cli$info("hmi.sharp_720s")$keywords %>%
  as_tibble(rownames = "keyword") %>%
  select(keyword, type) %>%
  # HARPNUM and T_REC are primary keys of hmi.sharp_720s as can be verified
  # using the code in the comments referred to above
  filter(keyword %in% c("HARPNUM", "T_REC", keywords)) %>%
  mutate(
    spec_fun = case_when( # This may need to be updated over time
      type %in% c("double", "float", "longlong") ~ "col_double()",
      type == "int" ~ "col_integer()",
      type %in% c("string", "time") ~ "col_character()"
    ),
    tag_val_pair = str_c(keyword, spec_fun, sep = " = ")
  ) %>%
  pull(tag_val_pair) %>%
  str_c(collapse = ",") %>%
  {str_glue("spec <- cols({.})")} %>%
  parse(text = .)
eval(spec_expr)

files <- list.files(here(str_glue("data/raw/harp/{subdir}")), full.names = TRUE)
harp_tbl <- foreach (file = files, .combine = bind_rows) %do% {
  writeLines(file_path_sans_ext(basename(file)))
  read_csv(file, col_types = spec, show_col_types = FALSE)
} %>%
  mutate(
    T_REC = parse_date_time(str_remove(T_REC, "_TAI"), "%Y.%m.%d_%H:%M:%S"),
    # See http://jsoc.stanford.edu/jsocwiki/Lev1qualBits for information about
    # this keyword
    QUALITY = str_c("0x", str_pad(as.hexmode(QUALITY), width = 8, pad = "0")),
    QUAL_S = str_c("0x", str_pad(as.hexmode(QUAL_S), width = 8, pad = "0")),
    # We flag records for HARPs that were entirely within 68 degrees of the
    # central meridian at the time of observation. Using only these records is
    # a way to avoid projection effects as stated in (1) and (2).
    #
    # (1) https://doi.org/10.1029/2020SW002440
    # (2) https://doi.org/10.3847/1538-4357/ab89ac
    are_lons_small = (abs(LON_MIN) <= 68) & (abs(LON_MAX) <= 68)
  ) %>%
  nest(.by = HARPNUM) %>%
  # There should be a set of SHARP parameter values every 12 minutes, but that
  # seems to be false; see eda/missingness_analysis/missingness_analysis.Rmd
  mutate(
    data = map(data, ~ complete(.x, T_REC = full_seq(T_REC, period = 720)))
  ) %>%
  unnest(data)

# This will come in handy in several places
sharp_params <- c(
  "ABSNJZH", "MEANALP", "MEANGAM", "MEANGBH", "MEANGBT", "MEANGBZ", "MEANJZD",
  "MEANJZH", "MEANPOT", "MEANSHR", "SAVNCPP", "SHRGT45", "TOTPOT", "TOTUSJH",
  "TOTUSJZ", "USFLUX"
)

# Save the processed data -------------------------------------------------

save(
  harp_tbl, sharp_params,
  file = here(str_glue("data/processed/harp/{output_file}"))
)