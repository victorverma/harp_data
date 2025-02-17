here::i_am("data/raw/harp/download_data.R")

# Load packages -----------------------------------------------------------

library(here)
library(reticulate)
library(tidyverse)
use_condaenv(here("conda/env"))
drms <- import("drms")

# Define functions --------------------------------------------------------

download_data <- function(start_dttm, end_dttm, keywords) {
  ds <- c(start_dttm, end_dttm) %>%
    format("%Y.%m.%d_%H:%M:%S") %>%
    {str_glue("hmi.sharp_720s[][{.[1]}_TAI-{.[2]}_TAI]")}
  key <- str_c(keywords, collapse = ", ")
  cli$query(ds, key, pkeys = TRUE)
}

# Download the data -------------------------------------------------------

# The metadata-downloading code below is based on (1), which documents the drms
# package. The data-downloading code is based on (2), particularly Examples 1-3.
#
# (1) https://docs.sunpy.org/projects/drms/en/stable/
# (2) https://github.com/mbobra/SHARPs/blob/master/plot_swx_d3.ipynb
# 
# Some other useful references are listed below. (3) has several code examples.
# (4) has lots of information about the data produced by the Solar Dynamics
# Observatory (SDO), how it is organized, how to access it, and how to process
# it. Section 4.2.2 ("Selecting Records") of (4) explains how to construct the
# queries that are used below. (5) has general information about the SHARP 
# parameters; (6) gives the meanings of the bits of the QUALITY keyword. (7) and
# (8) are papers on the pipeline that creates data from the SDO's Helioseismic
# and Magnetic Imager (HMI), including the SHARP parameter data
#
# (3) https://github.com/kbg/drms/tree/master/examples
# (4) https://www.lmsal.com/sdodocs/doc/dcur/SDOD0060.zip/zip/entry/
# (5) http://jsoc.stanford.edu/doc/data/hmi/sharp/sharp.htm
# (6) http://jsoc.stanford.edu/jsocwiki/Lev1qualBits
# (7) https://doi.org/10.1007/s11207-014-0529-3
# (8) https://doi.org/10.1007/s11207-014-0516-8

cli <- drms$Client()

# We use the hmi.sharp_720s data series. Other SHARP parameter series decompose
# the magnetic field vector B differently (cea series), correct for scattered
# light (dconS series), or contain near-real time data (nrt series). We use
# definitive data instead of near-real time data as recommended on page 17 of
# (7).
# 
# All available data series can be viewed using the series() method, which lists
# data series matching a given regular expression. For example, to display all
# the SHARP parameter data series, run
# 
# cli$series(regex = "hmi\\.sharp")
#
# The info() method of the Client class creates an object with information about
# a data series. For example, to verify that the primary keys of the series are
# HARPNUM and T_REC, run
#
# cli$info(ds = "hmi.sharp_720s")$primekeys
#
# To see all the keywords, their descriptions, and their data types, run
# 
# cli$info("hmi.sharp_720s")$keywords

# The command line arguments should be
# - the first date for which data is desired
# - the last date
# - the step size or number of time units that should be downloaded at a time
# - the file with the keywords
# - the subdirectory of harp/ where downloaded data should be saved
# - the number of seconds to sleep for between downloads,
# in that order.
# Per (7), the earliest date with data is 5/1/10. JSOC staff recommended
# downloading data in one-month chunks, though it seems that for some months
# data should be downloaded in smaller chunks. The step size can be determined
# using the lookdata tool at http://jsoc.stanford.edu/ajax/lookdata.html. On
# each step, no more than 30,000 records should be downloaded, which seems to be
# the largest number of records that can be downloaded at once according to
# http://jsoc.stanford.edu/ajax/exportdata.html. However, 202206 and 202209 have
# slightly fewer than 30,000 records, but still need to be downloaded in more
# than one chunk it seems. The step size should be a quoted string; see the help
# page for seq.POSIXt() for examples.
# The keywords file should be an R script in harp/ that creates a character
# vector called keywords containing the keywords. JSOC staff suggested
# submitting requests serially to avoid overloading the server; they also said
# that it wouldn't be necessary to sleep between requests made in a serial
# fashion. If the current script is being run interactively, it should be
# sourced so that the scan() call works properly
if (interactive()) {
  cat(
    "\nType first & last dates, step size, keywords file, subdir, sleep time\n"
  )
  command_args <- scan(what = character(), nmax = 6)
} else {
  command_args <- commandArgs(trailingOnly = TRUE)
}
stopifnot(length(command_args) == 6)
stopifnot(str_detect(command_args[1:2], "^\\d{8}$"))
first_start_dttm <- as_datetime(command_args[1])
# The SHARP parameters are computed every 12 minutes, so 23:48:00 is the last
# time of the day at which they are computed
last_end_dttm <- update(as_datetime(command_args[2]), hours = 23, minutes = 48)
step_size <- command_args[3]
keywords_file <- command_args[4]
subdir <- command_args[5]
num_seconds <- as.integer(command_args[6])

start_dttms <- seq(first_start_dttm, last_end_dttm, by = step_size)
end_dttms <- start_dttms %>%
  `[`(-1) %>%
  `-`(minutes(12)) %>%
  c(last_end_dttm)
source(here(str_glue("data/raw/harp/{keywords_file}")))

for (i in seq_along(start_dttms)) {
  start_dt <- format(start_dttms[i], "%Y%m%d")
  end_dt <- format(end_dttms[i], "%Y%m%d")
  write_csv(
    download_data(start_dttms[i], end_dttms[i], keywords),
    here(str_glue("data/raw/harp/{subdir}/{start_dt}-{end_dt}.csv"))
  )
  if (i < length(start_dttms)) Sys.sleep(num_seconds)
}