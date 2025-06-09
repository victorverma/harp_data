import argparse
import os
import pandas as pd
from datetime import datetime, timedelta
from drms import Client

parser = argparse.ArgumentParser(description="Download HARP data from JSOC")
parser.add_argument("series", type=str, help="String; a SHARP parameter series")
# Use the lookdata tool at http://jsoc.stanford.edu/ajax/lookdata.html to determine the earliest and most recent
# dates with data. Once a series has been selected, the user will be taken to the RecordSet Select tab; the
# earliest and most recent dates can be determined from the First Record and Last Record fields on that tab.
parser.add_argument("first_start_dt", type=str, help="String in yyyymmdd format; first date data is needed for")
parser.add_argument("last_end_dt", type=str, help="String in yyyymmdd format; last date data is needed for")
# JSOC staff recommended downloading data in one-month chunks, though it seems that for some months data
# should be downloaded in smaller chunks. On each step, no more than 30,000 records should be downloaded, which
# seems to be the largest number of records that can be downloaded at once according to
# http://jsoc.stanford.edu/ajax/exportdata.html. However, 202206 and 202209 have slightly fewer than 30,000
# records, but still need to be downloaded in more than one chunk it seems. The record count for a query can be
# determined using the RecordSet Select tab of the lookdata tool at http://jsoc.stanford.edu/ajax/lookdata.html.
parser.add_argument("step_size", type=str, help="String in Pandas period alias format; size of the step to take through the date range")
parser.add_argument("keywords_file", type=str, help="String; file with the keywords to download data on, with one keyword per line")

cmd_args = parser.parse_args()

cli = Client()
sharp_series = cli.series(regex="hmi\\.sharp")
if cmd_args.series not in sharp_series:
    raise ValueError(f"{cmd_args.series} is not a SHARP parameter series. Valid series are {', '.join(sharp_series)}")
if not os.path.exists("raw"):
    raise FileNotFoundError("raw/ doesn't exist; make it before running this script")

first_start_dttm = datetime.strptime(cmd_args.first_start_dt, "%Y%m%d")
# The SHARP parameters are computed every 12 minutes, so 23:48:00 is the last
# time of the day at which they are computed
last_end_dttm = datetime.strptime(cmd_args.last_end_dt, "%Y%m%d").replace(hour=23, minute=48)
start_dttms = pd.date_range(first_start_dttm, last_end_dttm, freq=cmd_args.step_size)
if first_start_dttm not in start_dttms:
    start_dttms = pd.DatetimeIndex([first_start_dttm]).append(start_dttms)
end_dttms = start_dttms[1:] - timedelta(minutes=12)
if last_end_dttm not in end_dttms:
    end_dttms = end_dttms.append(pd.DatetimeIndex([last_end_dttm]))

with open(cmd_args.keywords_file, "r") as file:
    keywords = [line.strip() for line in file if line.strip()]

def download_data(cli: Client, series: str, start_dttm: datetime, end_dttm: datetime, keywords: list[str]) -> None:
    """
    Download data on specified keywords for a given series over a given window of time.

    :param series: String that is the name of the series.
    :param start_dttm: Datetime that is the beginning of the time window.
    :param end_dttm: Datetime that is the end of the time window.
    :param keywords: List of strings giving the keywords.
    """
    ds = f"{series}[][{start_dttm.strftime('%Y.%m.%d_%H:%M:%S')}_TAI-{end_dttm.strftime('%Y.%m.%d_%H:%M:%S')}_TAI]"
    key = ", ".join(keywords)
    data = cli.query(ds, key=key, pkeys=True)
    start_dt = start_dttm.strftime("%Y%m%d")
    end_dt = end_dttm.strftime("%Y%m%d")
    data.to_parquet(os.path.join("raw", f"{start_dt}-{end_dt}.parquet"))

# JSOC staff suggested submitting requests serially to avoid overloading the server; they also said that it
# wouldn't be necessary to sleep between requests made in a serial fashion
for i, (start_dttm, end_dttm) in enumerate(zip(start_dttms, end_dttms)):
    download_data(cli, cmd_args.series, start_dttm, end_dttm, keywords)
    print(f"Downloaded data for {start_dttm}-{end_dttm}")
