import argparse
import numpy as np
import os
import pandas as pd
import time
from drms import Client

parser = argparse.ArgumentParser(description="Process HARP data downloaded from JSOC")
parser.add_argument("--series", type=str, required=True, help="A data series for which data has been downloaded, e.g., hmi.sharp_cea_720s")
cmd_args = parser.parse_args()
series = cmd_args.series
series_no_dot = series.replace(".", "_", 1)

print("Compiling the raw data", end="", flush=True)
start_time = time.time()
raw_dir = f"../data/raw/{series_no_dot}"
files = [os.path.join(raw_dir, file) for file in os.listdir(raw_dir) if file.endswith(".parquet")]
files.sort()
data = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
elapsed_time = time.time() - start_time
print(f"\rCompiling the raw data ({int(elapsed_time)}s)", flush=True)

print("Turning datetime strings into datetimes", end="", flush=True)
start_time = time.time()
cli = Client()
keywords = cli.info(series).keywords
types = keywords[keywords.index.isin(data.columns)]["type"]
time_cols = types[types == "time"].index
data[time_cols] = data[time_cols].apply(
    # The TAI system differs from the UTC system; see (1). The latter system is used in the GOES data (2, 3). However, the discrepancy is only 37
    # seconds as of 2025-01-11, so the impact of treating TAI times as UTC times should be negligible. It seems to be possible to convert TAI
    # times to UTC times using the astropy package though (4).
    #
    # (1) https://en.wikipedia.org/wiki/International_Atomic_Time
    # (2) https://www.ncei.noaa.gov/data/goes-space-environment-monitor/access/science/xrs/GOES_1-15_XRS_Science-Quality_Data_Readme.pdf
    # (3) https://data.ngdc.noaa.gov/platforms/solar-space-observing-satellites/goes/goes16/l2/docs/GOES-R_XRS_L2_Data_Users_Guide.pdf
    # (4) https://docs.astropy.org/en/stable/time/index.html
    lambda col: pd.to_datetime(col.str.replace("_TAI", ""), utc=True, format="%Y.%m.%d_%H:%M:%S")
)
elapsed_time = time.time() - start_time
print(f"\rTurning datetime strings into datetimes ({int(elapsed_time)}s)", flush=True)

def insert_missing_rows(group: pd.DataFrame) -> pd.DataFrame:
    """
    Insert rows in the Pandas DataFrame for a specified HARP for times that should have rows but do not.

    :param group: Pandas DataFrame containing the data for the HARP.
    :return: Pandas DataFrame with the missing rows added.
    """
    full_time_range = pd.date_range(start=group["T_REC"].min(), end=group["T_REC"].max(), freq="12min")
    group = group.set_index("T_REC").reindex(full_time_range, fill_value=pd.NA).reset_index(names="T_REC")
    group.insert(1, "T_REC", group.pop("T_REC"))
    group["HARPNUM"] = group["HARPNUM"].ffill().bfill()
    return group

print("Inserting missing rows", end="", flush=True)
start_time = time.time()
orig_dtypes = data.dtypes
orig_dtypes.where(orig_dtypes != np.dtype("int64"), pd.Int64Dtype(), inplace=True) # int64 doesn't support NA, but Int64 does
data = data.groupby("HARPNUM")[data.columns].apply(insert_missing_rows)
data.reset_index(drop=True, inplace=True)
data = data.astype(orig_dtypes)
data.sort_values(by=["HARPNUM", "T_REC"], inplace=True)
elapsed_time = time.time() - start_time
print(f"\rInserting missing rows ({int(elapsed_time)}s)", flush=True)

print("Saving the DataFrame", end="", flush=True)
start_time = time.time()
data.to_parquet(f"../data/processed/{series_no_dot}.parquet")
elapsed_time = time.time() - start_time
print(f"\rSaving the DataFrame ({int(elapsed_time)}s)", flush=True)
print("Done")