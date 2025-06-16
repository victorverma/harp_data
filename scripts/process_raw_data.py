import argparse
import numpy as np
import os
import pandas as pd
import time
from drms import Client

parser = argparse.ArgumentParser(description="Process raw HARP data downloaded from JSOC")
parser.add_argument("--series", type=str, required=True, help="A data series for which raw data has been downloaded, e.g., hmi.sharp_cea_720s")
parser.add_argument("--keep_low_qual_vals", action="store_true", help="Whether to keep predictor values in low-quality records (default: False)")
parser.add_argument("--keep_near_limb_recs", action="store_true", help="Whether to keep records for near-limb HARPs (default: False)")
parser.add_argument("--limb_threshold", type=float, default=70, help="Longitude threshold defining where the limb starts (default: 70)")
cmd_args = parser.parse_args()
series = cmd_args.series
series_no_dot = series.replace(".", "_", 1)
keep_low_qual_vals = cmd_args.keep_low_qual_vals
keep_near_limb_recs = cmd_args.keep_near_limb_recs
limb_threshold = cmd_args.limb_threshold

print("Compiling the raw data", end="", flush=True)
start_time = time.time()
files = [os.path.join("raw", file) for file in os.listdir("raw") if file.endswith(".parquet")]
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

# In a tiny number of records, some variables have infinite values. For example, for HARP 3672, at 2014-01-21 12:48:00, ABSNJZH is infinite.
# These infinities are probably invalid values, so we replace them with NaNs.
print("Replacing infinities with NaNs", end="", flush=True)
start_time = time.time()
number_cols = data.select_dtypes(include=["number"]).columns
data[number_cols] = data[number_cols].replace([np.inf, -np.inf], np.nan)
elapsed_time = time.time() - start_time
print(f"\rReplacing infinities with NaNs ({int(elapsed_time)}s)", flush=True)

predictors = [
    # SHARP parameters
    "USFLUX", "MEANGAM", "MEANGBT", "MEANGBZ", "MEANGBH", "MEANJZD", "TOTUSJZ", "MEANALP",
    "MEANJZH", "TOTUSJH", "ABSNJZH", "SAVNCPP", "MEANPOT", "TOTPOT", "MEANSHR", "SHRGT45",
    # Patch areas and pixel counts
    "NPIX", "SIZE", "AREA", "NACR", "SIZE_ACR", "AREA_ACR"
]
if not keep_low_qual_vals:
    if (data.columns == "QUALITY").any():
        print("Replacing low-quality values with NaNs", end="", flush=True)
        start_time = time.time()
        data.loc[data["QUALITY"] != 0, predictors] = np.nan
        elapsed_time = time.time() - start_time
        print(f"\rReplacing low-quality values with NaNs ({int(elapsed_time)}s)", flush=True)
    else:
        raise ValueError("To replace low-quality values with NaNs, QUALITY must be in the list of keywords")

# Per (1), the level of noise in the SHARP parameters is substantially higher when the HARP is near the limb of the Sun. Per (3)-(5),
# data measured on HARPs near the limb can be distorted by projection effects. Reference (1) seems to define the limb as the region
# more than 45 degrees from the central meridian (see Section 9), while reference (2) uses a threshold of 70 degrees. However, in
# (2), no X-class flare in the data occurred more than 68 degrees from the central meridian. References (3)-(5) use a threshold of 68
# degrees, seemingly because of (2). Based on Table 4 in (1) and (5), the longitude of the flux-weighted center of the active pixels
# (LON_FWT) is what needs to be within bounds.
#
# (1) https://doi.org/10.1007/s11207-014-0529-3
# (2) https://dx.doi.org/10.1088/0004-637X/798/2/135
# (3) https://doi.org/10.1029/2019SW002214
# (4) https://doi.org/10.1029/2020SW002440
# (5) https://doi.org/10.3847/1538-4357/ab89ac
if not keep_near_limb_recs:
    if (data.columns == "LON_FWT").any():
        print("Discarding near-limb records", end="", flush=True)
        start_time = time.time()
        data = data[data["LON_FWT"].abs() <= limb_threshold]
        elapsed_time = time.time() - start_time
        print(f"\rDiscarding near-limb records ({int(elapsed_time)}s)", flush=True)
    else:
        raise ValueError("To discard near-limb records, LON_FWT must be in the list of keywords")

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
if keep_low_qual_vals and keep_near_limb_recs:
    file_name = "all"
else:
    high_qual_str = "" if keep_low_qual_vals else "hq"
    limb_threshold = int(limb_threshold) \
        if limb_threshold.is_integer() else limb_threshold
    near_center_str = "" if keep_near_limb_recs else f"nc{limb_threshold}"
    file_name = "_".join(filter(None, [high_qual_str, near_center_str]))
data.to_parquet(f"processed/{file_name}.parquet")
elapsed_time = time.time() - start_time
print(f"\rSaving the DataFrame ({int(elapsed_time)}s)", flush=True)
print("Done")