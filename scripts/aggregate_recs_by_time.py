import argparse
import numpy as np
import os
import pandas as pd
import time

parser = argparse.ArgumentParser(description="Aggregate records for different HARPs at each time")
parser.add_argument("--series", type=str, required=True, help="A data series for which raw data has been processed, e.g., hmi.sharp_cea_720s")
parser.add_argument("--use_low_qual_recs", action="store_true", help="Whether to use low-quality records when aggregating (default: False)")
parser.add_argument("--use_near_limb_recs", action="store_true", help="Whether to use records for near-limb HARPs when aggregating (default: False)")
parser.add_argument("--limb_threshold", type=float, default=70, help="Longitude threshold defining where the limb starts (default: 70)")
cmd_args = parser.parse_args()
series = cmd_args.series
series_no_dot = series.replace(".", "_", 1)
use_low_qual_recs = cmd_args.use_low_qual_recs
use_near_limb_recs = cmd_args.use_near_limb_recs
limb_threshold = cmd_args.limb_threshold

cols = [
    # SHARP parameters
    "USFLUX", "MEANGAM", "MEANGBT", "MEANGBZ", "MEANGBH", "MEANJZD", "TOTUSJZ", "MEANALP",
    "MEANJZH", "TOTUSJH", "ABSNJZH", "SAVNCPP", "MEANPOT", "TOTPOT", "MEANSHR", "SHRGT45",
    # Patch areas and pixel counts
    "NPIX", "SIZE", "AREA", "NACR", "SIZE_ACR", "AREA_ACR",
]

data = pd.read_parquet(f"../data/processed/{series_no_dot}.parquet")

def combine_col_vals(group: pd.DataFrame, col: str, weights: pd.Series) -> float:
    """
    Combine values in a given column of a given group of records using given weights.

    :param group: Pandas DataFrame containing the records.
    :param col: String specifying the column that contains the values.
    :param weights: Pandas Series containing the weights to use.
    :return: Float that equals the combination of the values.
    """
    is_non_na = group[col].notna()
    if is_non_na.sum() == 0:
        combined_val = np.nan
    else:
        non_na_vals = group[col][is_non_na]
        non_na_weights = weights[is_non_na]
        weights_sum = non_na_weights.sum()
        if np.isclose(weights_sum, 0): # atol's default value of 1e-8 is being used, so the weights shouldn't be very close to zero
            combined_val = np.nan # Is this really appropriate? It probably is if the weights are USFLUX values, but otherwise?
        else:
            combined_val = (non_na_weights * non_na_vals).sum() / weights_sum
    return combined_val

def combine_recs(
        group: pd.DataFrame,
        cols: list[str],
        use_low_qual_recs: bool = False,
        use_near_limb_recs: bool = False,
        limb_threshold: float = 70
    ) -> dict:
    """
    Combine the records in a given group of records using only the specified columns, with some records optionally excluded.

    :param group: Pandas DataFrame containing the records.
    :param cols: List of strings giving the columns to use.
    :param use_low_qual_recs: Boolean indicating whether to use records flagged as being of low quality.
    :param use_near_limb_recs: Boolean indicating whether to use records for near-limb HARPs.
    :param limb_threshold: Float giving the longitude threshold that defines where the limb begins.
    :return: Dictionary whose keys are the column names and whose values are the combined values for the columns.
    """
    recs_to_use = group["USFLUX"].notna()
    if not use_low_qual_recs:
        recs_to_use &= group["QUALITY"] == 0
    if not use_near_limb_recs:
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
        recs_to_use &= group["LON_FWT"].abs() <= limb_threshold # Note that np.nan <= limb_threshold evaluates to False
    group = group[recs_to_use]
    # USFLUX can equal zero; for example, it equals zero for HARP 11 at 2010-05-06 10:12:00. Using atol's default value of 1e-8 is okay here
    # since nonzero USFLUX values are many orders of magnitude larger.
    if len(group) == 0 or np.isclose(group["USFLUX"].sum(), 0):
        row = {col: np.nan for col in cols}
    else:
        row = {col: combine_col_vals(group, col, weights=group["USFLUX"]) for col in cols}
    return row

print("Aggregating records by time", end="", flush=True)
start_time = time.time()
data = data.groupby("T_REC").apply(
    combine_recs,
    include_groups=False,
    cols=cols, use_low_qual_recs=use_low_qual_recs, use_near_limb_recs=use_near_limb_recs, limb_threshold=limb_threshold
)
data = pd.DataFrame(data.to_list(), index=data.index).reset_index(names="T_REC")
elapsed_time = time.time() - start_time
print(f"\rAggregating records by time ({int(elapsed_time)}s)", flush=True)

print("Saving the DataFrame", end="", flush=True)
start_time = time.time()
high_qual_suffix = "_high-qual" if not use_low_qual_recs else ""
near_center_suffix = f"_near-center-{limb_threshold}" if not use_near_limb_recs else ""
data.to_parquet(f"../data/processed/aggregated{high_qual_suffix}{near_center_suffix}.parquet")
elapsed_time = time.time() - start_time
print(f"\rSaving the DataFrame ({int(elapsed_time)}s)", flush=True)
print("Done")