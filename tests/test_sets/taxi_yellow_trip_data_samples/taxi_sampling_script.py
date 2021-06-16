"""
The purpose of this script is to automate sampling from NYC yellow taxi trip records.
This data is to be used within GE's testing suite.

CSV's used herein can be found at: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

If you'd like to run this script, please download your desired CSVs' from the above site and store them
in a directory (see 'INPUT_DIR' below) before program execution.
"""

from typing import List
import re
import numpy as np
import os
import pandas as pd
import tqdm

# Configure these dirs as desired
INPUT_DIR: str = "data"
OUTPUT_DIR: str = "out"

COLUMNS: List[str] = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]


def sample_taxi_data(filename: str, sample_size: int = 10_000) -> None:
    """
    Read an input CSV, ensure consistency with other datasets, and output samples to a new file.

    Args:
        filename: the relative path of the input CSV
        sample_size: the number of rows to select for the output CSV
    """

    # Certain columns have mixed types so we explicitly define what we're expecting
    df: pd.DataFrame = pd.read_csv(
        filename,
        dtype={
            "VendorID": pd.Int64Dtype(),
            "passenger_count": pd.Int64Dtype(),
            "RatecodeID": pd.Int64Dtype(),
            "payment_type": pd.Int64Dtype(),
        },
    )

    # 2018 data does not include this column so we add a placeholder for such edge cases
    if "congestion_surcharge" not in df:
        df["congestion_surcharge"] = np.nan

    df = df.sample(n=sample_size)
    df.reset_index(drop=True, inplace=True)
    df.columns = COLUMNS  # Rename columns to ensure consistency between samples

    date: str = re.search(r"\d{4}-\d{2}", filename).group()
    outfile: str = f"yellow_trip_data_sample_{date}.csv"
    df.to_csv(outfile, index=False)


def main():
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=False)
        for file in tqdm.tqdm(os.listdir(INPUT_DIR)):
            sample_taxi_data(os.path.join(INPUT_DIR, file))
        print("SUCCESS: Created valid samples for all input CSV's")

    except Exception as e:
        print(f"FAILURE: Something went wrong; {e}")


if __name__ == "__main__":
    main()
