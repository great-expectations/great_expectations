import pandas as pd
import pytest


@pytest.fixture
def mini_taxi_df() -> pd.DataFrame:
    """
    Returns: pandas dataframe that contains a small selection of columns and rows from taxi_data, for unittesting.
    """
    df = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4],
            "vendor_id": [1, 1, 1, 1, 1],
            "pickup_datetime": [
                "2019-01-15 3:36:12",
                "2019-01-25 18:20:32",
                "2019-01-05 6:47:31",
                "2019-01-09 15:08:02",
                "2019-01-25 18:49:51",
            ],
            "dropoff_datetime": [
                "2019-01-15 3:42:19",
                "2019-01-25 18:26:55",
                "2019-01-05 6:52:19",
                "2019-01-09 15:20:17",
                "2019-01-25 18:56:44",
            ],
            "trip_distance": [1, 0.8, 1.1, 2.5, 0.8],
            "tip_amount": [1.95, 1.55, 0, 3, 1.65],
            "total_amount": [9.75, 9.35, 6.8, 14.8, 9.95],
        }
    )
    return df


@pytest.fixture
def animal_table_df() -> pd.DataFrame:
    """
    Returns: pandas dataframe that contains example data for unexpected_index_column_names metric tests
    """
    df = pd.DataFrame(
        {
            "pk_1": [0, 1, 2, 3, 4, 5],
            "pk_2": ["zero", "one", "two", "three", "four", "five"],
            "animals": [
                "cat",
                "fish",
                "dog",
                "giraffe",
                "lion",
                "zebra",
            ],
        }
    )
    return df


@pytest.fixture
def metric_value_kwargs_complete() -> dict:
    """
    Test configuration for metric_value_kwargs. Contains `unexpected_index_column_names` key
    """
    return {
        "value_set": ["cat", "fish", "dog"],
        "parse_strings_as_datetimes": False,
        "result_format": {
            "result_format": "COMPLETE",
            "unexpected_index_column_names": ["pk_1"],
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
    }
