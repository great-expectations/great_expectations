# this is where we are going to be doing this
import pandas as pd
import pytest

# TODO:
# 5 expectations. here are the renders.
# and then the tests are going to be working, I think
#
import great_expectations as gx
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.data_context.util import file_relative_path


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
def sql_execution_engine_with_mini_taxi_loaded(sa, mini_taxi_df):
    sqlite_engine = sa.create_engine("sqlite://")
    dataframe = mini_taxi_df
    add_dataframe_to_db(
        df=dataframe,
        name="test_table",
        con=sqlite_engine,
        index=False,
    )
    return sqlite_engine


def test_multi_column_sum(sql_execution_engine_with_mini_taxi_loaded):
    context = gx.get_context()
    datasource_name = "my_datasource"
    asset_name = "asset"

    sqlite_path = file_relative_path(
        __file__, "../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    )
    connection_string = f"sqlite:///{sqlite_path}"

    datasource = context.sources.add_sqlite(
        datasource_name, connection_string=connection_string
    )
    asset = datasource.add_table_asset(
        name=asset_name, table_name="yellow_tripdata_sample_2019_01"
    )
    batch_request = asset.build_batch_request()
    expectation_suite_name = "my_new_expectation_suite"
    context.add_or_update_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name,
    )
    validator.head()
    validator.expect_multicolumn_sum_to_equal(
        column_list=["passenger_count", "rate_code_id"], sum_total=100
    )
    validator.expect_column_values_to_match_like_pattern(
        column="passenger_count", like_pattern="[123]+"
    )
    validator.expect_column_values_to_not_match_like_pattern(
        column="passenger_count", like_pattern="[123]+"
    )
    validator.expect_column_values_to_match_like_pattern_list(
        column="passenger_count", like_pattern_list=["[123]+", "[456]+"]
    )
    validator.expect_column_values_to_not_match_like_pattern_list(
        column="passenger_count", like_pattern_list=["[123]+", "[456]+"]
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    checkpoint = context.add_or_update_checkpoint(
        name="my_quickstart_checkpoint",
        validator=validator,
    )
    checkpoint_result = checkpoint.run(result_format={"result_format": "COMPLETE"})
    print(checkpoint_result.to_json_dict())
