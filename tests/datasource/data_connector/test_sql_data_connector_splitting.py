from typing import List

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING: str = "postgresql+psycopg2://postgres:@localhost/test_ci"
TAXI_DATA_TABLE_NAME: str = "taxi_data_all_samples"

# Load the first 10 rows of each month of taxi data
load_data_into_test_database(
    table_name=TAXI_DATA_TABLE_NAME,
    csv_paths=[
        f"../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_{year}-{month}.csv"
        for year in ["2018", "2019", "2020"]
        for month in [f"{mo:02d}" for mo in range(1, 12 + 1)]
    ],
    connection_string=CONNECTION_STRING,
    convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
)

YEARS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="AS")
    .to_pydatetime()
    .tolist()
)
MONTHS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="MS")
    .to_pydatetime()
    .tolist()
)
DAYS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="D")
    .to_pydatetime()
    .tolist()
)


@pytest.mark.parametrize(
    "splitter_method,num_expected_batch_definitions,expected_pickup_datetimes",
    [
        pytest.param("_split_on_year", 3, YEARS_IN_TAXI_DATA, id="_split_on_year"),
        pytest.param("_split_on_month", 36, MONTHS_IN_TAXI_DATA, id="_split_on_month"),
    ],
)
def test__split_on_year_configured_asset_sql_data_connector(
    splitter_method,
    num_expected_batch_definitions,
    expected_pickup_datetimes,
    data_context_with_datasource_postgresql_engine_no_data_connectors,
):

    # 1. Get conftest data_context with datasource which connects to TAXI_DATA_TABLE_NAME
    context: DataContext = (
        data_context_with_datasource_postgresql_engine_no_data_connectors
    )

    datasource_name: str = list(context.datasources.keys())[0]
    data_connector_name: str = "test_data_connector"
    data_asset_name: str = TAXI_DATA_TABLE_NAME

    # 2. Set splitter in data connector config
    data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name=data_connector_name,
        datasource_name=datasource_name,
        execution_engine=context.datasources["my_datasource"].execution_engine,
        assets={
            data_asset_name: {
                "splitter_method": splitter_method,
                "splitter_kwargs": {"column_name": "pickup_datetime"},
            }
        },
    )

    batch_request: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
    )

    # 3. Check if resulting batches are as expected
    # using data_connector.get_batch_definition_list_from_batch_request()
    batch_definition_list: List[
        BatchDefinition
    ] = data_connector.get_batch_definition_list_from_batch_request(batch_request)

    assert len(batch_definition_list) == num_expected_batch_definitions

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_identifiers=IDDict({"pickup_datetime": pickup_datetime}),
        )
        for pickup_datetime in expected_pickup_datetimes
    ]

    assert set(batch_definition_list) == set(expected_batch_definition_list)
