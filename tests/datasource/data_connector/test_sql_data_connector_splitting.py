from typing import List

import pandas as pd
import pytest

from great_expectations import DataContext
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import DatePart
from tests.test_utils import load_data_into_test_database

CONNECTION_STRING: str = "postgresql+psycopg2://postgres:@localhost/test_ci"
TAXI_DATA_TABLE_NAME: str = "taxi_data_all_samples"

# Load the first 10 rows of each month of taxi data
test_df = load_data_into_test_database(
    table_name=TAXI_DATA_TABLE_NAME,
    csv_paths=[
        f"../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_{year}-{month}.csv"
        for year in ["2018", "2019", "2020"]
        for month in [f"{mo:02d}" for mo in range(1, 12 + 1)]
    ],
    connection_string=CONNECTION_STRING,
    convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
)

TEST_COLUMN: str = "pickup_datetime"
YEAR_WEEK_BATCH_IDENTIFIER_DATA: List[dict] = list(
    {val[0]: val[1], val[2]: val[3]}
    for val in {
        (DatePart.YEAR.value, dt.year, DatePart.WEEK.value, dt.week)
        for dt in test_df[TEST_COLUMN]
    }
)
YEAR_WEEK_BATCH_IDENTIFIER_DATA: List[dict] = sorted(
    YEAR_WEEK_BATCH_IDENTIFIER_DATA,
    key=lambda x: (
        x[DatePart.YEAR.value],
        x[DatePart.WEEK.value],
    ),
)

# test_df.loc[:, "year"] = test_df[TEST_COLUMN].dt.year
# test_df.loc[:, "month"] = test_df[TEST_COLUMN].dt.month
# test_df.loc[:, "day"] = test_df[TEST_COLUMN].dt.day


YEAR_MONTH_DAY_BATCH_IDENTIFIER_DATA: List[dict] = list(
    {val[0]: val[1], val[2]: val[3], val[4]: val[5]}
    for val in {
        (
            DatePart.YEAR.value,
            dt.year,
            DatePart.MONTH.value,
            dt.month,
            DatePart.DAY.value,
            dt.day,
        )
        for dt in test_df[TEST_COLUMN]
    }
)
YEAR_MONTH_DAY_BATCH_IDENTIFIER_DATA: List[dict] = sorted(
    YEAR_MONTH_DAY_BATCH_IDENTIFIER_DATA,
    key=lambda x: (
        x[DatePart.YEAR.value],
        x[DatePart.MONTH.value],
        x[DatePart.DAY.value],
    ),
)


YEARS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="AS")
    .to_pydatetime()
    .tolist()
)
YEAR_STRINGS_IN_TAXI_DATA = [str(y.year) for y in YEARS_IN_TAXI_DATA]
YEAR_BATCH_IDENTIFIER_DATA: List[dict] = [
    {DatePart.YEAR.value: dt.year} for dt in YEARS_IN_TAXI_DATA
]

MONTHS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="MS")
    .to_pydatetime()
    .tolist()
)
MONTH_STRINGS_IN_TAXI_DATA = [str(mo) for mo in range(1, 12 + 1)]
MONTH_BATCH_IDENTIFIER_DATA: List[dict] = [
    {DatePart.MONTH.value: dt.month} for dt in MONTHS_IN_TAXI_DATA
]

DAYS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="D")
    .to_pydatetime()
    .tolist()
)


# TODO: AJB 20220412 move this test to integration tests
@pytest.mark.integration
@pytest.mark.parametrize(
    "splitter_method,splitter_kwargs,num_expected_batch_definitions,num_expected_rows_in_first_batch_definition,expected_pickup_datetimes",
    [
        pytest.param(
            "split_on_year",
            {"column_name": "pickup_datetime"},
            3,
            120,
            YEAR_BATCH_IDENTIFIER_DATA,
            id="split_on_year",
        ),
        pytest.param(
            "split_on_year_and_month",
            # {"column_name": "pickup_datetime", "date_parts": [DatePart.YEAR, DatePart.MONTH]},
            {"column_name": "pickup_datetime"},
            36,
            10,
            # [dt.strftime("year_%Ymonth_%-m") for dt in MONTHS_IN_TAXI_DATA],
            [
                {DatePart.YEAR.value: dt.year, DatePart.MONTH.value: dt.month}
                for dt in MONTHS_IN_TAXI_DATA
            ],
            id="split_on_year_and_month",
        ),
        pytest.param(
            "split_on_year_and_month_and_day",
            {"column_name": "pickup_datetime"},
            299,
            2,
            YEAR_MONTH_DAY_BATCH_IDENTIFIER_DATA,
            id="split_on_year_and_month_and_day",
        ),
        pytest.param(
            "split_on_date_parts",
            {"column_name": "pickup_datetime", "date_parts": [DatePart.MONTH]},
            12,
            30,
            MONTH_BATCH_IDENTIFIER_DATA,
            id="split_on_date_parts: month",
        )
        # pytest.param(
        #     "split_on_year_and_week",
        #     {"column_name": "pickup_datetime"},
        #     143,
        #     7,  # TODO: AJB 20220414 This includes 2 instances of 12-31-2018 which should not be there
        #     YEAR_WEEK_BATCH_IDENTIFIER_DATA,
        #     id="split_on_year_and_week"
        # ),
    ],
)
def test__split_on_x_configured_asset_sql_data_connector(
    splitter_method,
    splitter_kwargs,
    num_expected_batch_definitions,
    num_expected_rows_in_first_batch_definition,
    expected_pickup_datetimes,
    data_context_with_datasource_postgresql_engine_no_data_connectors,
    sa,
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
                "splitter_kwargs": splitter_kwargs,
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

    # 4. Check that loaded data is as expected

    # Use expected_batch_definition_list since it is sorted, and we already
    # asserted that it contains the same items as batch_definition_list
    batch_spec: SqlAlchemyDatasourceBatchSpec = data_connector.build_batch_spec(
        expected_batch_definition_list[0]
    )

    batch_data: SqlAlchemyBatchData = context.datasources[
        "my_datasource"
    ].execution_engine.get_batch_data(batch_spec=batch_spec)

    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == num_expected_rows_in_first_batch_definition
