import datetime
from dataclasses import dataclass
from typing import List, Tuple, Union

import pandas as pd
import sqlalchemy as sa

import great_expectations as ge
from great_expectations import DataContext
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition, BatchRequest
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from tests.test_utils import (
    get_bigquery_connection_url,
    get_snowflake_connection_url,
    load_data_into_test_database,
)

yaml_handler: YAMLHandler = YAMLHandler()


def _get_connection_string_and_dialect() -> Tuple[str, str]:

    with open("connection_string.yml") as f:
        db_config: dict = yaml_handler.load(f)

    dialect: str = db_config["dialect"]
    if dialect == "snowflake":
        CONNECTION_STRING: str = get_snowflake_connection_url()
    elif dialect == "bigquery":
        CONNECTION_STRING: str = get_bigquery_connection_url()
    else:
        CONNECTION_STRING: str = db_config["connection_string"]

    return dialect, CONNECTION_STRING

TAXI_DATA_TABLE_NAME: str = "taxi_data_all_samples"

def _load_data(connection_string: str, table_name: str = TAXI_DATA_TABLE_NAME):

    # Load the first 10 rows of each month of taxi data
    load_data_into_test_database(
        table_name=table_name,
        csv_paths=[
            f"./data/yellow_tripdata_sample_{year}-{month}.csv"
            for year in ["2018", "2019", "2020"]
            for month in [f"{mo:02d}" for mo in range(1, 12 + 1)]
        ],
        connection_string=connection_string,
        convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
    )

dialect, CONNECTION_STRING = _get_connection_string_and_dialect()

_load_data(connection_string=CONNECTION_STRING)

print(f"Testing dialect: {dialect}")

YEARS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="AS")
    .to_pydatetime()
    .tolist()
)
YEAR_STRINGS_IN_TAXI_DATA = [str(y.year) for y in YEARS_IN_TAXI_DATA]

MONTHS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="MS")
    .to_pydatetime()
    .tolist()
)
MONTH_STRINGS_IN_TAXI_DATA = [str(mo) for mo in range(1, 12 + 1)]

DAYS_IN_TAXI_DATA = (
    pd.date_range(start="2018-01-01", end="2020-12-31", freq="D")
    .to_pydatetime()
    .tolist()
)


@dataclass
class TestCase:
    splitter_method_name: str
    num_expected_batch_definitions: int
    num_expected_rows_in_first_batch_definition: int
    expected_pickup_datetimes: Union[List[datetime.datetime], List[str]]


test_cases: List[TestCase] = []

non_truncating_test_cases: List[TestCase] = [
    TestCase(
        splitter_method_name="_split_on_year",
        num_expected_batch_definitions=3,
        num_expected_rows_in_first_batch_definition=120,
        expected_pickup_datetimes=YEAR_STRINGS_IN_TAXI_DATA
    ),
    TestCase(
        splitter_method_name="_split_on_month",
        num_expected_batch_definitions=12,
        num_expected_rows_in_first_batch_definition=30,
        expected_pickup_datetimes=MONTH_STRINGS_IN_TAXI_DATA
    ),
]
truncating_test_cases: List[TestCase] = [
    TestCase(
        splitter_method_name="_split_on_truncated_year",
        num_expected_batch_definitions=3,
        num_expected_rows_in_first_batch_definition=120,
        expected_pickup_datetimes=YEARS_IN_TAXI_DATA,
    ),
    TestCase(
        splitter_method_name="_split_on_truncated_month",
        num_expected_batch_definitions=36,
        num_expected_rows_in_first_batch_definition=10,
        expected_pickup_datetimes=MONTHS_IN_TAXI_DATA,
    ),
]

test_cases.extend(non_truncating_test_cases)
# TODO: AJB 20220412 Enable these tests after enabling truncating in these dialects.
if dialect not in ["mysql", "mssql"]:
    test_cases.extend(truncating_test_cases)


for test_case in test_cases:

    print("Testing splitter method:", test_case.splitter_method_name)

    # 1. Setup

    context: DataContext = ge.get_context()

    datasource_name: str = "test_datasource"
    context.add_datasource(
        name=datasource_name,
        class_name="Datasource",
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": CONNECTION_STRING,
        },
    )

    # 2. Set splitter in data connector config
    data_connector_name: str = "test_data_connector"
    data_asset_name: str = TAXI_DATA_TABLE_NAME
    column_name: str = "pickup_datetime"
    data_connector: ConfiguredAssetSqlDataConnector = ConfiguredAssetSqlDataConnector(
        name=data_connector_name,
        datasource_name=datasource_name,
        execution_engine=context.datasources[datasource_name].execution_engine,
        assets={
            data_asset_name: {
                "splitter_method": test_case.splitter_method_name,
                "splitter_kwargs": {"column_name": column_name},
            }
        },
    )

    # 3. Check if resulting batches are as expected
    # using data_connector.get_batch_definition_list_from_batch_request()
    batch_request: BatchRequest = BatchRequest(
        datasource_name=datasource_name,
        data_connector_name=data_connector_name,
        data_asset_name=data_asset_name,
    )
    batch_definition_list: List[
        BatchDefinition
    ] = data_connector.get_batch_definition_list_from_batch_request(batch_request)

    assert len(batch_definition_list) == test_case.num_expected_batch_definitions

    expected_batch_definition_list: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_identifiers=IDDict({column_name: pickup_datetime}),
        )
        for pickup_datetime in test_case.expected_pickup_datetimes
    ]

    assert set(batch_definition_list) == set(expected_batch_definition_list), f"BatchDefinition lists don't match\n\nbatch_definition_list:\n{batch_definition_list}\n\nexpected_batch_definition_list:\n{expected_batch_definition_list}"

    # 4. Check that loaded data is as expected

    batch_spec: SqlAlchemyDatasourceBatchSpec = data_connector.build_batch_spec(
        batch_definition_list[0]
    )

    batch_data: SqlAlchemyBatchData = context.datasources[
        datasource_name
    ].execution_engine.get_batch_data(batch_spec=batch_spec)

    num_rows: int = batch_data.execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(batch_data.selectable)
    ).scalar()
    assert num_rows == test_case.num_expected_rows_in_first_batch_definition
