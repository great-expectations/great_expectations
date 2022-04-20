from dataclasses import dataclass
from typing import List, Tuple

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
from great_expectations.execution_engine.sqlalchemy_data_splitter import DatePart
from tests.test_utils import (
    LoadedTable,
    clean_up_tables_with_prefix,
    get_bigquery_connection_url,
    get_snowflake_connection_url,
    load_data_into_test_database,
)

yaml_handler: YAMLHandler = YAMLHandler()


def _get_connection_string_and_dialect() -> Tuple[str, str]:

    with open("./connection_string.yml") as f:
        db_config: dict = yaml_handler.load(f)

    dialect: str = db_config["dialect"]
    if dialect == "snowflake":
        connection_string: str = get_snowflake_connection_url()
    elif dialect == "bigquery":
        connection_string: str = get_bigquery_connection_url()
    else:
        connection_string: str = db_config["connection_string"]

    return dialect, connection_string


TAXI_DATA_TABLE_NAME: str = "taxi_data_all_samples"


def _load_data(
    connection_string: str, table_name: str = TAXI_DATA_TABLE_NAME
) -> LoadedTable:

    # Load the first 10 rows of each month of taxi data
    return load_data_into_test_database(
        table_name=table_name,
        csv_paths=[
            f"./data/yellow_tripdata_sample_{year}-{month}.csv"
            for year in ["2018", "2019", "2020"]
            for month in [f"{mo:02d}" for mo in range(1, 12 + 1)]
        ],
        connection_string=connection_string,
        convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        random_table_suffix=True,
    )


if __name__ == "test_script_module":

    dialect, connection_string = _get_connection_string_and_dialect()
    print(f"Testing dialect: {dialect}")

    print("Preemptively cleaning old tables")
    clean_up_tables_with_prefix(
        connection_string=connection_string, table_prefix=f"{TAXI_DATA_TABLE_NAME}_"
    )

    loaded_table: LoadedTable = _load_data(connection_string=connection_string)
    test_df: pd.DataFrame = loaded_table.inserted_dataframe
    table_name: str = loaded_table.table_name

    YEARS_IN_TAXI_DATA = (
        pd.date_range(start="2018-01-01", end="2020-12-31", freq="AS")
        .to_pydatetime()
        .tolist()
    )
    YEAR_BATCH_IDENTIFIER_DATA: List[dict] = [
        {DatePart.YEAR.value: dt.year} for dt in YEARS_IN_TAXI_DATA
    ]

    MONTHS_IN_TAXI_DATA = (
        pd.date_range(start="2018-01-01", end="2020-12-31", freq="MS")
        .to_pydatetime()
        .tolist()
    )
    YEAR_MONTH_BATCH_IDENTIFIER_DATA: List[dict] = [
        {DatePart.YEAR.value: dt.year, DatePart.MONTH.value: dt.month}
        for dt in MONTHS_IN_TAXI_DATA
    ]
    MONTH_BATCH_IDENTIFIER_DATA: List[dict] = [
        {DatePart.MONTH.value: dt.month} for dt in MONTHS_IN_TAXI_DATA
    ]

    TEST_COLUMN: str = "pickup_datetime"

    # Since taxi data does not contain all days, we need to introspect the data to build the fixture:
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

    @dataclass
    class SqlSplittingTestCase:
        splitter_method_name: str
        splitter_kwargs: dict
        num_expected_batch_definitions: int
        num_expected_rows_in_first_batch_definition: int
        expected_pickup_datetimes: List[dict]

    test_cases: List[SqlSplittingTestCase] = [
        SqlSplittingTestCase(
            splitter_method_name="split_on_year",
            splitter_kwargs={"column_name": TEST_COLUMN},
            num_expected_batch_definitions=3,
            num_expected_rows_in_first_batch_definition=120,
            expected_pickup_datetimes=YEAR_BATCH_IDENTIFIER_DATA,
        ),
        SqlSplittingTestCase(
            splitter_method_name="split_on_year_and_month",
            splitter_kwargs={"column_name": TEST_COLUMN},
            num_expected_batch_definitions=36,
            num_expected_rows_in_first_batch_definition=10,
            expected_pickup_datetimes=YEAR_MONTH_BATCH_IDENTIFIER_DATA,
        ),
        SqlSplittingTestCase(
            splitter_method_name="split_on_year_and_month_and_day",
            splitter_kwargs={"column_name": TEST_COLUMN},
            num_expected_batch_definitions=299,
            num_expected_rows_in_first_batch_definition=2,
            expected_pickup_datetimes=YEAR_MONTH_DAY_BATCH_IDENTIFIER_DATA,
        ),
        SqlSplittingTestCase(
            splitter_method_name="split_on_date_parts",
            splitter_kwargs={
                "column_name": TEST_COLUMN,
                "date_parts": [DatePart.MONTH],
            },
            num_expected_batch_definitions=12,
            num_expected_rows_in_first_batch_definition=30,
            expected_pickup_datetimes=MONTH_BATCH_IDENTIFIER_DATA,
        ),
        # date_parts as a string (with mixed case):
        SqlSplittingTestCase(
            splitter_method_name="split_on_date_parts",
            splitter_kwargs={"column_name": TEST_COLUMN, "date_parts": ["mOnTh"]},
            num_expected_batch_definitions=12,
            num_expected_rows_in_first_batch_definition=30,
            expected_pickup_datetimes=MONTH_BATCH_IDENTIFIER_DATA,
        ),
        # Mix of types of date_parts:
        SqlSplittingTestCase(
            splitter_method_name="split_on_date_parts",
            splitter_kwargs={
                "column_name": TEST_COLUMN,
                "date_parts": [DatePart.YEAR, "month"],
            },
            num_expected_batch_definitions=36,
            num_expected_rows_in_first_batch_definition=10,
            expected_pickup_datetimes=YEAR_MONTH_BATCH_IDENTIFIER_DATA,
        ),
    ]

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
                "connection_string": connection_string,
            },
        )

        # 2. Set splitter in data connector config
        data_connector_name: str = "test_data_connector"
        data_asset_name: str = table_name  # Read from generated table name
        column_name: str = TEST_COLUMN
        data_connector: ConfiguredAssetSqlDataConnector = (
            ConfiguredAssetSqlDataConnector(
                name=data_connector_name,
                datasource_name=datasource_name,
                execution_engine=context.datasources[datasource_name].execution_engine,
                assets={
                    data_asset_name: {
                        "splitter_method": test_case.splitter_method_name,
                        "splitter_kwargs": test_case.splitter_kwargs,
                    }
                },
            )
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

        assert set(batch_definition_list) == set(
            expected_batch_definition_list
        ), f"BatchDefinition lists don't match\n\nbatch_definition_list:\n{batch_definition_list}\n\nexpected_batch_definition_list:\n{expected_batch_definition_list}"

        # 4. Check that loaded data is as expected

        # Use expected_batch_definition_list since it is sorted, and we already
        # asserted that it contains the same items as batch_definition_list
        batch_spec: SqlAlchemyDatasourceBatchSpec = data_connector.build_batch_spec(
            expected_batch_definition_list[0]
        )

        batch_data: SqlAlchemyBatchData = context.datasources[
            datasource_name
        ].execution_engine.get_batch_data(batch_spec=batch_spec)

        num_rows: int = batch_data.execution_engine.engine.execute(
            sa.select([sa.func.count()]).select_from(batch_data.selectable)
        ).scalar()
        assert num_rows == test_case.num_expected_rows_in_first_batch_definition

    print("Clean up tables used in this test")
    clean_up_tables_with_prefix(
        connection_string=connection_string, table_prefix=f"{TAXI_DATA_TABLE_NAME}_"
    )
