import logging
from typing import List, Optional, Tuple

import pandas as pd
import sqlalchemy as sa

from great_expectations.datasource import BaseDatasource, Datasource, LegacyDatasource
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

logger = logging.getLogger(__name__)


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
from tests.integration.fixtures.split_and_sample_data.splitter_test_cases_and_fixtures import (
    TaxiSplittingTestCase,
    TaxiSplittingTestCases,
    TaxiTestData,
)
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
    connection_string: str, dialect: str, table_name: str = TAXI_DATA_TABLE_NAME
) -> LoadedTable:

    dialects_supporting_multiple_values_in_single_insert_clause: List[str] = [
        "redshift"
    ]
    to_sql_method: str = (
        "multi"
        if dialect in dialects_supporting_multiple_values_in_single_insert_clause
        else None
    )

    # Load the first 10 rows of each month of taxi data
    return load_data_into_test_database(
        table_name=table_name,
        csv_paths=[
            f"./data/ten_trips_from_each_month/yellow_tripdata_sample_10_trips_from_each_month.csv"
        ],
        connection_string=connection_string,
        convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        load_full_dataset=True,
        random_table_suffix=True,
        to_sql_method=to_sql_method,
    )


if __name__ == "test_script_module":

    dialect, connection_string = _get_connection_string_and_dialect()
    print(f"Testing dialect: {dialect}")

    print("Preemptively cleaning old tables")
    clean_up_tables_with_prefix(
        connection_string=connection_string, table_prefix=f"{TAXI_DATA_TABLE_NAME}_"
    )

    loaded_table: LoadedTable = _load_data(
        connection_string=connection_string, dialect=dialect
    )
    test_df: pd.DataFrame = loaded_table.inserted_dataframe
    table_name: str = loaded_table.table_name

    taxi_test_data: TaxiTestData = TaxiTestData(
        test_df, test_column_name="pickup_datetime"
    )
    taxi_splitting_test_cases: TaxiSplittingTestCases = TaxiSplittingTestCases(
        taxi_test_data
    )

    test_cases: List[TaxiSplittingTestCase] = taxi_splitting_test_cases.test_cases()

    for test_case in test_cases:

        print("Testing splitter method:", test_case.splitter_method_name)

        # 1. Setup
        # 2. Set splitter in data connector config
        context: DataContext = ge.get_context()

        datasource_name: str = "test_datasource"
        data_connector_name: str = "test_data_connector"
        data_asset_name: str = table_name  # Read from generated table name
        column_name: str = taxi_splitting_test_cases.test_column_name

        data_connector_config: dict = {
            "class_name": "ConfiguredAssetSqlDataConnector",
            "assets": {
                data_asset_name: {
                    "splitter_method": test_case.splitter_method_name,
                    "splitter_kwargs": test_case.splitter_kwargs,
                }
            },
        }
        context.add_datasource(
            name=datasource_name,
            class_name="Datasource",
            execution_engine={
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": connection_string,
            },
            data_connectors={data_connector_name: data_connector_config},
        )
        test_datasource: Optional[
            LegacyDatasource, BaseDatasource
        ] = context.get_datasource(datasource_name="test_datasource")
        data_connector: ConfiguredAssetSqlDataConnector = (
            test_datasource.data_connectors["test_data_connector"]
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
