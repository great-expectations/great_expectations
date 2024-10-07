from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import TYPE_CHECKING, List

import sqlalchemy as sa

import great_expectations as gx
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent import GxDatasourceWarning
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from tests.integration.fixtures.partition_and_sample_data.partitioner_test_cases_and_fixtures import (  # noqa: E501
    TaxiPartitioningTestCase,
    TaxiPartitioningTestCasesBase,
)
from tests.test_utils import (
    LoadedTable,
    add_datasource,
    clean_up_tables_with_prefix,
    load_and_concatenate_csvs,
    load_data_into_test_database,
)

if TYPE_CHECKING:
    import pandas as pd

TAXI_DATA_TABLE_NAME: str = "taxi_data_all_samples"


def _load_data(
    connection_string: str,
    dialect: str,
    table_name: str = TAXI_DATA_TABLE_NAME,
    random_table_suffix: bool = True,
) -> LoadedTable:
    dialects_supporting_multiple_values_in_single_insert_clause: List[str] = ["redshift"]
    to_sql_method: str = (
        "multi" if dialect in dialects_supporting_multiple_values_in_single_insert_clause else None
    )

    # Load the first 10 rows of each month of taxi data
    return load_data_into_test_database(
        table_name=table_name,
        csv_paths=[
            "./data/ten_trips_from_each_month/yellow_tripdata_sample_10_trips_from_each_month.csv"
        ],
        connection_string=connection_string,
        convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        load_full_dataset=True,
        random_table_suffix=random_table_suffix,
        to_sql_method=to_sql_method,
    )


def _is_dialect_athena(dialect: str) -> bool:
    """Is the dialect awsathena?"""
    return dialect == "awsathena"


@contextmanager
def loaded_table(dialect: str, connection_string: str) -> LoadedTable:
    test_df: pd.DataFrame
    table_name: str
    loaded_table: LoadedTable
    if _is_dialect_athena(dialect):
        table_name = "ten_trips_from_each_month"
        test_df = load_and_concatenate_csvs(
            csv_paths=[
                "./data/ten_trips_from_each_month/yellow_tripdata_sample_10_trips_from_each_month.csv"
            ],
            convert_column_names_to_datetime=["pickup_datetime", "dropoff_datetime"],
            load_full_dataset=True,
        )
        loaded_table = LoadedTable(
            table_name=table_name,
            inserted_dataframe=test_df,
        )
    else:
        loaded_table = _load_data(connection_string=connection_string, dialect=dialect)

    try:
        yield loaded_table
    finally:
        if not _is_dialect_athena(dialect):
            print("Cleaning up created loaded table")
            clean_up_tables_with_prefix(
                connection_string=connection_string,
                table_prefix=loaded_table.table_name,
            )


def _execute_taxi_partitioning_test_cases(
    taxi_partitioning_test_cases: TaxiPartitioningTestCasesBase,
    connection_string: str,
    table_name: str,
) -> None:
    test_cases: List[TaxiPartitioningTestCase] = taxi_partitioning_test_cases.test_cases()

    test_case: TaxiPartitioningTestCase
    for test_case in test_cases:
        print("Testing add_batch_definition_* method", test_case.add_batch_definition_method_name)

        # 1. Setup

        context = gx.get_context(mode="ephemeral")

        datasource_name: str = "test_datasource"
        batch_definition_name: str = "test_batch_definition"
        data_asset_name: str = table_name  # Read from generated table name

        column_name: str = taxi_partitioning_test_cases.test_column_name
        column_names: List[str] = taxi_partitioning_test_cases.test_column_names

        # 2. Set partitioner in DataConnector config
        datasource = add_datasource(
            context, name=datasource_name, connection_string=connection_string
        )

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "once",
                message="The `schema_name argument` is deprecated",
                category=DeprecationWarning,
            )
            warnings.filterwarnings(
                "once",
                message="schema_name None does not match datasource schema",
                category=GxDatasourceWarning,
            )

            asset = datasource.add_table_asset(
                data_asset_name, table_name=table_name, schema_name=None
            )

        add_batch_definition_method = getattr(
            asset, test_case.add_batch_definition_method_name or "MAKE THIS REQUIRED"
        )
        batch_definition: BatchDefinition = add_batch_definition_method(
            name=batch_definition_name, **test_case.add_batch_definition_kwargs
        )

        # 3. Check if resulting batches are as expected
        batch_request = batch_definition.build_batch_request()
        batch_identifiers_list = asset.get_batch_identifiers_list(batch_request)
        assert len(batch_identifiers_list) == test_case.num_expected_batch_definitions, (
            f"Found {len(batch_identifiers_list)} batch definitions "
            f"but expected {test_case.num_expected_batch_definitions}"
        )

        expected_batch_metadata: List[dict]

        if test_case.table_domain_test_case:
            expected_batch_metadata = [{}]
        elif column_name or column_names:
            # This condition is a smell. Consider refactoring.
            expected_batch_metadata = [data for data in test_case.expected_column_values]
        else:
            raise ValueError("Missing test_column_names or test_column_names attribute.")

        assert batch_identifiers_list == expected_batch_metadata, (
            f"Batch metadata lists don't match.\n\n"
            f"batch_list:\n{batch_identifiers_list}\n\n"
            f"expected_batch metadata:\n{expected_batch_metadata}"
        )

        # 4. Check that loaded data is as expected, using correctness
        # of arbitrary batch as a proxy for correctness of the whole list

        batch = asset.get_batch(batch_request)
        execution_engine = datasource.get_execution_engine()
        batch_data: SqlAlchemyBatchData = execution_engine.get_batch_data(
            batch_spec=batch.batch_spec
        )

        num_rows: int = execution_engine.execute_query(
            sa.select(sa.func.count()).select_from(batch_data.selectable)
        ).scalar()
        assert num_rows == test_case.num_expected_rows_in_first_batch_definition
