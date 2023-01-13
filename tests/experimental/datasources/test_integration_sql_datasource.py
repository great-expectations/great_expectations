import os
import pathlib

import pytest

from great_expectations import DataContext
from great_expectations.data_context.util import file_relative_path
from great_expectations.experimental.datasources.sql_datasource import TableAsset
from tests.experimental.datasources.integration_test_util import (
    run_checkpoint_and_datadoc_on_taxi_data_2019_01,
    run_data_assistant_and_checkpoint_on_month_of_taxi_data,
    run_multibatch_data_assistant_and_checkpoint_on_year_of_taxi_data,
)


@pytest.mark.integration
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(empty_data_context, include_rendered_content):
    """An integration test for running checkpoints on sqlalchemy datasources.

    This test does the following:
    1. Creates a brand new datasource using a sqlalchemy backend.
    2. Creates an expectation suite associated with this datasource.
    3. Runs the checkpoint and validates that it ran correctly.
    4. Creates datadocs from the checkpoint run and checks that no errors occurred.
    """
    db_file = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "sqlite",
            "yellow_tripdata.db",
        ),
    )
    context: DataContext = empty_data_context

    # Add sqlalchemy datasource.
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    # Add and configure a data asset
    table = "yellow_tripdata_sample_2019_01"
    split_col = "pickup_datetime"
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name=table,
        )
        .add_year_and_month_splitter(column_name=split_col)
        .add_sorters(["year", "month"])
    )
    batch_request = asset.get_batch_request({"year": 2019, "month": 1})
    run_checkpoint_and_datadoc_on_taxi_data_2019_01(
        context, include_rendered_content, batch_request
    )


@pytest.mark.integration
@pytest.mark.slow  # 7s
def test_run_data_assistant_and_checkpoint(empty_data_context):
    """Test using data assistants to create expectation suite and run checkpoint"""
    context: DataContext = empty_data_context
    path = pathlib.Path(
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata.db"
    )
    asset = _setup_table_asset(
        context=context,
        sqlite_db_file_relative_path=str(path),
        table_name="yellow_tripdata_sample_2019_01",
        date_splitter_column="pickup_datetime",
    )
    batch_request = asset.get_batch_request(options={"year": 2019, "month": 1})
    run_data_assistant_and_checkpoint_on_month_of_taxi_data(context, batch_request)


@pytest.mark.integration
@pytest.mark.slow  # 32s
def test_run_multibatch_data_assistant_and_checkpoint(empty_data_context):
    """Test using data assistants to create expectation suite and run checkpoint"""
    context: DataContext = empty_data_context
    db_path = pathlib.Path(
        "../../test_sets/taxi_yellow_tripdata_samples/sqlite/yellow_tripdata_sample_2020_all_months_combined.db"
    )
    asset = _setup_table_asset(
        context=empty_data_context,
        sqlite_db_file_relative_path=str(db_path),
        table_name="yellow_tripdata_sample_2020",
        date_splitter_column="pickup_datetime",
    )
    batch_request = asset.get_batch_request(options={"year": 2020})
    run_multibatch_data_assistant_and_checkpoint_on_year_of_taxi_data(
        context, batch_request
    )


def _setup_table_asset(
    context: DataContext,
    sqlite_db_file_relative_path: str,
    table_name: str,
    date_splitter_column: str,
) -> TableAsset:
    db_file = file_relative_path(__file__, sqlite_db_file_relative_path)
    # Add sqlalchemy datasource.
    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    # Add and configure a data asset
    table = table_name
    split_col = date_splitter_column
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name=table,
        )
        .add_year_and_month_splitter(column_name=split_col)
        .add_sorters(["year", "month"])
    )
    return asset
