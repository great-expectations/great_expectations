from __future__ import annotations

import pytest

from great_expectations.data_context import AbstractDataContext
from great_expectations.experimental.datasources.interfaces import (
    BatchRequest,
    DataAsset,
    Datasource,
)
from tests.experimental.datasources.integration.conftest import sqlite_datasource
from tests.experimental.datasources.integration.integration_test_utils import (
    run_batch_head,
    run_checkpoint_and_data_doc,
    run_data_assistant_and_checkpoint,
    run_multibatch_data_assistant_and_checkpoint,
)


@pytest.mark.integration
@pytest.mark.parametrize("include_rendered_content", [False, True])
def test_run_checkpoint_and_data_doc(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    include_rendered_content: bool,
):
    run_checkpoint_and_data_doc(
        datasource_test_data=datasource_test_data,
        include_rendered_content=include_rendered_content,
    )


@pytest.mark.integration
@pytest.mark.slow  # sql: 7s  # pandas: 4s
def test_run_data_assistant_and_checkpoint(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
):
    run_data_assistant_and_checkpoint(datasource_test_data=datasource_test_data)


@pytest.mark.integration
@pytest.mark.slow  # sql: 33s  # pandas: 9s
def test_run_multibatch_data_assistant_and_checkpoint(multibatch_datasource_test_data):
    """Test using data assistants to create expectation suite using multiple batches and to run checkpoint"""
    run_multibatch_data_assistant_and_checkpoint(
        multibatch_datasource_test_data=multibatch_datasource_test_data
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ["n_rows", "fetch_all", "success"],
    [
        (None, False, True),
        (3, False, True),
        (7, False, True),
        (-100, False, True),
        ("invalid_value", False, False),
        (1.5, False, False),
        (True, False, False),
        (0, False, True),
        (200000, False, True),
        (1, False, True),
        (-50000, False, True),
        (-5, True, True),
        (0, True, True),
        (3, True, True),
        (50000, True, True),
        (-20000, True, True),
        (None, True, True),
        (15, "invalid_value", False),
    ],
)
def test_batch_head(
    datasource_test_data: tuple[
        AbstractDataContext, Datasource, DataAsset, BatchRequest
    ],
    fetch_all: bool | str,
    n_rows: int | float | str | None,
    success: bool,
) -> None:
    run_batch_head(
        datasource_test_data=datasource_test_data,
        fetch_all=fetch_all,
        n_rows=n_rows,
        success=success,
    )


def test_sql_query_data_asset(empty_data_context):
    context = empty_data_context
    datasource = sqlite_datasource(context, "yellow_tripdata.db")
    passenger_count_value = 5
    asset = (
        datasource.add_query_asset(
            name="query_asset",
            query=f"   SELECT * from yellow_tripdata_sample_2019_02 WHERE passenger_count = {passenger_count_value}",
        )
        .add_splitter_year_and_month(column_name="pickup_datetime")
        .add_sorters(["year"])
    )
    validator = context.get_validator(
        batch_request=asset.build_batch_request({"year": 2019})
    )
    result = validator.expect_column_distinct_values_to_equal_set(
        column="passenger_count",
        value_set=[passenger_count_value],
        result_format={"result_format": "BOOLEAN_ONLY"},
    )
    assert result.success
