from typing import Optional

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource
from great_expectations.self_check.util import get_test_validator_with_data


@pytest.mark.spark
def test_expect_table_row_count_to_be_between_runtime_custom_query_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501 # FIXME CoP
    datasource = context.data_sources.all()["my_sqlite_db_datasource"]
    assert isinstance(datasource, SQLDatasource)

    batch = (
        datasource.add_query_asset("titanic", query="select * from titanic")
        .add_batch_definition("my_batch_definition")
        .get_batch()
    )

    expectation = gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=2000)
    results = batch.validate(expectation)

    assert results == ExpectationValidationResult(
        success=True,
        result={"observed_value": 1313},
        meta={},
        expectation_config={
            "kwargs": {
                "min_value": 100,
                "max_value": 2000,
                "batch_id": "a47a711a9984cb2a482157adf54c3cb6",
            },
            "id": None,
            "meta": {},
            "expectation_type": "expect_table_row_count_to_be_between",
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )


@pytest.mark.spark
def test_expect_table_row_count_to_be_between_runtime_custom_query_with_where_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501 # FIXME CoP
    datasource = context.data_sources.all()["my_sqlite_db_datasource"]
    assert isinstance(datasource, SQLDatasource)

    batch = (
        datasource.add_query_asset("titanic", query="select * from titanic where sexcode = 1")
        .add_batch_definition("my_batch_definition")
        .get_batch()
    )

    expectation = gxe.ExpectTableRowCountToBeBetween(min_value=100, max_value=2000)
    results = batch.validate(expectation)

    assert results == ExpectationValidationResult(
        success=True,
        result={"observed_value": 462},
        meta={},
        expectation_config={
            "kwargs": {
                "min_value": 100,
                "max_value": 2000,
                "batch_id": "a47a711a9984cb2a482157adf54c3cb6",
            },
            "id": None,
            "meta": {},
            "expectation_type": "expect_table_row_count_to_be_between",
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
    )


@pytest.mark.unit
@pytest.mark.parametrize(
    "min_value,max_value,row_condition,success,expected_count",
    [
        # Without row_condition - should count all 10 rows
        (5, 15, None, True, 10),
        (1, 5, None, False, 10),
        # With row_condition filtering age >= 18 - should count 6 rows
        (5, 10, "age >= 18", True, 6),
        (1, 5, "age >= 18", False, 6),
        # With row_condition filtering age < 18 - should count 4 rows
        (3, 5, "age < 18", True, 4),
        (5, 10, "age < 18", False, 4),
        # With row_condition filtering name == "José" - should count 1 row
        (1, 1, 'name == "José"', True, 1),
        (2, 5, 'name == "José"', False, 1),
    ],
)
def test_expect_table_row_count_to_be_between_with_row_condition(
    min_value: int,
    max_value: int,
    row_condition: Optional[str],
    success: bool,
    expected_count: int,
):
    """Test that row_condition properly filters rows before counting."""
    # Create test dataframe with known counts
    df = pd.DataFrame(
        {
            "name": [
                "José",
                "Bob",
                "Charlie",
                "David",
                "Eve",
                "Frank",
                "Grace",
                "Hannah",
                "Ian",
                "Jane",
            ],
            "age": [25, 30, 15, 35, 22, 16, 28, 17, 40, 12],
        }
    )

    context = gx.get_context(mode="ephemeral")
    validator = get_test_validator_with_data(
        execution_engine="pandas",
        data=df,  # type: ignore[arg-type] # pandas DataFrame is valid data type
        context=context,
    )

    result = validator.expect_table_row_count_to_be_between(
        min_value=min_value,
        max_value=max_value,
        row_condition=row_condition,
        condition_parser="pandas",
    )
    assert result.success is success
    assert result.result["observed_value"] == expected_count
