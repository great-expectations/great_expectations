import pytest

import great_expectations.expectations as gxe
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource


@pytest.mark.spark
def test_expect_table_row_count_to_be_between_runtime_custom_query_no_temp_table_sa(
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
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
    context = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
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
