import pandas as pd
import pytest

import great_expectations.exceptions.exceptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.expectations.core.expect_column_distinct_values_to_be_continuous import (
    ExpectColumnDistinctValuesToBeContinuous,
)


def test_expect_column_values_to_be_continuous_fail_for_missing_date(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame(
        {
            "a": [
                "2021-01-01",
                "2021-01-31",
                "2021-02-28",
                "2021-03-20",
                "2021-02-21",
                "2021-05-01",
                "2021-06-18",
            ]
        }
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_distinct_values_to_be_continuous(
        column="a", datetime_format="%Y-%m-%d"
    )
    assert result.success is False


def test_expect_column_values_in_set_pass_for_continuous_date(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame(
        {
            "a": [
                "2021-01-01 10:56:30",
                "2021-01-03 10:56:30",
                "2021-01-02 10:56:30",  # out of order row to make sure we're ignoring order
                "2021-01-04 10:56:30",
                "2021-01-05 10:56:30",
                "2021-01-06 10:56:30",
                "2021-01-07 10:56:30",
            ]
        }
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_distinct_values_to_be_continuous(
        column="a", datetime_format="%Y-%m-%d %H:%M:%S"
    )
    assert result.success is True


def test_expect_column_values_to_be_continuous_pass_for_list_of_integers(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": [2, 3, 4, 5, 6, 7, 8, 9, 1]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_distinct_values_to_be_continuous(column="a")
    assert result.success is True


def test_expect_column_values_to_be_continuous_fail_for_list_of_integers(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": [1, 2, 3, 4, 5, 8, 9]})

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    validator = context.get_validator(
        batch_request=batch_request,
        create_expectation_suite_with_name="test",
    )

    result = validator.expect_column_distinct_values_to_be_continuous(column="a")
    assert result.success is False
