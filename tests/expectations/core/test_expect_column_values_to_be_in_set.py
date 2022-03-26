from datetime import datetime

import pandas as pd
import pytest

import great_expectations.exceptions.exceptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)


# <snippet>
class ExpectColumnValuesToBeTwoLetterCountryCode(ExpectColumnValuesToBeInSet):
    default_kwarg_values = {
        "value_set": ["FR", "DE", "CH", "ES", "IT", "BE", "NL", "PL"],
    }


# </snippet>
def test_expect_column_values_to_be_in_set_fail(
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

    result = validator.expect_column_values_to_be_in_set(
        column="a", value_set=["2021-06-18"]
    )
    assert result.success is False


def test_expect_column_values_in_set_pass(
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

    result = validator.expect_column_values_to_be_in_set(
        column="a",
        value_set=[
            "2021-01-01",
            "2021-01-31",
            "2021-02-28",
            "2021-03-20",
            "2021-02-21",
            "2021-05-01",
            "2021-06-18",
        ],
    )
    assert result.success is True


def test_expect_column_values_country_fail(
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

    result = validator.expect_column_values_to_be_two_letter_country_code(column="a")

    assert result.success is False


def test_expect_column_values_country_pass(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["FR", "DE", "CH", "ES", "IT", "BE", "NL", "PL"]})

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

    result = validator.expect_column_values_to_be_two_letter_country_code(column="a")

    assert result.success is True


def test_expect_column_values_to_be_in_set_no_set(
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
    with pytest.raises(
        great_expectations.exceptions.exceptions.InvalidExpectationConfigurationError
    ):
        result = validator.expect_column_values_to_be_in_set(column="a")
