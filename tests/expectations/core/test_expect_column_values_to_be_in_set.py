from __future__ import annotations

from typing import List

import pandas as pd
import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import AbstractDataContext
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)


# <snippet name="tests/expectations/core/test_expect_column_values_to_be_in_set.py ExpectColumnValuesToBeTwoLetterCountryCode_class_def">
class ExpectColumnValuesToBeTwoLetterCountryCode(ExpectColumnValuesToBeInSet):
    value_set: List[str] = ["FR", "DE", "CH", "ES", "IT", "BE", "NL", "PL"]


# </snippet>
@pytest.mark.big
def test_expect_column_values_to_be_in_set_fail(
    data_context_with_datasource_pandas_engine,
):
    context: AbstractDataContext = data_context_with_datasource_pandas_engine

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


@pytest.mark.filesystem
def test_expect_column_values_in_set_pass(
    data_context_with_datasource_pandas_engine,
):
    context: AbstractDataContext = data_context_with_datasource_pandas_engine

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


@pytest.mark.big
def test_expect_column_values_country_fail(
    data_context_with_datasource_pandas_engine,
):
    context: AbstractDataContext = data_context_with_datasource_pandas_engine

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


@pytest.mark.big
def test_expect_column_values_country_pass(
    data_context_with_datasource_pandas_engine,
):
    context: AbstractDataContext = data_context_with_datasource_pandas_engine

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


@pytest.mark.big
def test_expect_column_values_to_be_in_set_invalid_set(
    data_context_with_datasource_pandas_engine,
):
    context: AbstractDataContext = data_context_with_datasource_pandas_engine

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
    with pytest.raises(pydantic.ValidationError):
        _ = validator.expect_column_values_to_be_in_set(column="a", value_set="foo")
