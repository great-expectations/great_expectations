import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.self_check.util import get_test_validator_with_data
from great_expectations.expectations.core.expect_column_values_to_be_string_integers_monotonically_increasing import (
    ExpectColumnValuesToBeStringIntegersMonotonicallyIncreasing
)
from great_expectations.core.expectation_validation_result import ExpectationValidationResult
from great_expectations.exceptions.exceptions import MetricResolutionError


def test_expect_column_values_to_be_string_integers_monotonically_increasing_success(
        data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame(
        {
            "a": [
                "0",
                "1",
                "2",
                "3",
                "3",
                "9",
                "11"
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

    result = validator.expect_column_values_to_be_string_integers_monotonically_increasing(column='a')

    assert result == ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config={
            "expectation_type": "expect_column_values_to_be_string_integers_monotonically_increasing",
            "kwargs": {
                "column": "a",
                "batch_id": "57175eeb4a8baa7ae63f44c6540eb559",
            },
            "meta": {},
            "ge_cloud_id": None,
        },
        meta={},
        result={
            "observed_value": [
                {
                    "index": True,
                    "a": 1.0
                }
            ]
        },
        success=True
    )

def test_expect_column_values_to_be_string_integers_monotonically_increasing_failure(
        data_context_with_datasource_pandas_engine,
):
    with pytest.raises(MetricResolutionError):
        context: DataContext = data_context_with_datasource_pandas_engine

        df = pd.DataFrame(
            {
                "a": [
                    "1",
                    "2",
                    "3",
                    "3",
                    "0",
                    "6",
                    "2021-05-01",
                    "test",
                    8,
                    9
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

        result = validator.expect_column_values_to_be_string_integers_monotonically_increasing(column='a')
