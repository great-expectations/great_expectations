import pandas as pd
import pytest

from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.self_check.util import get_test_validator_with_data
from great_expectations.expectations.core.expect_column_values_to_be_string_integers_monotonically_increasing import (
    ExpectColumnValuesToBeStringIntegersMonotonicallyIncreasing
)

def test_expect_column_values_to_be_string_integers_monotonically_increasing(
    data_context_with_datasource_pandas_engine,
):
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

    result = validator.expect_column_values_to_be_string_integers_monotonically_increasing(column='a',
                                                                                           type=int)

    assert result == ExpectationValidationResult(
        success=False,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "a",
            },
            "meta": {},
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )
