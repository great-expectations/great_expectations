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
    # context: DataContext = data_context_with_datasource_pandas_engine

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

    validator = get_test_validator_with_data(data=df, execution_engine='pandas')

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="my_data_asset",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "my_identifier"},
    )
    # validator = context.get_validator(
    #     batch_request=batch_request,
    #     create_expectation_suite_with_name="test",
    # )

    result = validator.expect_column_values_to_be_string_integers_monotonically_increasing(column='a')

    assert result == ExpectationValidationResult(
        success=True,
        expectation_config={
            "expectation_type": "expect_column_values_to_be_in_type_list",
            "kwargs": {
                "column": "col",
                "type_list": ["STRINGTYPE", "BOOLEAN"],
            },
            "meta": {},
        },
        result={
            "element_count": 2,
            "unexpected_count": 0,
            "unexpected_percent": 0.0,
            "partial_unexpected_list": [],
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_percent_total": 0.0,
            "unexpected_percent_nonmissing": 0.0,
        },
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        meta={},
    )
