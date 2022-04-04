from typing import Optional

import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.expectations.core import ExpectColumnValuesToMatchRegex


class ExpectColumnValuesAsStringToBePositiveInteger(ExpectColumnValuesToMatchRegex):
    default_kwarg_values = {
        "regex": "^\\d+$",
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        assert "regex" not in configuration.kwargs, "regex cannot be altered"


def test_expect_column_values_as_string_to_be_positive_integers_pass(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["1", "2", "3", "4", "5"]})

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

    assert validator.expect_column_values_as_string_to_be_positive_integer(
        column="a"
    ).success


def test_expect_column_values_as_string_to_be_positive_integers_fail(
    data_context_with_datasource_pandas_engine,
):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": ["1", "2", "3", "4", "a"]})

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

    assert not validator.expect_column_values_as_string_to_be_positive_integer(
        column="a"
    ).success
