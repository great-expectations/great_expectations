import pandas as pd
import pytest

import great_expectations.exceptions.exceptions
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import DataContext
from great_expectations.expectations.core.expect_column_mean_to_be_between import (
    ExpectColumnMeanToBeBetween,
)


# <snippet>
class ExpectColumnMeanToBePositive(ExpectColumnMeanToBeBetween):
    """Expects the mean of values in this column to be positive"""

    default_kwarg_values = {
        "min_value": 0,
        "strict_min": True,
    }

    def validate_configuration(self, configuration):
        super().validate_configuration(configuration)
        assert "min_value" not in configuration.kwargs, "min_value cannot be altered"
        assert "max_value" not in configuration.kwargs, "max_value cannot be altered"
        assert "strict_min" not in configuration.kwargs, "strict_min cannot be altered"
        assert "strict_max" not in configuration.kwargs, "strict_max cannot be altered"

    library_metadata = {"tags": ["basic stats"], "contributors": ["@joegargery"]}


# </snippet>
def test_expect_column_mean_to_be_positive(data_context_with_datasource_pandas_engine):
    context: DataContext = data_context_with_datasource_pandas_engine

    df = pd.DataFrame({"a": [0, 1, 3, 4, 5]})

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

    result = validator.expect_column_mean_to_be_positive(column="a")

    assert result.success is True
