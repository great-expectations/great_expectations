import pandas as pd

from great_expectations import ExpectColumnQuantileValuesToBeBetween
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine


def test_expect_column_quantile_values_to_be_between_impl():
    df = pd.DataFrame({"a": [1,2,2,3,3,3,4]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_quantile_values_to_be_between",
        kwargs={"column": "a", "quantile_ranges": {
                    "quantiles": [0., 0.333, 0.6667, 1.],
                    "value_ranges": [[0,1], [2,3], [4,5], [6,7]]
                }},
    )
    expectation = ExpectColumnQuantileValuesToBeBetween(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)