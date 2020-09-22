import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_values_to_be_decreasing import (
    ExpectColumnValuesToBeDecreasing,
)


def test_expect_column_values_to_be_decreasing_impl():
    df_dec = pd.DataFrame({"a": [5, 4, 3, 3, 2, 1]})
    batch = Batch(data=df_dec)
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_decreasing",
        kwargs={"column": "a", "strictly": False, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeDecreasing(expectationConfiguration)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)

    # check for "strictly" kwarg
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_decreasing",
        kwargs={"column": "a", "strictly": True, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeDecreasing(expectationConfiguration)
    batch = Batch(data=df_dec)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)

    df_inc = pd.DataFrame({"a": [1, 2, 3, 3, 4, 5]})
    batch = Batch(data=df_inc)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)
