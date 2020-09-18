import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_value_lengths_to_be_between import (
    ExpectColumnValueLengthsToBeBetween,
)


def test_expect_column_value_lengths_to_be_between_impl():
    df = pd.DataFrame({"a": [
        "1",
        "22",
        "333",
        "4444",
        "55555"
    ]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_lengths_to_be_between",
        kwargs={
            "column": "a",
            "min_value": 1,
            "max_value": 5,
            "mostly": 1
        },
    )
    expectation = ExpectColumnValueLengthsToBeBetween(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True, )
