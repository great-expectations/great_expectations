import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_values_to_be_unique import (
    ExpectColumnValuesToBeUnique,
)


def test_expect_column_values_to_be_unique_impl():
    df = pd.DataFrame({"a": [None, None, None, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "a", "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeUnique(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
