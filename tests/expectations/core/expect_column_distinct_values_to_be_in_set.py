import pandas as pd

from great_expectations import ExpectColumnDistinctValuesToEqualSet, ExpectColumnDistinctValuesToBeInSet
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine


def test_expect_column_distinct_values_to_be_in_set_impl():
    df = pd.DataFrame({"a": [1, 2, 2, 3, 4, 5]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_distinct_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1,2,3,4]},
    )
    expectation = ExpectColumnDistinctValuesToBeInSet(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)
