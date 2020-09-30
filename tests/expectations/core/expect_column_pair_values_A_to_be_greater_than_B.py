import numpy as np
import pandas as pd

from great_expectations import ExpectColumnPairValuesAToBeGreaterThanB
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine


def test_expect_column_pair_values_a_to_be_greater_than_b_impl():
    df = pd.DataFrame({"b": [2, 3, 4], "c": [1, 2, 3]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_pair_values_a_to_be_greater_than_b", kwargs={"column_A": "b", "column_B": "c"},
    )
    expectation = ExpectColumnPairValuesAToBeGreaterThanB(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
