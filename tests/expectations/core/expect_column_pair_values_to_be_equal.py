import numpy as np
import pandas as pd

from great_expectations import ExpectColumnPairValuesToBeEqual
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine


def test_expect_column_pair_values_to_be_equal_impl():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_equal", kwargs={"column_A": "a", "column_B": "b"},
    )
    expectation = ExpectColumnPairValuesToBeEqual(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
