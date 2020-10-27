import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_value_z_scores_to_be_less_than import (
    ExpectColumnValueZScoresToBeLessThan,
)


def test_expect_column_value_z_scores_to_be_less_than_impl():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches=[batch], execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
