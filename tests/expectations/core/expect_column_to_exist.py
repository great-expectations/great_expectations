import pandas as pd
import numpy as np

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_to_exist import ExpectColumnToExist


def test_expect_column_to_exist():
    df = pd.DataFrame({"a": [2, 3, 4], "b":[1,2,3], "c": [1,2,3]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column":"c"},
    )
    expectation = ExpectColumnToExist(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)