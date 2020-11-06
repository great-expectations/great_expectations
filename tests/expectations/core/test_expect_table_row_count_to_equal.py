import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_table_row_count_to_equal import (
    ExpectTableRowCountToEqual,
)


def test_expect_table_row_count_to_equal_impl():
    df = pd.DataFrame({"a": [2, 3, 4, 5], "b": [1, 2, 3, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal", kwargs={"value": 3},
    )
    expectation = ExpectTableRowCountToEqual(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)
