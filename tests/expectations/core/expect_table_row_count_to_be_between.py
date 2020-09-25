import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)


def test_expect_table_row_count_to_be_between_impl():
    df = pd.DataFrame({"a": [2, 3, 4, 5], "b": [1, 2, 3, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 4, "max_value": 5},
    )
    expectation = ExpectTableRowCountToBeBetween(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
