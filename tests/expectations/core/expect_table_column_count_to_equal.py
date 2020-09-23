import pandas as pd
import numpy as np

from great_expectations import ExpectTableColumnCountToEqual
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine



def test_expect_table_column_count_to_equal_impl():
    df = pd.DataFrame({"a": [2, 3, 4], "b":[1,2,3]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value":3},
    )
    expectation = ExpectTableColumnCountToEqual(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)