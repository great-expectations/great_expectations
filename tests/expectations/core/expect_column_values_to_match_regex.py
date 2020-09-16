import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_values_to_match_regex import (
    ExpectColumnValuesToMatchRegex,
)


def test_expect_column_values_to_match_regex_impl():
    df = pd.DataFrame({"a": ["bat", "rat", "cat"]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={"column": "a", "regex": ".at", "mostly": 1},
    )
    expectation = ExpectColumnValuesToMatchRegex(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
