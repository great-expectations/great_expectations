import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_environment.types import (
    BatchSpec,
    SqlAlchemyDatasourceTableBatchSpec,
)
from great_expectations.expectations.core.expect_column_value_lengths_to_equal import ExpectColumnValueLengthsToEqual



def test_expect_column_value_lengths_to_equal_int_impl():
    df = pd.DataFrame({"a": ["hey", "myn", "jef", 1234]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_lengths_to_equal",
        kwargs={"column": "a", "value": 3, "mostly": .75},
    )
    expectation = ExpectColumnValueLengthsToEqual(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
