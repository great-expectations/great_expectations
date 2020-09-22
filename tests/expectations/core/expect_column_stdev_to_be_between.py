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
from great_expectations.expectations.core.expect_column_stdev_to_be_between import ExpectColumnStdevToBeBetween


def test_expect_column_stdev_to_be_between_impl():
    df = pd.DataFrame({"a": [1, 2, 3, 4, 5]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_stdev_to_be_between",
        kwargs={"column": "a", "min_value": 1, "max_value": 2},
    )
    expectation = ExpectColumnStdevToBeBetween(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
