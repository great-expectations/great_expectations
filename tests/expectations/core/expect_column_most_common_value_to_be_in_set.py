from great_expectations import ExpectColumnMostCommonValueToBeInSet
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
import pandas as pd
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
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)


def test_expect_column_most_common_value_to_be_in_set_int_impl():
    df = pd.DataFrame({"a": [1, 2, 2, 3]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_most_common_value_to_be_in_set",
        kwargs={"column": "a", "value_set": [3]},
    )
    expectation = ExpectColumnMostCommonValueToBeInSet(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)

