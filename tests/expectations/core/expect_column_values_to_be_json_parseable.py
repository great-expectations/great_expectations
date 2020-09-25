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
from great_expectations.expectations.core.expect_column_values_to_be_json_parseable import (
    ExpectColumnValuesToBeJsonParseable,
)


def test_expect_column_values_to_be_json_parseable_impl():
    df = pd.DataFrame(
        {
            "a": [
                '{"a": 1, "b": 2, "c": 3}',
                '{"cat": 2,"bird": 21,"dog": 100}',
                '{"dfsfds": 2, "wekl": 21, "oqw": 100}',
            ]
        }
    )
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_json_parseable",
        kwargs={"column": "a", "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeJsonParseable(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
