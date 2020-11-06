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
from great_expectations.expectations.core.expect_column_values_to_match_json_schema import (
    ExpectColumnValuesToMatchJsonSchema,
)


def test_expect_column_values_to_match_json_schema_impl():
    df = pd.DataFrame(
        {
            "a": [
                '{"a": 1, "b": 2, "c": 3}',
                '{"a": 2,"b": 4,"c": 10}',
                '{"a": 2, "b": 21, "c": 10}',
            ]
        }
    )
    schema = {
        "type": "object",
        "properties": {
            "a": {"type": "number", "maximum": 5},
            "b": {"type": "number", "maximum": 21},
            "c": {"type": "number", "maximum": 10},
        },
    }
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_json_schema",
        kwargs={"column": "a", "json_schema": schema, "mostly": 1},
    )
    expectation = ExpectColumnValuesToMatchJsonSchema(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
