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
from great_expectations.expectations.core.expect_column_values_to_match_strftime_format import (
    ExpectColumnValuesToMatchStrftimeFormat,
)


def test_expect_column_values_to_match_strftime_format_impl():
    df = pd.DataFrame({"a": ["10/10/20", "11/11/10", "05/03/23"]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_strftime_format",
        kwargs={"column": "a", "strftime_format": "%m/%d/%y", "mostly": 1},
    )
    expectation = ExpectColumnValuesToMatchStrftimeFormat(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)
