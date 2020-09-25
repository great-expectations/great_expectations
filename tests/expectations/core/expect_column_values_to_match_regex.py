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
from great_expectations.expectations.core.expect_column_values_to_match_regex import (
    ExpectColumnValuesToMatchRegex,
)


def test_expect_column_values_to_match_regex_impl():
    df = pd.DataFrame({"a": [None, None, None]})
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


def test_sa_expect_column_values_to_be_match_regex_impl():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": ["bat", "rat", "cat"]})
    df.to_sql("test", eng)
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_match_regex",
        kwargs={"column": "a", "regex": ".at", "mostly": 1},
    )
    expectation = ExpectColumnValuesToMatchRegex(expectationConfiguration)
    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    myengine = SqlAlchemyExecutionEngine(engine=eng)
    batch = myengine.load_batch(batch_spec=batch_spec)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)
