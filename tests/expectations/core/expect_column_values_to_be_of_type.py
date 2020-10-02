import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_environment.types import (
    SqlAlchemyDatasourceTableBatchSpec,
)
from great_expectations.expectations.core.expect_column_values_to_be_of_type import (
    ExpectColumnValuesToBeOfType,
)


def test_expect_column_values_to_be_of_type_impl():
    # expect success
    df = pd.DataFrame({"a": ["1", "22", "333", "4444", "55555"]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "a", "min_value": 1, "max_value": 5, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)

    # expect failure
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "a", "min_value": 1, "max_value": 3, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={
            "column": "a",
            "min_value": 1,
            "max_value": 5,
            "mostly": 1,
            "strict_min": True,
            "strict_max": True,
        },
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)

    # expect success due to lower 'mostly'
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={
            "column": "a",
            "min_value": 1,
            "max_value": 5,
            "mostly": 0.6,
            "strict_min": True,
            "strict_max": True,
        },
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)


def test_sa_expect_column_values_to_be_of_type_impl():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")

    # expect success
    df = pd.DataFrame({"a": ["1", "22", "333", "4444", "55555"]})
    df.to_sql("test", eng)
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "a", "min_value": 1, "max_value": 5, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)

    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    myengine = SqlAlchemyExecutionEngine(engine=eng)
    batch = myengine.load_batch(batch_spec=batch_spec)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)

    # expect failure
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={"column": "a", "min_value": 1, "max_value": 3, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=False,)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={
            "column": "a",
            "min_value": 1,
            "max_value": 5,
            "mostly": 1,
            "strict_min": True,
            "strict_max": True,
        },
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=False,)

    # expect success due to lower 'mostly'
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_of_type",
        kwargs={
            "column": "a",
            "min_value": 1,
            "max_value": 5,
            "mostly": 0.6,
            "strict_min": True,
            "strict_max": True,
        },
    )
    expectation = ExpectColumnValuesToBeOfType(expectationConfiguration)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)
