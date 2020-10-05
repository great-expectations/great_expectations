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
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)


def test_expect_column_values_to_be_in_set_int_impl():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2],
            "mostly": 1,
            "catch_exceptions": True,
        },
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)


def test_expect_column_values_to_be_in_set_str_impl():
    df = pd.DataFrame({"a": ["dog", "cat", "mouse"]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": ["dog", "cat"], "mostly": 0.5},
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)


def test_sa_expect_column_values_to_be_in_set_impl():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 1]})
    df.to_sql("test", eng)
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2], "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    myengine = SqlAlchemyExecutionEngine(engine=eng)
    batch = myengine.load_batch(batch_spec=batch_spec)
    result = expectation.validate(
        batches={batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)


def test_spark_expect_column_values_to_be_in_set_impl():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 1]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2], "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    myengine = SparkDFExecutionEngine()
    batch = myengine.load_batch(
        batch_definition={"data_asset_name": "foo", "partition_name": "bar"},
        batch_spec=BatchSpec({"blarg": "bah"}),
        in_memory_dataset=df,
    )

    result = expectation.validate(
        batches={batch.batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)

    df = pd.DataFrame({"a": [1, 2, 3]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2], "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeInSet(expectationConfiguration)
    myengine = SparkDFExecutionEngine()
    batch = myengine.load_batch(
        batch_definition={"data_asset_name": "foo", "partition_name": "bar"},
        batch_spec=BatchSpec({"blarg": "bah"}),
        in_memory_dataset=df,
    )

    result = expectation.validate(
        batches={batch.batch_spec.to_id(): batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=False,)
