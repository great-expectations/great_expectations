import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.id_dict import BatchSpec
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.expectations.core.expect_column_values_to_be_increasing import (
    ExpectColumnValuesToBeIncreasing,
)


def test_expect_column_values_to_be_increasing_int_impl():
    df = pd.DataFrame({"a": [1, 2, 3, 3, 4, 5]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": False, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)

    # check for "strictly" kwarg
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": True, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)


def test_expect_column_values_to_be_increasing_str_impl():
    df = pd.DataFrame({"a": ["a", "b", "c", "c", "d", "e"]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": False, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=True,)

    # check for "strictly" kwarg
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": True, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=PandasExecutionEngine()
    )
    assert result == ExpectationValidationResult(success=False,)


def test_spark_expect_column_values_to_be_increasing_impl():
    from pyspark.sql import SparkSession

    df = pd.DataFrame({"a": [1, 2, 3, 3, 4, 5]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": False, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    myengine = SparkDFExecutionEngine()
    batch = myengine.load_batch(
        batch_definition={
            "data_asset_name": "foo",
            "partition_name": "bar"
        },
        batch_spec=BatchSpec({"blarg": "bah"}),
        in_memory_dataset=df
    )
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=True,)

    # check for "strictly" kwarg
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_increasing",
        kwargs={"column": "a", "strictly": True, "mostly": 1},
    )
    expectation = ExpectColumnValuesToBeIncreasing(expectationConfiguration)
    batch = Batch(data=df)
    result = expectation.validate(
        batches={"batch_id": batch}, execution_engine=myengine
    )
    assert result == ExpectationValidationResult(success=False,)
