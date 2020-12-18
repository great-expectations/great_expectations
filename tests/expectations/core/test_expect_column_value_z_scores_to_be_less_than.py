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
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.core.expect_column_value_z_scores_to_be_less_than import (
    ExpectColumnValueZScoresToBeLessThan,
)
from great_expectations.validator.validator import Validator


def test_expect_column_value_z_scores_to_be_less_than_impl():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    result = expectation.validate(Validator(execution_engine=engine))
    assert result == ExpectationValidationResult(
        success=True,
    )


def test_sa_expect_column_value_z_scores_to_be_less_than_impl(postgresql_engine):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    df.to_sql("z_score_test_data", postgresql_engine, if_exists="replace")
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch_data = SqlAlchemyBatchData(
        engine=postgresql_engine, table_name="z_score_test_data"
    )
    engine = SqlAlchemyExecutionEngine(
        engine=postgresql_engine, batch_data_dict={"my_id": batch_data}
    )
    result = expectation.validate(Validator(execution_engine=engine))
    assert result == ExpectationValidationResult(
        success=True,
    )


def test_spark_expect_column_value_z_scores_to_be_less_than_impl(spark_session):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    spark = spark_session.builder.getOrCreate()
    df = spark.createDataFrame(df)

    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    engine = SparkDFExecutionEngine(batch_data_dict={"my_id": df})
    result = expectation.validate(Validator(execution_engine=engine))
    assert result == ExpectationValidationResult(
        success=True,
    )
