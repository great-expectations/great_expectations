import logging

import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pytest

from great_expectations.core.batch import Batch
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics import (
    ColumnMean,
    ColumnStandardDeviation,
    ColumnValuesInSet,
    ColumnValuesUnique,
    ColumnValuesZScore,
)
from great_expectations.validator.validation_graph import MetricConfiguration


def test_add_column_row_condition(spark_session):
    from pyspark.sql import functions as F

    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})
    domain_kwargs = {"column": "foo"}

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo) AND foo IS NOT NULL"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (None,), (2,), (3,), (4,), (5,), (6,)]

    # This time, our skip value *will* be nan
    df = pd.DataFrame({"foo": [1, 2, 3, 3, None, 2, 3, 4, 5, 6]})
    df = spark_session.createDataFrame(df)
    engine = SparkDFExecutionEngine(batch_data_dict={tuple(): df})

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=False, filter_nan=True
    )
    assert new_domain_kwargs["row_condition"] == "NOT isnan(foo)"
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    assert res == [(1,), (2,), (3,), (3,), (2,), (3,), (4,), (5,), (6,)]

    new_domain_kwargs = engine.add_column_row_condition(
        domain_kwargs, filter_null=True, filter_nan=False
    )
    assert new_domain_kwargs["row_condition"] == 'col("foo").notnull()'
    df, cd, ad = engine.get_compute_domain(new_domain_kwargs, domain_type="table")
    res = df.collect()
    expected = [(1,), (2,), (3,), (3,), (np.nan,), (2,), (3,), (4,), (5,), (6,)]
    # since nan != nan by default
    assert np.allclose(res, expected, rtol=0, atol=0, equal_nan=True)


# Function to test for spark dataframe equality
def dataframes_equal(first_table, second_table):
    if first_table.schema != second_table.schema:
        return False
    if first_table.collect() != second_table.collect():
        return False
    return True


# Builds a Spark Execution Engine
def _build_spark_engine(df):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            tuple(
                None if isinstance(x, (float, int)) and np.isnan(x) else x
                for x in record.tolist()
            )
            for record in df.to_records(index=False)
        ],
        df.columns.tolist(),
    )
    batch = Batch(data=df)
    engine = SparkDFExecutionEngine(batch_data_dict={batch.id: batch.data})
    return engine


# Ensuring that, given aggregate metrics, they can be properly bundled together
def test_sparkdf_batch_aggregate_metrics(caplog, spark_session):
    import datetime

    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        )
    )
    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_1},
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_2},
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_3},
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric_4},
    )
    start = datetime.datetime.now()
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    res = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    end = datetime.datetime.now()
    print(end - start)
    assert res[desired_metric_1.id] == 3
    assert res[desired_metric_2.id] == 1
    assert res[desired_metric_3.id] == 4
    assert res[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SparkDFExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


# Ensuring functionality of compute_domain when no domain kwargs are given
def test_get_compute_domain_with_no_domain_kwargs():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={}, domain_type="table"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_column_pair():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="column_pair"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Accessor kwargs have been modified"

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column_A": "a", "column_B": "b"}, domain_type="identity"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "column_A": "a",
        "column_B": "b",
    }, "Compute domain kwargs should not be modified"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 3, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="multicolumn"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be empty"
    assert accessor_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Accessor kwargs have been modified"

    # Checking for identity
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"columns": ["a", "b", "c"]}, domain_type="identity"
    )

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {
        "columns": ["a", "b", "c"]
    }, "Compute domain kwargs should not change for identity domain"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"column": "a"}, domain_type="column"
    )

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert dataframes_equal(
        data, df
    ), "Data does not match after getting compute domain"
    assert compute_kwargs == {}, "Compute domain kwargs should be empty"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# Using an unmeetable row condition to see if empty dataset will result in errors
def test_get_compute_domain_with_row_condition():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe
    expected_df = df.where("b > 2")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 2", "condition_parser": "spark"},
        domain_type="identity",
    )

    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe
    expected_df = df.where("b > 24")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={"row_condition": "b > 24", "condition_parser": "spark",},
        domain_type="identity",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"

    # Ensuring errors for column and column_ pair domains are caught
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={"row_condition": "b > 24", "condition_parser": "spark",},
            domain_type="column",
        )
    with pytest.raises(GreatExpectationsError) as g:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={"row_condition": "b > 24", "condition_parser": "spark",},
            domain_type="column_pair",
        )


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Filtering expected data based on row condition
    expected_df = df.where("b == 2")

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Obtaining data from computation
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="column",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "b"}, "Accessor kwargs have been modified"

    # Should react differently for domain type identity
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
        domain_kwargs={
            "column": "b",
            "row_condition": 'col("b") == 2',
            "condition_parser": "great_expectations__experimental__",
        },
        domain_type="identity",
    )
    # Ensuring data has been properly queried
    assert dataframes_equal(
        data, expected_df.select("b")
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_nonexistent_condition_parser():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    )
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(
            domain_kwargs={
                "row_condition": "b > 24",
                "condition_parser": "nonexistent",
            },
            domain_type=MetricDomainTypes.IDENTITY,
        )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric():
    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.does_not_exist",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )

    # Ensuring a metric provider error is raised if metric does not exist
    with pytest.raises(MetricProviderError) as e:
        res = engine.resolve_metrics(
            metrics_to_resolve=(
                desired_metric_1,
                desired_metric_2,
                desired_metric_3,
                desired_metric_4,
            )
        )
        print(e)


# Making sure dataframe property is functional
def test_dataframe_property_given_loaded_batch():
    from pyspark.sql import SparkSession

    engine = SparkDFExecutionEngine()

    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Ensuring Data not distorted
    assert engine.dataframe == df
