import numpy as np
import pandas as pd
import pytest
import logging

from great_expectations.core.batch import Batch
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import SqlAlchemyBatchData, \
    SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics import ColumnMean, ColumnStandardDeviation, ColumnValuesZScore
from great_expectations.validator.validation_graph import MetricConfiguration
import pyspark.sql.functions as F


# Function to test for spark dataframe equality
from tests.test_utils import _build_sa_engine


def dataframes_equal(first_table, second_table):
    if first_table.schema != second_table.schema:
        return False
    if first_table.collect() != second_table.collect():
        return False
    return True


# Testing batching of aggregate metrics
def test_sa_batch_aggregate_metrics(caplog, sa):
    import datetime

    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}))

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
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    start = datetime.datetime.now()
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
    print("t1")
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
            == "SqlAlchemyExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


# Ensuring functionality of compute_domain when no domain kwargs are given
def test_get_compute_domain_with_no_domain_kwargs():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b":[2,3,4,None]}))
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={}, domain_type="table")

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(data, df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_column_pair(sa):
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))

    # Fetching data, compute_domain_kwargs, accessor_kwargs
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column_A": "a", "column_B" : "b"},
                                                                      domain_type="column_pair")

    raw_data = engine.engine.execute(sa.select(["*"]).select_from(engine.active_batch_data.selectable)).fetchall()
    domain_data = engine.engine.execute(sa.select(["*"]).select_from(data)).fetchall()

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert "column_A" not in compute_kwargs.keys() and "column_B" not in compute_kwargs.keys(),"domain kwargs should be existent"
    assert accessor_kwargs == {"column_A": "a", "column_B" : "b"}, "Accessor kwargs have been modified"

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column_A": "a", "column_B": "b"},
                                                                      domain_type="identity")

    # Ensuring that with no domain nothing happens to the data itself
    assert raw_data == domain_data, "Data does not match after getting compute domain"
    assert compute_kwargs == {"column_A": "a", "column_B" : "b"}, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing for only untested use case - multicolumn
def test_get_compute_domain_with_multicolumn():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1,2,3, None]}))
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"columns": ["a", "b", "c"]},
                                                                      domain_type="multicolumn")

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(data, df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"columns" : ["a", "b", "c"]}, "Accessor kwargs have been modified"

    # Checking for identity
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"columns": ["a", "b", "c"]},
                                                                      domain_type="identity")

    # Ensuring that with no domain nothing happens to the data itself
    assert dataframes_equal(data, df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Testing whether compute domain is properly calculated, but this time obtaining a column
def test_get_compute_domain_with_column_domain():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "a"})

    # Ensuring that column domain is now an accessor kwarg, and data remains unmodified
    assert dataframes_equal(data, df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"


# Using an unmeetable row condition to see if empty dataset will result in errors
def test_get_compute_domain_with_row_condition():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    df = engine.dataframe
    expected_df = df.where('b > 2')

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 2",
                                                                              "condition_parser": "spark"},
                                                                                domain_type="identity")

    # Ensuring data has been properly queried
    assert dataframes_equal(data, expected_df), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys(), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    df = engine.dataframe
    expected_df = df.where('b > 24')

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 24",
                                                                                  "condition_parser": "spark", },
                                                                                    domain_type= "identity")
    # Ensuring data has been properly queried
    assert dataframes_equal(data, expected_df), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys(), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"

    # Ensuring errors for column and column_ pair domains are caught
    with pytest.raises(GreatExpectationsError) as e:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 24",
                                                                                         "condition_parser": "spark", },
                                                                                            domain_type="column")
    with pytest.raises(GreatExpectationsError) as g:
        data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 24",
                                                                                         "condition_parser": "spark", },
                                                                                            domain_type="column_pair")


# Testing to ensure that great expectation experimental parser also works in terms of defining a compute domain
def test_get_compute_domain_with_ge_experimental_condition_parser():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    df = engine.dataframe

    # Filtering expected data based on row condition
    expected_df = df.where('b == 2')

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Obtaining data from computation
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "b",
                                                                                  "row_condition": 'col("b") == 2',
                                                                                  "condition_parser": "great_expectations__experimental__"},
                                                                                   domain_type = "column")
    # Ensuring data has been properly queried
    assert dataframes_equal(data, expected_df), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys(), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {"column": "b"}, "Accessor kwargs have been modified"

    # Should react differently for domain type identity
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "b",
                                                                                     "row_condition": 'col("b") == 2',
                                                                                     "condition_parser": "great_expectations__experimental__"},
                                                                                        domain_type="identity")
    # Ensuring data has been properly queried
    assert dataframes_equal(data, expected_df.select('b')), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert "row_condition" in compute_kwargs.keys(), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_nonexistent_condition_parser():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]}))
    df = engine.dataframe

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Expect GreatExpectationsError because parser doesn't exist
    with pytest.raises(GreatExpectationsError) as e:
     data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 24",
                                                                                  "condition_parser": "nonexistent"})


# Testing that non-aggregate metrics aren't bundled
def test_resolve_metric_bundle_with_nonaggregate_metric(caplog):
    import datetime

    engine = _build_sa_engine(
     pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    # Non-aggregate metric configurations
    desired_metric_1 = MetricConfiguration(
         metric_name="column_values.unique",
         metric_domain_kwargs={"column": "a"},
         metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
     metric_name="column_values.in_set",
     metric_domain_kwargs={"column": "a"},
     metric_value_kwargs={"value_set": [1,2,3,4,5]},
    )

    # Aggregate metric configurations
    desired_metric_3 = MetricConfiguration(
     metric_name="column.aggregate.max",
     metric_domain_kwargs={"column": "b"},
     metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
     metric_name="column.aggregate.min",
     metric_domain_kwargs={"column": "b"},
     metric_value_kwargs=dict(),
    )
    res = engine.resolve_metrics(
     metrics_to_resolve=(
         desired_metric_1,
         desired_metric_2,
         desired_metric_3,
         desired_metric_4,
     )
    )
    # Ensuring that metric ideas of nonaggregates actually represent computation
    assert res[desired_metric_1.id] != 3
    assert res[desired_metric_2.id] != 1
    assert res[desired_metric_3.id] == 4
    assert res[desired_metric_4.id] == 4

    # Check that all only aggregate metrics are computed over a single domain
    found_message = False
    for record in caplog.records:
     if (
             record.message
             == "SparkDFExecutionEngine computed 2 metrics on domain_id ()"
     ):
         found_message = True
    assert found_message

# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric():
    engine = _build_sa_engine(
     pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
     metric_name="column_values.unique",
     metric_domain_kwargs={"column": "a"},
     metric_value_kwargs=dict(),
    )
    desired_metric_2 = MetricConfiguration(
     metric_name="column.aggregate.min",
     metric_domain_kwargs={"column": "a"},
     metric_value_kwargs=dict(),
    )
    desired_metric_3 = MetricConfiguration(
     metric_name="column.aggregate.max",
     metric_domain_kwargs={"column": "b"},
     metric_value_kwargs=dict(),
    )
    desired_metric_4 = MetricConfiguration(
     metric_name="column.aggregate.does_not_exist",
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

