import numpy as np
import pandas as pd
import pytest

from great_expectations.core.batch import Batch
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics import (
    ColumnMean,
    ColumnStandardDeviation,
    ColumnValuesZScore,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.execution_engine.execution_engine import MetricDomainTypes


def test_reader_fn():
    engine = PandasExecutionEngine()

    # Testing that can recognize basic excel file
    fn = engine._get_reader_fn(path="myfile.xlsx")
    assert "<function read_excel" in str(fn)

    # Ensuring that other way around works as well - reader_method should always override path
    fn_new = engine._get_reader_fn(reader_method="read_csv")
    assert "<function" in str(fn_new)


def test_get_compute_domain_with_no_domain_kwargs():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={}, domain_type="identity")
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={}, domain_type= MetricDomainTypes.TABLE)
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_pair_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b":[2, 3, 4, 5], "c": [1, 2, 3, 4]})
    expected_identity = df.drop(columns = ['c'])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column_A": "a", "column_B" : "b"},
                                                                      domain_type="column_pair")
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column_A": "a", "column_B" : "b"}, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column_A": "a", "column_B" : "b"},
                                                                      domain_type= "identity")

    assert data.equals(expected_identity), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_multicolumn_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None], "c": [1, 2, 2, 3], "d":  [2, 7, 9, 2]})
    expected_identity = df.drop(columns = ['d'])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"columns": ['a', 'b', 'c']},
                                                                      domain_type="multicolumn")
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"columns": ['a', 'b', 'c']}, "Accessor kwargs have been modified"

    # Trying same test with enum form of table domain - should work the same way
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"columns": ['a', 'b', 'c']},
                                                                      domain_type= "identity")
    assert data.equals(expected_identity), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_column_domain():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_identity = df.drop(columns= ['b'])

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "a"}, domain_type =
                                                                      MetricDomainTypes.COLUMN)
    assert data.equals(df), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {"column": "a"}, "Accessor kwargs have been modified"

    # Doing this using identity domain should yield different results
    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"column": "a"}, domain_type=
    MetricDomainTypes.IDENTITY)

    assert data.equals(expected_identity), "Data does not match after getting compute domain"
    assert compute_kwargs is not None, "Compute domain kwargs should be existent"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


def test_get_compute_domain_with_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df["b"] > 2].reset_index()

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 2",
                                                                                     "condition_parser": "pandas"},
                                                                                        domain_type= "table")
    # Ensuring data has been properly queried
    assert data["b"].equals(
        expected_df["b"]
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# What happens when we filter such that no value meets the condition?
def test_get_compute_domain_with_unmeetable_row_condition():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, None]})
    expected_df = df[df["b"] > 24].reset_index()

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    data, compute_kwargs, accessor_kwargs = engine.get_compute_domain(domain_kwargs={"row_condition": "b > 24",
                                                                                     "condition_parser": "pandas",},
                                                                                        domain_type= "identity")
    # Ensuring data has been properly queried
    assert data["b"].equals(
        expected_df["b"]
    ), "Data does not match after getting compute domain"

    # Ensuring compute kwargs have not been modified
    assert (
        "row_condition" in compute_kwargs.keys()
    ), "Row condition should be located within compute kwargs"
    assert accessor_kwargs == {}, "Accessor kwargs have been modified"


# Just checking that the Pandas Execution Engine can perform these in sequence
def test_resolve_metric_bundle():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    batch = Batch(data=df)

    # Building engine and configurations in attempt to resolve metrics
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)
    metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)

    # Ensuring metrics have been properly resolved
    assert (
        metrics[("column.mean", "column=a", ())] == 2.0
    ), "mean metric not properly computed"
    assert metrics[("column.standard_deviation", "column=a", ())] == 1.0, (
        "standard deviation " "metric not properly computed"
    )


# Ensuring that we can properly inform user when metric doesn't exist - should get a metric provider error
def test_resolve_metric_bundle_with_nonexistent_metric():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    batch = Batch(data=df)

    # Building engine and configurations in attempt to resolve metrics
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    mean = MetricConfiguration(
        metric_name="column.i_don't_exist",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.nonexistent",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)

    with pytest.raises(MetricProviderError) as e:
        metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)


# Making sure dataframe property is functional
def test_dataframe_property_given_loaded_batch():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, 4]})

    # Loading batch data
    engine.load_batch_data(batch_data=df, batch_id="1234")

    # Ensuring Data not distorted
    assert engine.dataframe.equals(df)
