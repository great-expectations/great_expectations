import logging

import numpy as np
import pandas as pd
import sqlalchemy as sa

from great_expectations.core.batch import Batch
from great_expectations.dataset import MetaSqlAlchemyDataset, SqlAlchemyDataset
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics import *
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.validation_graph import MetricConfiguration


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


def _build_sa_engine(df):
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://", echo=False)
    df.to_sql("test", eng, index=False)
    batch_data = SqlAlchemyBatchData(engine=eng, table_name="test")
    batch = Batch(data=batch_data)
    engine = SqlAlchemyExecutionEngine(
        engine=eng, batch_data_dict={batch.id: batch_data}
    )
    return engine


def _build_pandas_engine(df):
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    return engine


def test_metric_loads():
    assert (
        get_metric_provider("column.aggregate.max", PandasExecutionEngine()) is not None
    )


def test_basic_metric():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 3}


def test_mean_metric_pd():
    engine = _build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, None]}))
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_stdev_metric_pd():
    engine = _build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, None]}))
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 1}


def test_max_metric_sa():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 1]}))
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_max_metric_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 1]}))
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_map_value_set_sa():
    engine = _build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # Note: metric_dependencies is optional here in the config when called from a validator.
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": desired_metric},
    )
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 0}


def test_map_of_type_sa():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df.to_sql("test", eng, index=False)
    batch_data = SqlAlchemyBatchData(engine=eng, table_name="test")
    batch = Batch(data=batch_data)
    engine = SqlAlchemyExecutionEngine(
        engine=eng, batch_data_dict={batch.id: batch_data}
    )
    desired_metric = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={},
    )

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results[desired_metric.id][0]["name"] == "a"
    assert isinstance(results[desired_metric.id][0]["type"], sa.FLOAT)


def test_map_value_set_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))

    condition_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,))
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": condition_metric},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 0}

    # We run the same computation again, this time with None being replaced by nan instead of NULL
    # to demonstrate this behavior
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)
    batch = Batch(data=df)
    engine = SparkDFExecutionEngine(batch_data_dict={batch.id: batch.data})

    condition_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,))
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": condition_metric},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 1}


def test_map_column_value_lengths_between_pd():
    # NOTE: 20201106 - JPC - this test is unusual because it's checking against the map_fn value.
    # We plan to implement map_values in the future.
    engine = _build_pandas_engine(
        pd.DataFrame({"a": ["a", "aaa", "bcbc", "defgh", None]})
    )
    desired_metric = MetricConfiguration(
        metric_name="column_values.value_length.map_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1]},
    )
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # assert results == {desired_metric.id: 1}
    ser_expected_lengths = pd.Series([1, 3, 4, 5])
    assert ser_expected_lengths.equals(results[desired_metric.id])


def test_map_unique_pd():
    engine = _build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))
    desired_metric = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1]},
    )

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results[desired_metric.id]) == [False, False, True, True]


def test_map_unique_spark():
    engine = _build_spark_engine(
        pd.DataFrame(
            {
                "a": [1, 2, 3, 3, 4, None],
                "b": [None, "foo", "bar", "baz", "qux", "fish"],
            }
        )
    )

    condition_metric = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [(3, 2)]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_rows",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [(3, "bar"), (3, "baz")]


def test_map_unique_sa():
    engine = _build_sa_engine(
        pd.DataFrame(
            {"a": [1, 2, 3, 3, None], "b": ["foo", "bar", "baz", "qux", "fish"]}
        )
    )
    condition_metric = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(metrics_to_resolve=(condition_metric,))

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [(3, 2)]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_rows",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == [(3, "baz"), (3, "qux")]


def test_z_score_under_threshold_pd():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    mean = MetricConfiguration(
        metric_name="column.aggregate.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.aggregate.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)
    metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "column.aggregate.standard_deviation": stdev,
            "column.aggregate.mean": mean,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"column_values.z_score.map_fn": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert list(results[desired_metric.id]) == [False, False, False]
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"unexpected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_z_score_under_threshold_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))
    mean = MetricConfiguration(
        metric_name="column.aggregate.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    stdev = MetricConfiguration(
        metric_name="column.aggregate.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    desired_metrics = (mean, stdev)
    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metrics))

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "column.aggregate.standard_deviation": stdev,
            "column.aggregate.mean": mean,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"column_values.z_score.map_fn": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"unexpected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_table_metric_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None], "b": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 5}


def test_column_pairs_equal_metric_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3], "b": [1, 2, 3, 3]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.equal",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results.values())[0].equals(pd.Series([True, True, True, True]))


def test_column_pairs_greater_metric_pd():
    df = pd.DataFrame({"a": [2, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.a_greater_than_b",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs={
            "or_equal": True,
            "ignore_row_if": "either_value_is_missing",
        },
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        list(results.values())[0]
        .reset_index(drop=True)
        .equals(pd.Series([True, True, True, True]))
    )


def test_column_pairs_in_set_metric_pd():
    df = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.in_set",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "ignore_row_if": "either_value_is_missing",
        },
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        list(results.values())[0]
        .reset_index(drop=True)
        .equals(pd.Series([False, True, True, True]))
    )


def test_table_metric_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 1]}))

    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 3}


def test_median_metric_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 3]}))

    row_count = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=dict(),
    )

    desired_metrics = (row_count,)
    metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)

    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"table.row_count": row_count},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 2}


def test_distinct_metric_spark():
    engine = _build_spark_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}))

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert pd.Series(index=[1, 2, 3], data=[2, 2, 2]).equals(metrics[desired_metric.id])

    desired_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"column.value_counts": desired_metric},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: {1, 2, 3}}


def test_distinct_metric_sa():
    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )
    desired_metric_b = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric, desired_metric_b)
    )
    assert pd.Series(index=[1, 2, 3], data=[2, 2, 2]).equals(metrics[desired_metric.id])
    assert pd.Series(index=[4], data=[6]).equals(metrics[desired_metric_b.id])

    desired_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"column.value_counts": desired_metric},
    )
    desired_metric_b = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={"column.value_counts": desired_metric_b},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric, desired_metric_b), metrics=metrics
    )
    assert results[desired_metric.id] == {1, 2, 3}
    assert results[desired_metric_b.id] == {4}


def test_distinct_metric_pd():
    engine = _build_pandas_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}))

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert pd.Series(index=[1, 2, 3], data=[2, 2, 2]).equals(metrics[desired_metric.id])

    desired_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"column.value_counts": desired_metric},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: {1, 2, 3}}


def test_sa_batch_aggregate_metrics():
    import datetime

    engine = _build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column.aggregate.max",
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
        metric_name="column.aggregate.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
    )
    start = datetime.datetime.now()
    res = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        )
    )
    end = datetime.datetime.now()
    print("t1")
    print(end - start)
    assert res[desired_metric_1.id] == 3
    assert res[desired_metric_2.id] == 1
    assert res[desired_metric_3.id] == 4
    assert res[desired_metric_4.id] == 4


def test_sparkdf_batch_aggregate_metrics(caplog):
    import datetime

    engine = _build_spark_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]})
    )

    desired_metric_1 = MetricConfiguration(
        metric_name="column.aggregate.max",
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
        metric_name="column.aggregate.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
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
        )
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
