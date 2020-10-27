import numpy as np
import pandas as pd
import sqlalchemy as sa

from great_expectations.core.batch import Batch
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_environment.types import (
    BatchSpec,
    SqlAlchemyDatasourceTableBatchSpec,
)
from great_expectations.expectations.metrics import *
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.validation_graph import MetricConfiguration


def test_metric_loads():
    assert (
        get_metric_provider("column.aggregate.max", PandasExecutionEngine()) is not None
    )


def test_basic_metric():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 3}


def test_mean_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 1, 3, 3, None]})
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_stdev_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 1}


def test_max_metric_sa():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 1]})
    df.to_sql("test", eng)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    batch_data = SqlAlchemyBatchData(engine=eng, table_name="test")
    engine._batches = {"batch_id": batch_data}
    engine._active_batch_data_id = "batch_id"
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_max_metric_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 1]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"

    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_map_value_set_sa():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df.to_sql("test", eng)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )
    batch_data = SqlAlchemyBatchData(engine=eng, table_name="test")
    engine.load_batch_data("", batch_data)
    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # Note: metric_dependencies is optional here in the config when called from a validator.
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"expected_condition": desired_metric},
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
    df.to_sql("test", eng)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    desired_metric = MetricConfiguration(
        metric_name="column_values.of_type",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"type_": int},
    )

    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    batch = engine.load_batch(batch_spec=batch_spec)

    # engine._batches = {"batch_id": df}

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # Note: metric_dependencies is optional here in the config when called from a validator.
    desired_metric = MetricConfiguration(
        metric_name="column_values.of_type.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"type": int},
        metric_dependencies={"expected_condition": desired_metric},
    )

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 4}


def test_map_value_set_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
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

    engine = SparkDFExecutionEngine()
    batch = Batch(data=df)
    engine.load_batch_data(batch.id, batch.data)
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
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    batch = Batch(data=df)
    engine.load_batch_data(batch.id, batch.data)
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


def test_map_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_values.value_lengths",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1]},
    )
    df = pd.DataFrame({"a": ["a", "aaa", "bcbc", "defgh", None]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    # assert results == {desired_metric.id: 1}
    ser_expected_lengths = pd.Series([1, 3, 4, 5])
    assert ser_expected_lengths.equals(
        results[("column_values.value_lengths", "column=a", "value_set=[1]")]
    )


def test_map_unique_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1]},
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results[desired_metric.id]) == [True, True, False, False]


def test_map_unique_sa():
    import sqlalchemy as sa

    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df.to_sql("test", eng)
    engine = SqlAlchemyExecutionEngine(engine=eng)
    desired_metric = MetricConfiguration(
        metric_name="column_values.unique",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    batch = engine.load_batch(batch_spec=batch_spec)
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results[desired_metric.id]) == [True, True, False, False]


def test_map_column_value_lengths_between_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_value_lengths_between",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"min_value": 1, "max_value": 2},
    )
    df = pd.DataFrame({"a": ["a", "aa", "aaa", "aaaa", None]})
    batch = engine.load_batch(in_memory_dataset=df)
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results[desired_metric.id]) == [True, True, False, False]


def test_z_score_under_threshold_pd():
    engine = PandasExecutionEngine()
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
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
        metric_name="column_values.z_score.map_function",
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
        metric_dependencies={"column_values.z_score.map_function": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert list(results[desired_metric.id]) == [True, True, True]
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"expected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_z_score_under_threshold_sa():
    engine = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    df.to_sql("test", engine)
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
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
        metric_name="column_values.z_score.map_function",
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
        metric_dependencies={"column_values.z_score.map_function": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert list(results[desired_metric.id]) == [True, True, True]
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"expected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_z_score_under_threshold_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
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
        metric_name="column_values.z_score.map_function",
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
        metric_dependencies={"column_values.z_score.map_function": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert list(results[desired_metric.id]) == [True, True, True]
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"expected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_table_metric():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3, None], "b": [1, 2, 3, 3, None]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 5}


def test_column_pairs_equal_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.equal",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3], "b": [1, 2, 3, 3]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results.values())[0].equals(pd.Series([True, True, True, True]))


def test_column_pairs_equal_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.equal",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs=dict(),
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3, None], "b": [1, 2, 3, 3, None]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert list(results.values())[0].equals(pd.Series([True, True, True, True]))


def test_column_pairs_greater_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.a_greater_than_b",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs={
            "or_equal": True,
            "ignore_row_if": "either_value_is_missing",
        },
    )
    df = pd.DataFrame({"a": [2, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        list(results.values())[0]
        .reset_index(drop=True)
        .equals(pd.Series([True, True, True, True]))
    )


def test_column_pairs_in_set_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.in_set",
        metric_domain_kwargs={"column_a": "a", "column_b": "b"},
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "ignore_row_if": "either_value_is_missing",
        },
    )
    df = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine._active_batch_data_id = "batch_id"
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        list(results.values())[0]
        .reset_index(drop=True)
        .equals(pd.Series([False, True, True, True]))
    )


def test_table_metric_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 1]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"

    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_median_metric_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 3]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"

    row_count = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=dict(),
    )

    desired_metrics = (row_count,)
    metrics = engine.resolve_metrics(metrics_to_resolve=desired_metrics)

    desired_metric = MetricConfiguration(
        metric_name="column_values.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"table_row_count": row_count,},
    )
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 2}


def test_distinct_metric_spark():
    from pyspark.sql import DataFrame, SparkSession

    df = pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]})
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    engine._batches = {"batch_id": df}
    engine._active_batch_data_id = "batch_id"

    desired_metric = MetricConfiguration(
        metric_name="column.aggregate.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert set(list(results.values())[0].reset_index(drop=True)) == {1, 2, 3}
