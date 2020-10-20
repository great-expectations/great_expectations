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

    batch_spec = SqlAlchemyDatasourceTableBatchSpec(table="test")
    batch = engine.load_batch(batch_spec=batch_spec)

    # engine._batches = {"batch_id": df}

    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
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
    assert results == {desired_metric.id: 4}


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
    df = spark.createDataFrame(df)

    engine = SparkDFExecutionEngine()
    batch = engine.load_batch(
        batch_definition={"data_asset_name": "foo", "partition_name": "bar"},
        batch_spec=BatchSpec({"blarg": "bah"}),
        in_memory_dataset=df,
    )
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
    )

    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
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
    assert results == {desired_metric.id: 4}


def test_map_metric_pd():
    engine = PandasExecutionEngine()
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1]},
    )
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    engine._batches = {"batch_id": df}
    # results = engine.resolve_metrics(batches={"batch_id": batch}, metrics_to_resolve=(desired_metric,))
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 1}


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
