import logging

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyBatchData,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.self_check.util import (
    build_pandas_engine,
    build_sa_engine,
    build_spark_engine,
)
from great_expectations.validator.validation_graph import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


def test_metric_loads():
    assert get_metric_provider("column.max", PandasExecutionEngine()) is not None


def test_basic_metric():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 3}


def test_mean_metric_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, None]}))

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 2}


def test_stdev_metric_pd():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, None]}))

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 1}


def test_max_metric_pd_column_exists():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 3}


def test_max_metric_pd_column_does_not_exist():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    batch = Batch(data=df)
    engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(
            metrics_to_resolve=(desired_metric,), metrics=metrics
        )
        metrics.update(results)
    assert (
        str(eee.value)
        == 'Error: The column "non_existent_column" in BatchData does not exist.'
    )


def test_max_metric_sa_column_exists(sa):
    engine = build_sa_engine(pd.DataFrame({"a": [1, 2, 1, None]}), sa)

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(partial_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": partial_metric,
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 2}


def test_max_metric_sa_column_does_not_exist(sa):
    engine = build_sa_engine(pd.DataFrame({"a": [1, 2, 1, None]}), sa)

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(
            metrics_to_resolve=(partial_metric,), metrics=metrics
        )
        metrics.update(results)
    assert (
        'Error: The column "non_existent_column" in BatchData does not exist.'
        in str(eee.value)
    )


def test_max_metric_spark_column_exists(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [1, 2, 1]}),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(partial_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": partial_metric,
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 2}


def test_max_metric_spark_column_does_not_exist(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame({"a": [1, 2, 1]}),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    partial_metric = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(
            metrics_to_resolve=(partial_metric,), metrics=metrics
        )
        metrics.update(results)
    assert (
        str(eee.value)
        == 'Error: The column "non_existent_column" in BatchData does not exist.'
    )


def test_map_value_set_sa(sa):
    engine = build_sa_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}), sa)

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": desired_metric},
    )

    metrics = engine.resolve_metrics(
        metrics_to_resolve=(aggregate_partial,), metrics=metrics
    )
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"metric_partial_fn": aggregate_partial},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 0}


def test_map_of_type_sa(sa):
    eng = sa.create_engine("sqlite://")
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df.to_sql(name="test", con=eng, index=False)
    batch_data = SqlAlchemyBatchData(
        execution_engine=eng, table_name="test", source_table_name="test"
    )
    engine = SqlAlchemyExecutionEngine(
        engine=eng, batch_data_dict={"my_id": batch_data}
    )
    desired_metric = MetricConfiguration(
        metric_name="table.column_types",
        metric_domain_kwargs={},
        metric_value_kwargs={},
    )

    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results[desired_metric.id][0]["name"] == "a"
    assert isinstance(results[desired_metric.id][0]["type"], sa.FLOAT)


def test_map_value_set_spark(spark_session, basic_spark_df_execution_engine):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 3, None]},
        ),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name="column_values.in_set.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,), metrics=metrics
    )
    metrics.update(results)

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(aggregate_partial,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"metric_partial_fn": aggregate_partial},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 0}

    # We run the same computation again, this time with None being replaced by nan instead of NULL
    # to demonstrate this behavior
    df = pd.DataFrame({"a": [1, 2, 3, 3, None]})
    df = spark_session.createDataFrame(df)
    engine = basic_spark_df_execution_engine
    engine.load_batch_data(batch_id="my_id", batch_data=df)

    condition_metric = MetricConfiguration(
        metric_name="column_values.in_set.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,), metrics=metrics
    )
    metrics.update(results)

    # Note: metric_dependencies is optional here in the config when called from a validator.
    aggregate_partial = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"unexpected_condition": condition_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(aggregate_partial,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.in_set.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3]},
        metric_dependencies={"metric_partial_fn": aggregate_partial},
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: 1}


def test_map_column_value_lengths_between_pd():
    engine = build_pandas_engine(
        pd.DataFrame({"a": ["a", "aaa", "bcbc", "defgh", None]})
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.value_length.map",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    ser_expected_lengths = pd.Series([1, 3, 4, 5])
    result_series, _, _ = results[desired_metric.id]
    assert ser_expected_lengths.equals(result_series)


def test_map_unique_pd_column_exists():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,),
        metrics=metrics,
    )

    assert list(results[desired_metric.id][0]) == [False, False, True, True]


def test_map_unique_pd_column_does_not_exist():
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 3, 3, None]}))

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        results = engine.resolve_metrics(
            metrics_to_resolve=(desired_metric,), metrics=metrics
        )
    assert (
        str(eee.value)
        == 'Error: The column "non_existent_column" in BatchData does not exist.'
    )


def test_map_unique_sa_column_exists(sa):
    engine = build_sa_engine(
        pd.DataFrame(
            {"a": [1, 2, 3, 3, None], "b": ["foo", "bar", "baz", "qux", "fish"]}
        ),
        sa,
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,), metrics=metrics
    )
    metrics.update(results)

    # This is no longer a MAP_CONDITION because mssql does not support it. Instead, it is a WINDOW_CONDITION
    #
    # aggregate_fn = MetricConfiguration(
    #     metric_name="column_values.unique.unexpected_count.aggregate_fn",
    #     metric_domain_kwargs={"column": "a"},
    #     metric_value_kwargs=dict(),
    #     metric_dependencies={"unexpected_condition": condition_metric},
    # )
    # aggregate_fn_metrics = engine.resolve_metrics(
    #     metrics_to_resolve=(aggregate_fn,), metrics=metrics
    # )

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        # metric_dependencies={"metric_partial_fn": aggregate_fn},
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,),
        metrics=metrics,  # metrics=aggregate_fn_metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
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
    metrics.update(results)
    assert results[desired_metric.id] == [(3, "baz"), (3, "qux")]


def test_map_unique_sa_column_does_not_exist(sa):
    engine = build_sa_engine(
        pd.DataFrame(
            {"a": [1, 2, 3, 3, None], "b": ["foo", "bar", "baz", "qux", "fish"]}
        ),
        sa,
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        metrics = engine.resolve_metrics(
            metrics_to_resolve=(condition_metric,), metrics=metrics
        )
    assert (
        'Error: The column "non_existent_column" in BatchData does not exist.'
        in str(eee.value)
    )


def test_map_unique_spark_column_exists(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 3, 3, 4, None],
                "b": [None, "foo", "bar", "baz", "qux", "fish"],
            }
        ),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(condition_metric,), metrics=metrics
    )
    metrics.update(results)

    # unique is a *window* function so does not use the aggregate_fn version of unexpected count
    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == 2

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == [3, 3]

    desired_metric = MetricConfiguration(
        metric_name="column_values.unique.unexpected_value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        metric_dependencies={
            "unexpected_condition": condition_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
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
    metrics.update(results)
    assert results[desired_metric.id] == [(3, "bar"), (3, "baz")]


def test_map_unique_spark_column_does_not_exist(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {
                "a": [1, 2, 3, 3, 4, None],
                "b": [None, "foo", "bar", "baz", "qux", "fish"],
            }
        ),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    condition_metric = MetricConfiguration(
        metric_name="column_values.unique.condition",
        metric_domain_kwargs={"column": "non_existent_column"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    with pytest.raises(ge_exceptions.ExecutionEngineError) as eee:
        # noinspection PyUnusedLocal
        metrics = engine.resolve_metrics(
            metrics_to_resolve=(condition_metric,), metrics=metrics
        )
    assert (
        str(eee.value)
        == 'Error: The column "non_existent_column" in BatchData does not exist.'
    )


def test_z_score_under_threshold_pd():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "column.standard_deviation": stdev,
            "column.mean": mean,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={
            "column_values.z_score.map": desired_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert list(results[desired_metric.id][0]) == [False, False, False]
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


def test_z_score_under_threshold_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3, 3, None]},
        ),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": mean},
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": stdev,
            "table.columns": table_columns_metric,
        },
    )
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "column.standard_deviation": stdev,
            "column.mean": mean,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={
            "column_values.z_score.map": desired_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"unexpected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"metric_partial_fn": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results[desired_metric.id] == 0


def test_table_metric_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3, None], "b": [1, 2, 3, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results == {desired_metric.id: 5}


def test_column_pairs_equal_metric_pd():
    df = pd.DataFrame({"a": [1, 2, 3, 3], "b": [1, 2, 3, 3]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.equal.condition",
        metric_domain_kwargs={"column_A": "a", "column_B": "b"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert results[desired_metric.id][0].equals(pd.Series([True, True, True, True]))


def test_column_pairs_greater_metric_pd():
    df = pd.DataFrame({"a": [2, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.a_greater_than_b.condition",
        metric_domain_kwargs={"column_A": "a", "column_B": "b"},
        metric_value_kwargs={
            "or_equal": True,
            "ignore_row_if": "either_value_is_missing",
        },
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        results[desired_metric.id][0]
        .reset_index(drop=True)
        .equals(pd.Series([True, True, True, True]))
    )


def test_column_pairs_in_set_metric_pd():
    df = pd.DataFrame({"a": [10, 3, 4, None, 3, None], "b": [1, 2, 3, None, 3, 5]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})
    desired_metric = MetricConfiguration(
        metric_name="column_pair_values.in_set.condition",
        metric_domain_kwargs={"column_A": "a", "column_B": "b"},
        metric_value_kwargs={
            "value_pairs_set": [(2, 1), (3, 2), (4, 3), (3, 3)],
            "ignore_row_if": "either_value_is_missing",
        },
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))
    assert (
        results[desired_metric.id][0]
        .reset_index(drop=True)
        .equals(pd.Series([False, True, True, True]))
    )


def test_table_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1]},
        ),
        batch_id="my_id",
    )

    desired_metric = MetricConfiguration(
        metric_name="table.row_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))

    desired_metric = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=results
    )

    assert results == {desired_metric.id: 3}


def test_median_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 3]},
        ),
        batch_id="my_id",
    )

    desired_metric = MetricConfiguration(
        metric_name="table.row_count.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
    )
    metrics = engine.resolve_metrics(metrics_to_resolve=(desired_metric,))

    row_count = MetricConfiguration(
        metric_name="table.row_count",
        metric_domain_kwargs={},
        metric_value_kwargs=dict(),
        metric_dependencies={"metric_partial_fn": desired_metric},
    )
    metrics = engine.resolve_metrics(metrics_to_resolve=(row_count,), metrics=metrics)

    desired_metric = MetricConfiguration(
        metric_name="column.median",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={"table.row_count": row_count},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    assert results == {desired_metric.id: 2}


def test_distinct_metric_spark(spark_session):
    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1, 2, 3, 3, None]},
        ),
        batch_id="my_id",
    )

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


def test_distinct_metric_sa(sa):
    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
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
    engine = build_pandas_engine(pd.DataFrame({"a": [1, 2, 1, 2, 3, 3]}))

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column.value_counts",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"sort": "value", "collate": None},
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert pd.Series(index=[1, 2, 3], data=[2, 2, 2]).equals(metrics[desired_metric.id])

    desired_metric = MetricConfiguration(
        metric_name="column.distinct_values",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "column.value_counts": desired_metric,
            "table.columns": table_columns_metric,
        },
    )

    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results == {desired_metric.id: {1, 2, 3}}


def test_sa_batch_aggregate_metrics(caplog, sa):
    import datetime

    engine = build_sa_engine(
        pd.DataFrame({"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]}), sa
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_1,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_2,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_3,
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "metric_partial_fn": desired_metric_4,
            "table.columns": table_columns_metric,
        },
    )
    caplog.clear()
    caplog.set_level(logging.DEBUG, logger="great_expectations")
    start = datetime.datetime.now()
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)
    end = datetime.datetime.now()
    print("t1")
    print(end - start)
    assert results[desired_metric_1.id] == 3
    assert results[desired_metric_2.id] == 1
    assert results[desired_metric_3.id] == 4
    assert results[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SqlAlchemyExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message


def test_sparkdf_batch_aggregate_metrics(caplog, spark_session):
    import datetime

    engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark_session,
        df=pd.DataFrame(
            {"a": [1, 2, 1, 2, 3, 3], "b": [4, 4, 4, 4, 4, 4]},
        ),
        batch_id="my_id",
    )

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)
    metrics.update(results)

    desired_metric_1 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_2 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_3 = MetricConfiguration(
        metric_name="column.max.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metric_4 = MetricConfiguration(
        metric_name="column.min.aggregate_fn",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=dict(),
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)

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
    results = engine.resolve_metrics(
        metrics_to_resolve=(
            desired_metric_1,
            desired_metric_2,
            desired_metric_3,
            desired_metric_4,
        ),
        metrics=metrics,
    )
    metrics.update(results)
    end = datetime.datetime.now()
    print(end - start)
    assert results[desired_metric_1.id] == 3
    assert results[desired_metric_2.id] == 1
    assert results[desired_metric_3.id] == 4
    assert results[desired_metric_4.id] == 4

    # Check that all four of these metrics were computed on a single domain
    found_message = False
    for record in caplog.records:
        if (
            record.message
            == "SparkDFExecutionEngine computed 4 metrics on domain_id ()"
        ):
            found_message = True
    assert found_message
