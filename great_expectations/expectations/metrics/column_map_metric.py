from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type

import numpy as np

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions.metric_exceptions import MetricError
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sparkdf_execution_engine import (
    F,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
    sa,
)
from great_expectations.expectations.metrics.metric import Metric, metric
from great_expectations.expectations.registry import register_metric
from great_expectations.validator.validation_graph import MetricConfiguration


def column_map_function(engine: Type[ExecutionEngine], **kwargs):
    """Return the column aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "PandasExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )

                df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs,
                )
                if filter_column_isnull:
                    df = df[df[accessor_domain_kwargs["column"]].notnull()]
                values = metric_fn(
                    cls,
                    df[accessor_domain_kwargs["column"]],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return values

            inner_func.map_function_metric_engine = engine
            inner_func.map_function_metric_kwargs = kwargs
            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SqlAlchemyExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )
                if filter_column_isnull:
                    compute_domain_kwargs = execution_engine.add_column_null_filter_row_condition(
                        metric_domain_kwargs
                    )
                else:
                    # We do not copy here because if compute domain is different, it will be copied by get_compute_domain
                    compute_domain_kwargs = metric_domain_kwargs
                (
                    selectable,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(compute_domain_kwargs)
                column_name = accessor_domain_kwargs["column"]
                dialect = execution_engine.dialect
                column_function = metric_fn(
                    cls,
                    sa.column(column_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _metrics=metrics,
                )
                return column_function, compute_domain_kwargs

            inner_func.map_function_metric_engine = engine
            inner_func.map_function_metric_kwargs = kwargs
            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SparkDFExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )

                if filter_column_isnull:
                    compute_domain_kwargs = execution_engine.add_column_null_filter_row_condition(
                        metric_domain_kwargs
                    )
                else:
                    # We do not copy here because if compute domain is different, it will be copied by get_compute_domain
                    compute_domain_kwargs = metric_domain_kwargs

                (
                    data,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=compute_domain_kwargs
                )
                column_name = accessor_domain_kwargs["column"]
                column_function = metric_fn(
                    cls,
                    column=data[column_name],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return column_function, compute_domain_kwargs

            inner_func.map_function_metric_engine = engine
            inner_func.map_function_metric_kwargs = kwargs
            return inner_func

        return wrapper

    else:
        raise ValueError("Unsupported engine for column_aggregate_metric")


def column_map_condition(engine: Type[ExecutionEngine], **kwargs):
    """Return the column aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "PandasExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )

                df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs,
                )
                if filter_column_isnull:
                    df = df[df[accessor_domain_kwargs["column"]].notnull()]

                meets_expectation_series = metric_fn(
                    cls,
                    df[accessor_domain_kwargs["column"]],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return meets_expectation_series

            inner_func.map_condition_metric_engine = engine
            inner_func.map_condition_metric_kwags = kwargs
            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SqlAlchemyExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )

                (
                    selectable,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(metric_domain_kwargs)
                column_name = accessor_domain_kwargs["column"]
                dialect = execution_engine.dialect
                sqlalchemy_engine = execution_engine.engine

                expected_condition = metric_fn(
                    cls,
                    sa.column(column_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )
                if filter_column_isnull:
                    expected_condition = sa.and_(
                        sa.not_(sa.column(column_name).is_(None)), expected_condition
                    )
                else:
                    expected_condition = expected_condition
                return expected_condition, compute_domain_kwargs

            inner_func.map_condition_metric_engine = engine
            inner_func.map_condition_metric_kwags = kwargs
            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):

        def wrapper(metric_fn: Callable):
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SparkDFExecutionEngine",
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )
                (
                    data,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs
                )
                column_name = accessor_domain_kwargs["column"]
                column = data[column_name]
                expected_condition = metric_fn(
                    cls, column, **metric_value_kwargs, _metrics=metrics
                )
                if filter_column_isnull:
                    expected_condition = column.isNotNull() & expected_condition
                return expected_condition, compute_domain_kwargs

            inner_func.map_condition_metric_engine = engine
            inner_func.map_condition_metric_kwags = kwargs
            return inner_func

        return wrapper
    else:
        raise ValueError("Unsupported engine for column_map_condition")


def _pandas_column_map_count(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """Return the count of nonzero values from the map-style metric in the metrics dictionary"""
    return np.count_nonzero(~metrics.get("expected_condition"))


def _pandas_column_map_values(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
        domain_kwargs=metric_domain_kwargs,
    )
    data = df[accessor_domain_kwargs["column"]]

    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    boolean_mapped_success_values = metrics.get("expected_condition")
    if result_format["result_format"] == "COMPLETE":
        return list(
            data[
                # boolean_mapped_success_values[
                #     metric_name[: -len(".unexpected_values")]
                # ]
                boolean_mapped_success_values
                == False
            ]
        )
    else:
        return list(
            data[
                # boolean_mapped_success_values[
                #     metric_name[: -len(".unexpected_values")]
                # ]
                boolean_mapped_success_values
                == False
            ][: result_format["partial_unexpected_count"]]
        )


def _pandas_column_map_index(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    filter_column_isnull,
    **kwargs,
):
    """Maps metric values and kwargs to results of success kwargs"""
    df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
        domain_kwargs=metric_domain_kwargs,
    )
    data = df[accessor_domain_kwargs["column"]]
    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    boolean_mapped_success_values = metrics.get("expected_condition")
    if result_format["result_format"] == "COMPLETE":
        return list(data[boolean_mapped_success_values == False].index)
    else:
        return list(
            data[boolean_mapped_success_values == False].index[
                : result_format["partial_unexpected_count"]
            ]
        )


def _pandas_column_map_value_counts(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    filter_column_isnull,
    **kwargs,
):
    """Returns respective value counts for distinct column values"""
    df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
        domain_kwargs=metric_domain_kwargs,
    )
    data = df[accessor_domain_kwargs["column"]]
    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    boolean_mapped_success_values = metrics.get("expected_condition")
    value_counts = None
    try:
        value_counts = data[boolean_mapped_success_values == False].value_counts()
    except ValueError:
        try:
            value_counts = (
                data[boolean_mapped_success_values == False].apply(tuple).value_counts()
            )
        except ValueError:
            pass

    if not value_counts:
        raise MetricError("Unable to compute value counts")

    if result_format["result_format"] == "COMPLETE":
        return value_counts
    else:
        return value_counts[result_format["partial_unexpected_count"]]


def _pandas_column_map_rows(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    filter_column_isnull,
    **kwargs,
):
    """Return values from the specified domain (ignoring the column constraint) that match the map-style metric in the metrics dictionary."""
    row_domain = {k: v for (k, v) in metric_domain_kwargs.items() if k != "column"}
    df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
        domain_kwargs=metric_domain_kwargs,
    )
    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    boolean_mapped_success_values = metrics.get("expected_condition")
    if result_format["result_format"] == "COMPLETE":
        return df[boolean_mapped_success_values == False]
    else:
        return df[boolean_mapped_success_values == False][
            result_format["partial_unexpected_count"]
        ]


def _sqlalchemy_column_map_count(
    cls,
    execution_engine: "SqlAlchemyExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """Returns column nonnull count for ColumnMapExpectations"""
    expected_condition, compute_domain_kwargs = metrics.get("expected_condition")
    return (
        sa.func.sum(sa.case([(expected_condition, 0,)], else_=1,)),
        compute_domain_kwargs,
    )


def _sqlalchemy_column_map_values(
    cls,
    execution_engine: "SqlAlchemyExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """
    Particularly for the purpose of finding unexpected values, returns all the metric values which do not meet an
    expected Expectation condition for ColumnMapExpectation Expectations.
    """
    (
        selectable,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(metric_domain_kwargs)

    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    expected_condition = metrics.get("expected_condition")
    unexpected_condition = sa.not_(expected_condition)

    query = (
        sa.select([sa.column(accessor_domain_kwargs.get("column"))])
        .select_from(selectable)
        .where(unexpected_condition)
    )
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    return execution_engine.engine.execute(query).fetchall()


def _sqlalchemy_column_map_value_counts(
    cls,
    execution_engine: "SqlAlchemyExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """
    Returns value counts for all the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    (
        selectable,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(metric_domain_kwargs)

    expected_condition = metrics.get("expected_condition")
    unexpected_condition = sa.not_(expected_condition)
    column = sa.column(accessor_domain_kwargs["column"])
    return execution_engine.engine.execute(
        sa.select([column, sa.func.count(column)])
        .select_from(selectable)
        .where(unexpected_condition)
        .groupby(column)
    ).fetchall()


def _sqlalchemy_column_map_rows(
    cls,
    execution_engine: "SqlAlchemyExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    """
    Returns all rows of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    (
        selectable,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(metric_domain_kwargs)

    result_format = metric_value_kwargs["result_format"]
    expected_condition = metrics.get("expected_condition")
    unexpected_condition = sa.not_(expected_condition)
    query = (
        sa.select([sa.text("*")]).select_from(selectable).where(unexpected_condition)
    )
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    return execution_engine.engine.execute(query).fetchall()


def _spark_column_map_count(
    cls,
    execution_engine: "SparkDFExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    expected_condition, compute_domain_kwargs = metrics.get("expected_condition")
    return F.sum(F.when(expected_condition, 0).otherwise(1)), compute_domain_kwargs


def _spark_column_map_values(
    cls,
    execution_engine: "SparkDFExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    (
        data,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(metric_domain_kwargs)

    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    result_format = metric_value_kwargs["result_format"]
    condition = metrics.get("expected_condition")
    column_name = accessor_domain_kwargs["column"]
    # column = self._get_eval_column_name(metric_domain_kwargs["column"])
    filtered = data.filter(~condition)
    if result_format["result_format"] == "COMPLETE":
        return list(filtered.select(F.col(column_name)).collect())
    else:
        return list(
            filtered.select(F.col(column_name))
            .limit(result_format["partial_unexpected_count"])
            .collect()
        )


def _spark_column_map_value_counts(
    cls,
    execution_engine: "SparkDFExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    (
        data,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(metric_domain_kwargs)
    """Returns all unique values in the column and their corresponding counts"""
    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    condition = metrics.get("expected_condition")
    column_name = accessor_domain_kwargs["column"]
    filtered = data.filter(~condition)
    value_counts = filtered.groupBy(F.col(column_name)).count()
    if result_format["result_format"] == "COMPLETE":
        return value_counts
    else:
        return value_counts[result_format["partial_unexpected_count"]]


def _spark_column_map_rows(
    cls,
    execution_engine: "PandasExecutionEngine",
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[Tuple, Any],
    **kwargs,
):
    row_domain = {k: v for (k, v) in metric_domain_kwargs.items() if k != "column"}

    (
        data,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = execution_engine.get_compute_domain(row_domain)

    # column_map_values adds "result_format" as a value_kwarg to its underlying metric; get and remove it
    result_format = metric_value_kwargs["result_format"]
    condition = metrics.get("expected_condition")
    filtered = data.filter(~condition)
    if result_format["result_format"] == "COMPLETE":
        return filtered.collect()
    else:
        return filtered.limit(result_format["partial_unexpected_count"]).collect()


class ColumnMapMetric(Metric):
    condition_domain_keys = (
        "batch_id",
        "table",
        "column",
    )
    function_domain_keys = (
        "batch_id",
        "table",
        "column",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()
    filter_column_isnull = True

    @classmethod
    def _register_metric_functions(cls):
        if not hasattr(cls, "function_metric_name") and not hasattr(
            cls, "condition_metric_name"
        ):
            return

        for attr, candidate_metric_fn in cls.__dict__.items():
            if not hasattr(
                candidate_metric_fn, "map_condition_metric_engine"
            ) and not hasattr(candidate_metric_fn, "map_function_metric_engine"):
                # This is not a metric
                continue
            if hasattr(candidate_metric_fn, "map_condition_metric_engine"):
                engine = candidate_metric_fn.map_condition_metric_engine
                if not issubclass(engine, ExecutionEngine):
                    raise ValueError(
                        "metric functions must be defined with an Execution Engine"
                    )
                if not hasattr(cls, "condition_metric_name"):
                    raise ValueError(
                        "A ColumnMapMetric must have a metric_condition_name to have a decorated column_map_condition method."
                    )

                # rename for readability
                condition_provider = candidate_metric_fn
                metric_name = cls.condition_metric_name
                metric_domain_keys = cls.condition_domain_keys
                metric_value_keys = cls.condition_value_keys
                map_condition_metric_kwags = getattr(
                    condition_provider, "map_condition_metric_kwags", dict()
                )
                if issubclass(engine, PandasExecutionEngine):
                    register_metric(
                        metric_name=metric_name,
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".count",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_column_map_count,
                        bundle_metric=True,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_values",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_column_map_values,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_value_counts",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_column_map_value_counts,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_rows",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_column_map_rows,
                        bundle_metric=False,
                    )

                if issubclass(engine, SqlAlchemyExecutionEngine):
                    register_metric(
                        metric_name=metric_name,
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".count",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_column_map_count,
                        bundle_metric=True,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_values",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_column_map_values,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_value_counts",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_column_map_value_counts,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_rows",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_column_map_rows,
                        bundle_metric=False,
                    )
                elif issubclass(engine, SparkDFExecutionEngine):
                    register_metric(
                        metric_name=metric_name,
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".count",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_column_map_count,
                        bundle_metric=True,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_values",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_column_map_values,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_value_counts",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_column_map_value_counts,
                        bundle_metric=False,
                    )
                    register_metric(
                        metric_name=metric_name + ".unexpected_rows",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_column_map_rows,
                        bundle_metric=False,
                    )
            if hasattr(candidate_metric_fn, "map_function_metric_engine"):
                engine = candidate_metric_fn.map_function_metric_engine
                if not issubclass(engine, ExecutionEngine):
                    raise ValueError(
                        "metric functions must be defined with an Execution Engine"
                    )
                if not hasattr(cls, "function_metric_name"):
                    raise ValueError(
                        "A ColumnMapMetric must have a function_metric_name to have a decorated column_map_function method."
                    )
                # rename for readability
                map_function_provider = candidate_metric_fn
                metric_name = cls.function_metric_name
                metric_domain_keys = cls.function_domain_keys
                metric_value_keys = cls.function_value_keys
                metric_map_function_kwargs = getattr(
                    map_function_provider, "metric_map_function_kwargs", dict()
                )
                register_metric(
                    metric_name=metric_name,
                    metric_domain_keys=metric_domain_keys,
                    metric_value_keys=metric_value_keys,
                    execution_engine=engine,
                    metric_class=cls,
                    metric_provider=map_function_provider,
                    bundle_metric=False,
                )

    @classmethod
    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """This should return a dictionary:

        {
          "dependency_name": MetricConfiguration,
          ...
        }
        """
        metric_name = metric.metric_name
        base_metric_value_kwargs = {
            k: v for k, v in metric.metric_value_kwargs.items() if k != "result_format"
        }

        if metric_name.endswith(".count"):
            return {
                "expected_condition": MetricConfiguration(
                    metric_name[: -len(".count")],
                    metric.metric_domain_kwargs,
                    base_metric_value_kwargs,
                )
            }

        if metric_name.endswith(".unexpected_values"):
            return {
                "expected_condition": MetricConfiguration(
                    metric_name[: -len(".unexpected_values")],
                    metric.metric_domain_kwargs,
                    base_metric_value_kwargs,
                )
            }

        if metric_name.endswith(".unexpected_index_list"):
            return {
                "expected_condition": MetricConfiguration(
                    metric_name[: -len(".unexpected_index_list")],
                    metric.metric_domain_kwargs,
                    base_metric_value_kwargs,
                )
            }

        if metric_name.endswith(".unexpected_value_counts"):
            return {
                "expected_condition": MetricConfiguration(
                    metric_name[: -len(".unexpected_value_counts")],
                    metric.metric_domain_kwargs,
                    base_metric_value_kwargs,
                )
            }

        if metric_name.endswith(".unexpected_rows"):
            return {
                "expected_condition": MetricConfiguration(
                    metric_name[: -len(".unexpected_rows")],
                    metric.metric_domain_kwargs,
                    base_metric_value_kwargs,
                )
            }

        return dict()
