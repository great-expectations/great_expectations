import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple, Type

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric,
)

logger = logging.getLogger(__name__)


def aggregate_metric(engine: Type[ExecutionEngine], **kwargs):
    """Return the aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=PandasExecutionEngine, metric_fn_type="data")
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "PandasExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                df, _, _ = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs,
                )
                return metric_fn(cls, df, **metric_value_kwargs, _metrics=metrics,)

            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=SqlAlchemyExecutionEngine, metric_fn_type="aggregate_fn")
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SqlAlchemyExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                (
                    selectable,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(metric_domain_kwargs)
                dialect = execution_engine.dialect
                sqlalchemy_engine = execution_engine.engine
                metric_aggregate = metric_fn(
                    cls,
                    selectable,
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )
                return metric_aggregate, compute_domain_kwargs

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric(engine=SparkDFExecutionEngine, metric_fn_type="aggregate_fn")
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SparkDFExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                (
                    data,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs
                )
                metric_aggregate = metric_fn(
                    cls, data, **metric_value_kwargs, _metrics=metrics,
                )
                return metric_aggregate, compute_domain_kwargs

            return inner_func

        return wrapper

    else:
        raise ValueError("Unsupported engine for aggregate_metric")


class TableMetricProvider(MetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    metric_fn_type = "aggregate_fn"
