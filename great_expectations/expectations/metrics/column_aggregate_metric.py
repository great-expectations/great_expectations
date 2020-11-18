import logging
from functools import wraps
from typing import Any, Callable, Dict, Tuple, Type

from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.execution_engine.sparkdf_execution_engine import (
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
    sa,
)
from great_expectations.expectations.metrics.metric_provider import (
    metric_partial,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric import TableMetricProvider

logger = logging.getLogger(__name__)


def column_aggregate_value(
    engine: Type[ExecutionEngine],
    metric_fn_type="value",
    domain_type="column",
    **kwargs
):
    """Return the column aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric_value(
                engine=PandasExecutionEngine,
                metric_fn_type=metric_fn_type,
                domain_type=domain_type,
            )
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "PandasExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )

                df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs, domain_type=domain_type
                )
                if filter_column_isnull:
                    df = df[df[accessor_domain_kwargs["column"]].notnull()]
                return metric_fn(
                    cls,
                    column=df[accessor_domain_kwargs["column"]],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )

            return inner_func

        return wrapper
    else:
        raise ValueError(
            "column_aggregate_value decorator only supports PandasExecutionEngine"
        )


def column_aggregate_partial(engine: Type[ExecutionEngine], **kwargs):
    """Return the column aggregate metric decorator for the specified engine.

    Args:
        engine:
        **kwargs:

    Returns:

    """
    partial_fn_type = MetricPartialFunctionTypes.AGGREGATE_FN
    domain_type = MetricDomainTypes.COLUMN
    if issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=SqlAlchemyExecutionEngine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
            )
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SqlAlchemyExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )
                if filter_column_isnull:
                    compute_domain_kwargs = execution_engine.add_column_row_condition(
                        metric_domain_kwargs
                    )
                else:
                    # We do not copy here because if compute domain is different, it will be copied by get_compute_domain
                    compute_domain_kwargs = metric_domain_kwargs
                (
                    selectable,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    compute_domain_kwargs, domain_type=domain_type
                )
                column_name = accessor_domain_kwargs["column"]
                sqlalchemy_engine = execution_engine.engine
                dialect = sqlalchemy_engine.dialect
                metric_aggregate = metric_fn(
                    cls,
                    column=sa.column(column_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _column_name=column_name,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )
                return metric_aggregate, compute_domain_kwargs, accessor_domain_kwargs

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=SparkDFExecutionEngine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
            )
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: "SparkDFExecutionEngine",
                metric_domain_kwargs: Dict,
                metric_value_kwargs: Dict,
                metrics: Dict[Tuple, Any],
                runtime_configuration: Dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )

                if filter_column_isnull:
                    compute_domain_kwargs = execution_engine.add_column_row_condition(
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
                    domain_kwargs=compute_domain_kwargs, domain_type=domain_type
                )
                column_name = accessor_domain_kwargs["column"]
                metric_aggregate = metric_fn(
                    cls,
                    column=data[column_name],
                    **metric_value_kwargs,
                    _table=data,
                    _column_name=column_name,
                    _metrics=metrics,
                )
                return metric_aggregate, compute_domain_kwargs, accessor_domain_kwargs

            return inner_func

        return wrapper

    else:
        raise ValueError("Unsupported engine for column_aggregate_partial")


class ColumnMetricProvider(TableMetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    filter_column_isnull = False
