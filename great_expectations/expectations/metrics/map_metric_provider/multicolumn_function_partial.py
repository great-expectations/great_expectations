from __future__ import annotations

import logging
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Type,
    Union,
)

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypes,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import (
    metric_partial,
)
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy


def multicolumn_function_partial(  # noqa: C901 - 16
    engine: Type[ExecutionEngine],
    partial_fn_type: Optional[Union[str, MetricPartialFunctionTypes]] = None,
    **kwargs,
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature.

    A metric function that is decorated as a multicolumn_function_partial will be called with the engine-specific
    column_list type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine: The `ExecutionEngine` used to to evaluate the condition
        partial_fn_type: The metric function
        **kwargs: Arguments passed to specified function

    Returns:
        An annotated metric_function which will be called with a simplified signature.
    """
    domain_type = MetricDomainTypes.MULTICOLUMN
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_SERIES

        partial_fn_type__typed = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_SERIES:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_SERIES.value}" for \
"multicolumn_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type__typed,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(  # noqa: PLR0913
                cls,
                execution_engine: PandasExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
            ):
                (
                    df,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs, domain_type=domain_type
                )

                column_list: List[
                    Union[str, sqlalchemy.quoted_name]
                ] = accessor_domain_kwargs["column_list"]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=metrics["table.columns"],
                )

                values = metric_fn(
                    cls,
                    df[column_list],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return values, compute_domain_kwargs, accessor_domain_kwargs

            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_FN

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_FN:
            raise ValueError(
                f"""SqlAlchemyExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_FN.value}" for \
"multicolumn_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(  # noqa: PLR0913
                cls,
                execution_engine: SqlAlchemyExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
            ):
                (
                    selectable,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs, domain_type=domain_type
                )

                column_list: List[
                    Union[str, sqlalchemy.quoted_name]
                ] = accessor_domain_kwargs["column_list"]

                table_columns = metrics["table.columns"]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=table_columns,
                )

                sqlalchemy_engine: sqlalchemy.Engine = execution_engine.engine

                column_selector = [
                    sa.column(column_name) for column_name in column_list
                ]
                dialect = execution_engine.dialect_module
                multicolumn_function = metric_fn(
                    cls,
                    column_selector,
                    **metric_value_kwargs,
                    _column_names=column_list,
                    _table_columns=table_columns,
                    _dialect=dialect,
                    _table=selectable,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )
                return (
                    multicolumn_function,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_FN

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_FN:
            raise ValueError(
                f"""SparkDFExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_FN.value}" for \
"multicolumn_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(  # noqa: PLR0913
                cls,
                execution_engine: SparkDFExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
            ):
                (
                    data,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs, domain_type=domain_type
                )

                column_list: List[
                    Union[str, sqlalchemy.quoted_name]
                ] = accessor_domain_kwargs["column_list"]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=metrics["table.columns"],
                )

                multicolumn_function = metric_fn(
                    cls,
                    data[column_list],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    multicolumn_function,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    else:
        raise ValueError(
            'Unsupported engine for "multicolumn_function_partial" metric function decorator.'
        )
