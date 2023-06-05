from __future__ import annotations

import logging
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Optional,
    Type,
    Union,
)

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core._docs_decorators import public_api
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


@public_api
def column_condition_partial(  # noqa: C901, PLR0915
    engine: Type[ExecutionEngine],
    partial_fn_type: Optional[Union[str, MetricPartialFunctionTypes]] = None,
    **kwargs,
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature.

    A column_condition_partial must provide a map function that evaluates to a boolean value; it will be used to provide
    supplemental metrics, such as the unexpected_value count, unexpected_values, and unexpected_rows.

    A metric function that is decorated as a column_condition_partial will be called with the engine-specific column
    type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine: The `ExecutionEngine` used to to evaluate the condition
        partial_fn_type: The metric function
        **kwargs: Arguments passed to specified function

    Returns:
        An annotated metric_function which will be called with a simplified signature.
    """
    domain_type = MetricDomainTypes.COLUMN
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_CONDITION_SERIES

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [MetricPartialFunctionTypes.MAP_CONDITION_SERIES]:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_CONDITION_SERIES.value}" for \
"column_condition_partial" "partial_fn_type" property."""
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

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )
                if filter_column_isnull:
                    df = df[df[column_name].notnull()]

                meets_expectation_series = metric_fn(
                    cls,
                    df[column_name],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    ~meets_expectation_series,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    elif issubclass(engine, SqlAlchemyExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_CONDITION_FN

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [
            MetricPartialFunctionTypes.MAP_CONDITION_FN,
            MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
        ]:
            raise ValueError(
                f"""SqlAlchemyExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_CONDITION_FN.value}" and \
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "column_condition_partial" "partial_fn_type" property."""
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

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                sqlalchemy_engine: sqlalchemy.Engine = execution_engine.engine

                dialect = execution_engine.dialect_module
                if dialect is None:
                    # Trino
                    if hasattr(sqlalchemy_engine, "dialect"):
                        dialect = sqlalchemy_engine.dialect

                expected_condition = metric_fn(
                    cls,
                    sa.column(column_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _execution_engine=execution_engine,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )

                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )
                if filter_column_isnull:
                    # If we "filter" (ignore) nulls then we allow null as part of our new expected condition
                    unexpected_condition = sa.and_(
                        sa.not_(sa.column(column_name).is_(None)),
                        sa.not_(expected_condition),
                    )
                else:
                    unexpected_condition = sa.not_(expected_condition)
                return (
                    unexpected_condition,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_CONDITION_FN

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [
            MetricPartialFunctionTypes.MAP_CONDITION_FN,
            MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
        ]:
            raise ValueError(
                f"""SparkDFExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_CONDITION_FN.value}" and \
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "column_condition_partial" "partial_fn_type" property."""
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

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                column = data[column_name]
                expected_condition = metric_fn(
                    cls,
                    column,
                    **metric_value_kwargs,
                    _table=data,
                    _metrics=metrics,
                    _compute_domain_kwargs=compute_domain_kwargs,
                    _accessor_domain_kwargs=accessor_domain_kwargs,
                )
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", True)
                )
                if partial_fn_type == MetricPartialFunctionTypes.WINDOW_CONDITION_FN:
                    if filter_column_isnull:
                        compute_domain_kwargs = (
                            execution_engine.add_column_row_condition(
                                compute_domain_kwargs, column_name=column_name
                            )
                        )
                    unexpected_condition = ~expected_condition
                else:
                    if filter_column_isnull:  # noqa: PLR5501
                        unexpected_condition = column.isNotNull() & ~expected_condition
                    else:
                        unexpected_condition = ~expected_condition

                return (
                    unexpected_condition,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper
    else:
        raise ValueError(
            'Unsupported engine for "column_condition_partial" metric function decorator.'
        )
