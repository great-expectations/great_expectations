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

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
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
def column_pair_function_partial(  # noqa: C901 - 16
    engine: Type[ExecutionEngine], partial_fn_type: Optional[str] = None, **kwargs
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature.

    A metric function that is decorated as a column_pair_function_partial will be called with the engine-specific
    column_list type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine: The `ExecutionEngine` used to to evaluate the condition
        partial_fn_type: The metric function
        **kwargs: Arguments passed to specified function

    Returns:
        An annotated metric_function which will be called with a simplified signature.
    """
    domain_type = MetricDomainTypes.COLUMN_PAIR
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_SERIES

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_SERIES:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_SERIES.value}" for \
"column_pair_function_partial" "partial_fn_type" property."""
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

                # noinspection PyPep8Naming
                column_A_name = accessor_domain_kwargs["column_A"]
                # noinspection PyPep8Naming
                column_B_name = accessor_domain_kwargs["column_B"]

                column_names: List[Union[str, sqlalchemy.quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                values = metric_fn(
                    cls,
                    df[column_A_name],
                    df[column_B_name],
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
"column_pair_function_partial" "partial_fn_type" property."""
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

                # noinspection PyPep8Naming
                column_A_name = accessor_domain_kwargs["column_A"]
                # noinspection PyPep8Naming
                column_B_name = accessor_domain_kwargs["column_B"]

                column_names: List[Union[str, sqlalchemy.quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                column_pair_function = metric_fn(
                    cls,
                    sa.column(column_A_name),
                    sa.column(column_B_name),
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    column_pair_function,
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
"column_pair_function_partial" "partial_fn_type" property."""
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

                # noinspection PyPep8Naming
                column_A_name = accessor_domain_kwargs["column_A"]
                # noinspection PyPep8Naming
                column_B_name = accessor_domain_kwargs["column_B"]

                column_names: List[Union[str, sqlalchemy.quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                column_pair_function = metric_fn(
                    cls,
                    data[column_A_name],
                    data[column_B_name],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    column_pair_function,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    else:
        raise ValueError(
            'Unsupported engine for "column_pair_function_partial" metric function decorator.'
        )
