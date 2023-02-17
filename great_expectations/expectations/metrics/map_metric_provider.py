from __future__ import annotations

import inspect
import logging
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)

import numpy as np
import pandas as pd

import great_expectations.exceptions as gx_exceptions
from great_expectations.core import ExpectationConfiguration  # noqa: TCH001
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    MetricFunctionTypes,
    MetricPartialFunctionTypes,
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    OperationalError,
)
from great_expectations.expectations.metrics import MetaMetricProvider  # noqa: TCH001
from great_expectations.expectations.metrics.import_manager import F, quoted_name, sa
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_partial,
)
from great_expectations.expectations.metrics.util import (
    Engine,
    Insert,
    Label,
    Select,
    compute_unexpected_pandas_indices,
    get_dbms_compatible_column_names,
    get_sqlalchemy_source_table_and_schema,
    sql_statement_with_post_compile_to_string,
    verify_column_names_exist,
)
from great_expectations.expectations.registry import (
    get_metric_provider,
    register_metric,
)
from great_expectations.util import (
    generate_temporary_table_name,
    get_sqlalchemy_selectable,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    import pyspark

logger = logging.getLogger(__name__)


def column_function_partial(  # noqa: C901 - 19
    engine: Type[ExecutionEngine],
    partial_fn_type: Optional[str] = None,
    **kwargs,
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature.

    A metric function that is decorated as a column_function_partial will be called with the engine-specific column type
    and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine:
        partial_fn_type:
        **kwargs:

    Returns:
        An annotated metric_function which will be called with a simplified signature.

    """
    domain_type = MetricDomainTypes.COLUMN
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_SERIES

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_SERIES:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_SERIES.value}" for \
"column_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )
                if filter_column_isnull:
                    df = df[df[column_name].notnull()]

                values = metric_fn(
                    cls,
                    df[column_name],
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
        if partial_fn_type not in [MetricPartialFunctionTypes.MAP_FN]:
            raise ValueError(
                f"""SqlAlchemyExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_FN.value}" for \
"column_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: SqlAlchemyExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
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
                    domain_kwargs=compute_domain_kwargs, domain_type=domain_type
                )

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                dialect = execution_engine.dialect_module
                column_function = metric_fn(
                    cls,
                    sa.column(column_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _metrics=metrics,
                )
                return column_function, compute_domain_kwargs, accessor_domain_kwargs

            return inner_func

        return wrapper

    elif issubclass(engine, SparkDFExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_FN

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [
            MetricPartialFunctionTypes.MAP_FN,
            MetricPartialFunctionTypes.WINDOW_FN,
        ]:
            raise ValueError(
                f"""SparkDFExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_FN.value}" and \
"{MetricPartialFunctionTypes.WINDOW_FN.value}" for "column_function_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
                cls,
                execution_engine: SparkDFExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
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

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                column = data[column_name]
                column_function = metric_fn(
                    cls,
                    column=column,
                    **metric_value_kwargs,
                    _metrics=metrics,
                    _compute_domain_kwargs=compute_domain_kwargs,
                )
                return column_function, compute_domain_kwargs, accessor_domain_kwargs

            return inner_func

        return wrapper

    else:
        raise ValueError(
            'Unsupported engine for "column_function_partial" metric function decorator.'
        )


@public_api
def column_condition_partial(  # noqa: C901 - 23
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
            def inner_func(
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

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

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
            def inner_func(
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

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                sqlalchemy_engine: Engine = execution_engine.engine

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
            def inner_func(
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

                column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

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
                    if filter_column_isnull:
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
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
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
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
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
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
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


def column_pair_condition_partial(  # noqa: C901 - 16
    engine: Type[ExecutionEngine],
    partial_fn_type: Optional[Union[str, MetricPartialFunctionTypes]] = None,
    **kwargs,
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature. A
    column_pair_condition_partial must provide a map function that evaluates to a boolean value; it will be used to
    provide supplemental metrics, such as the unexpected_value count, unexpected_values, and unexpected_rows.

    A metric function that is decorated as a column_pair_condition_partial will be called with the engine-specific
    column_list type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine:
        partial_fn_type:
        **kwargs:

    Returns:
        An annotated metric_function which will be called with a simplified signature.

    """
    domain_type = MetricDomainTypes.COLUMN_PAIR
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_CONDITION_SERIES

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [MetricPartialFunctionTypes.MAP_CONDITION_SERIES]:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_CONDITION_SERIES.value}" for \
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
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                meets_expectation_series = metric_fn(
                    cls,
                    df[column_A_name],
                    df[column_B_name],
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
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "column_pair_condition_partial" "partial_fn_type" property.
"""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                sqlalchemy_engine: Engine = execution_engine.engine

                dialect = execution_engine.dialect_module
                expected_condition = metric_fn(
                    cls,
                    sa.column(column_A_name),
                    sa.column(column_B_name),
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )

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
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "column_pair_condition_partial" "partial_fn_type" property.
"""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_names: List[Union[str, quoted_name]] = [
                    column_A_name,
                    column_B_name,
                ]
                # noinspection PyPep8Naming
                column_A_name, column_B_name = get_dbms_compatible_column_names(
                    column_names=column_names,
                    batch_columns_list=metrics["table.columns"],
                )

                expected_condition = metric_fn(
                    cls,
                    data[column_A_name],
                    data[column_B_name],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    ~expected_condition,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    else:
        raise ValueError(
            'Unsupported engine for "column_pair_condition_partial" metric function decorator.'
        )


def multicolumn_function_partial(  # noqa: C901 - 16
    engine: Type[ExecutionEngine], partial_fn_type: Optional[str] = None, **kwargs
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

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type != MetricPartialFunctionTypes.MAP_SERIES:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_SERIES.value}" for \
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
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

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
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

                table_columns = metrics["table.columns"]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=table_columns,
                )

                sqlalchemy_engine: Engine = execution_engine.engine

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
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

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


def multicolumn_condition_partial(  # noqa: C901 - 16
    engine: Type[ExecutionEngine],
    partial_fn_type: Optional[Union[str, MetricPartialFunctionTypes]] = None,
    **kwargs,
):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature. A
    multicolumn_condition_partial must provide a map function that evaluates to a boolean value; it will be used to
    provide supplemental metrics, such as the unexpected_value count, unexpected_values, and unexpected_rows.

    A metric function that is decorated as a multicolumn_condition_partial will be called with the engine-specific
    column_list type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine:
        partial_fn_type:
        **kwargs:

    Returns:
        An annotated metric_function which will be called with a simplified signature.

    """
    domain_type = MetricDomainTypes.MULTICOLUMN
    if issubclass(engine, PandasExecutionEngine):
        if partial_fn_type is None:
            partial_fn_type = MetricPartialFunctionTypes.MAP_CONDITION_SERIES

        partial_fn_type = MetricPartialFunctionTypes(partial_fn_type)
        if partial_fn_type not in [MetricPartialFunctionTypes.MAP_CONDITION_SERIES]:
            raise ValueError(
                f"""PandasExecutionEngine only supports "{MetricPartialFunctionTypes.MAP_CONDITION_SERIES.value}" for \
"multicolumn_condition_partial" "partial_fn_type" property."""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=metrics["table.columns"],
                )

                meets_expectation_series = metric_fn(
                    cls,
                    df[column_list],
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
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "multicolumn_condition_partial" "partial_fn_type" property.
"""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=metrics["table.columns"],
                )

                sqlalchemy_engine: Engine = execution_engine.engine

                column_selector = [
                    sa.column(column_name) for column_name in column_list
                ]
                dialect = execution_engine.dialect_module
                expected_condition = metric_fn(
                    cls,
                    column_selector,
                    **metric_value_kwargs,
                    _dialect=dialect,
                    _table=selectable,
                    _sqlalchemy_engine=sqlalchemy_engine,
                    _metrics=metrics,
                )

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
"{MetricPartialFunctionTypes.WINDOW_CONDITION_FN.value}" for "multicolumn_condition_partial" "partial_fn_type" property.
"""
            )

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=engine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
                **kwargs,
            )
            @wraps(metric_fn)
            def inner_func(
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

                column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
                    "column_list"
                ]

                column_list = get_dbms_compatible_column_names(
                    column_names=column_list,
                    batch_columns_list=metrics["table.columns"],
                )

                expected_condition = metric_fn(
                    cls,
                    data[column_list],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )
                return (
                    ~expected_condition,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                )

            return inner_func

        return wrapper

    else:
        raise ValueError(
            'Unsupported engine for "multicolumn_condition_partial" metric function decorator.'
        )


def _pandas_map_condition_unexpected_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations"""
    return np.count_nonzero(metrics["unexpected_condition"][0])


def _pandas_column_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return list(domain_values)

    return list(domain_values[: result_format["partial_unexpected_count"]])


def _pandas_column_pair_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_values).
"""
        )

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [
        column_A_name,
        column_B_name,
    ]
    # noinspection PyPep8Naming
    column_A_name, column_B_name = get_dbms_compatible_column_names(
        column_names=column_names,
        batch_columns_list=metrics["table.columns"],
    )

    domain_values = df[column_names]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    unexpected_list = [
        value_pair
        for value_pair in zip(
            domain_values[column_A_name].values, domain_values[column_B_name].values
        )
    ]
    if result_format["result_format"] == "COMPLETE":
        return unexpected_list

    return unexpected_list[: result_format["partial_unexpected_count"]]


def _pandas_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
        raise ValueError(
            """No "column_A" and "column_B" found in provided metric_domain_kwargs, but it is required for a column pair map metric
(_pandas_column_pair_map_condition_filtered_row_count).
"""
        )

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return df.shape[0]


def _pandas_multicolumn_map_condition_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    domain_values = df[column_list]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return domain_values.to_dict("records")

    return domain_values[: result_format["partial_unexpected_count"]].to_dict("records")


def _pandas_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_pandas_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.shape[0]


# TODO: <Alex>11/15/2022: Please DO_NOT_DELETE this method (even though it is not currently utilized).  Thanks.</Alex>
def _pandas_column_map_series_and_domain_values(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    (
        map_series,
        compute_domain_kwargs_2,
        accessor_domain_kwargs_2,
    ) = metrics["metric_partial_fn"]
    assert (
        compute_domain_kwargs == compute_domain_kwargs_2
    ), "map_series and condition must have the same compute domain"
    assert (
        accessor_domain_kwargs == accessor_domain_kwargs_2
    ), "map_series and condition must have the same accessor kwargs"
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_series_and_domain_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    domain_values = domain_values[
        boolean_mapped_unexpected_values == True  # noqa: E712
    ]
    map_series = map_series[boolean_mapped_unexpected_values == True]  # noqa: E712

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return (
            list(domain_values),
            list(map_series),
        )

    return (
        list(domain_values[: result_format["partial_unexpected_count"]]),
        list(map_series[: result_format["partial_unexpected_count"]]),
    )


def _pandas_map_condition_index(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[int], List[Dict[str, Any]]]:
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )

        ###
        # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
        # currently handle filter_column_isnull differently than other map_fn / map_condition
        # cases.
        ###
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[
                domain_records_df[column_name].notnull()
            ]
        domain_column_name_list.append(column_name)

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
        domain_column_name_list = column_list

    # column pair expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
        domain_column_name_list = column_list

    result_format = metric_value_kwargs["result_format"]
    domain_records_df = domain_records_df[boolean_mapped_unexpected_values]

    unexpected_index_list: Union[
        List[int], List[Dict[str, Any]]
    ] = compute_unexpected_pandas_indices(
        domain_records_df=domain_records_df,
        result_format=result_format,
        execution_engine=execution_engine,
        metrics=metrics,
        expectation_domain_column_list=domain_column_name_list,
    )
    if result_format["result_format"] == "COMPLETE":
        return unexpected_index_list
    return unexpected_index_list[: result_format["partial_unexpected_count"]]


def _pandas_map_condition_query(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[List[Any]]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation. For Pandas, this is currently the full set of unexpected_indices.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """
    result_format: dict = metric_value_kwargs["result_format"]

    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return

    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    domain_records_df: pd.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            domain_records_df = domain_records_df[
                domain_records_df[column_name].notnull()
            ]

    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )
    domain_values_df_filtered = domain_records_df[boolean_mapped_unexpected_values]
    return domain_values_df_filtered.index.to_list()


def _pandas_column_map_condition_value_counts(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns respective value counts for distinct column values"""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_pandas_column_map_condition_value_counts).
"""
        )

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    ###
    # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
    # currently handle filter_column_isnull differently than other map_fn / map_condition
    # cases.
    ###
    filter_column_isnull = kwargs.get(
        "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
    )
    if filter_column_isnull:
        df = df[df[column_name].notnull()]

    domain_values = df[column_name]

    result_format = metric_value_kwargs["result_format"]
    value_counts = None
    try:
        value_counts = domain_values[boolean_mapped_unexpected_values].value_counts()
    except ValueError:
        try:
            value_counts = (
                domain_values[boolean_mapped_unexpected_values]
                .apply(tuple)
                .value_counts()
            )
        except ValueError:
            pass

    if not value_counts:
        raise gx_exceptions.MetricComputationError("Unable to compute value counts")

    if result_format["result_format"] == "COMPLETE":
        return value_counts

    return value_counts[result_format["partial_unexpected_count"]]


def _pandas_map_condition_rows(
    cls,
    execution_engine: PandasExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain (ignoring the column constraint) that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
        )

        ###
        # NOTE: 20201111 - JPC - in the map_series / map_condition_series world (pandas), we
        # currently handle filter_column_isnull differently than other map_fn / map_condition
        # cases.
        ###
        filter_column_isnull = kwargs.get(
            "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
        )
        if filter_column_isnull:
            df = df[df[column_name].notnull()]

    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        verify_column_names_exist(
            column_names=column_list, batch_columns_list=metrics["table.columns"]
        )

    result_format = metric_value_kwargs["result_format"]

    df = df[boolean_mapped_unexpected_values]

    if result_format["result_format"] == "COMPLETE":
        return df

    return df.iloc[: result_format["partial_unexpected_count"]]


def _sqlalchemy_map_condition_unexpected_count_aggregate_fn(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations"""
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )

    return (
        sa.func.sum(
            sa.case(
                [(unexpected_condition, 1)],
                else_=0,
            )
        ),
        compute_domain_kwargs,
        accessor_domain_kwargs,
    )


def _sqlalchemy_map_condition_unexpected_count_value(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Returns unexpected count for MapExpectations. This is a *value* metric, which is useful for
    when the unexpected_condition is a window function.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # The integral values are cast to SQL Numeric in order to avoid a bug in AWS Redshift (converted to integer later).
    count_case_statement: List[Label] = sa.case(
        [
            (
                unexpected_condition,
                sa.sql.expression.cast(1, sa.Numeric),
            )
        ],
        else_=sa.sql.expression.cast(0, sa.Numeric),
    ).label("condition")

    count_selectable: Select = sa.select([count_case_statement])
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        count_selectable = count_selectable.select_from(selectable)

    try:
        if execution_engine.engine.dialect.name.lower() == GXSqlDialect.MSSQL:
            temp_table_name: str = generate_temporary_table_name(
                default_table_name_prefix="#ge_temp_"
            )

            with execution_engine.engine.begin():
                metadata: sa.MetaData = sa.MetaData(execution_engine.engine)
                temp_table_obj: sa.Table = sa.Table(
                    temp_table_name,
                    metadata,
                    sa.Column(
                        "condition", sa.Integer, primary_key=False, nullable=False
                    ),
                )
                temp_table_obj.create(execution_engine.engine, checkfirst=True)

                inner_case_query: Insert = temp_table_obj.insert().from_select(
                    [count_case_statement],
                    count_selectable,
                )
                execution_engine.engine.execute(inner_case_query)

                count_selectable = temp_table_obj

        count_selectable = get_sqlalchemy_selectable(count_selectable)
        unexpected_count_query: Select = (
            sa.select(
                [
                    sa.func.sum(sa.column("condition")).label("unexpected_count"),
                ]
            )
            .select_from(count_selectable)
            .alias("UnexpectedCountSubquery")
        )

        unexpected_count: Union[float, int] = execution_engine.engine.execute(
            sa.select(
                [
                    unexpected_count_query.c[
                        f"{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
                    ],
                ]
            )
        ).scalar()
        # Unexpected count can be None if the table is empty, in which case the count
        # should default to zero.
        try:
            unexpected_count = int(unexpected_count)
        except TypeError:
            unexpected_count = 0

    except OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {str(oe)}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=exception_message
        )

    return convert_to_json_serializable(unexpected_count)


def _sqlalchemy_column_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Particularly for the purpose of finding unexpected values, returns all the metric values which do not meet an
    expected Expectation condition for ColumnMapExpectation Expectations.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    query = sa.select([sa.column(column_name).label("unexpected_values")]).where(
        unexpected_condition
    )
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    elif (
        result_format["result_format"] == "COMPLETE"
        and execution_engine.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
    ):
        logger.warning(
            "BigQuery imposes a limit of 10000 parameters on individual queries; "
            "if your data contains more than 10000 columns your results will be truncated."
        )
        query = query.limit(10000)  # BigQuery upper bound on query parameters

    return [
        val.unexpected_values
        for val in execution_engine.engine.execute(query).fetchall()
    ]


def _sqlalchemy_column_pair_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [
        column_A_name,
        column_B_name,
    ]
    # noinspection PyPep8Naming
    column_A_name, column_B_name = get_dbms_compatible_column_names(
        column_names=column_names,
        batch_columns_list=metrics["table.columns"],
    )

    query = sa.select(
        [
            sa.column(column_A_name).label("unexpected_values_A"),
            sa.column(column_B_name).label("unexpected_values_B"),
        ]
    ).where(boolean_mapped_unexpected_values)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])

    unexpected_list = [
        (val.unexpected_values_A, val.unexpected_values_B)
        for val in execution_engine.engine.execute(query).fetchall()
    ]
    return unexpected_list


def _sqlalchemy_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(selectable)
    ).scalar()


def _sqlalchemy_multicolumn_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        boolean_mapped_unexpected_values,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    column_selector = [sa.column(column_name) for column_name in column_list]
    query = sa.select(column_selector).where(boolean_mapped_unexpected_values)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])

    return [dict(val) for val in execution_engine.engine.execute(query).fetchall()]


def _sqlalchemy_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_sqlalchemy_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    selectable = get_sqlalchemy_selectable(selectable)

    return execution_engine.engine.execute(
        sa.select([sa.func.count()]).select_from(selectable)
    ).scalar()


def _sqlalchemy_column_map_condition_value_counts(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Returns value counts for all the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    selectable = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_sqlalchemy_column_map_condition_value_counts).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    column: sa.Column = sa.column(column_name)

    query = (
        sa.select([column, sa.func.count(column)])
        .where(unexpected_condition)
        .group_by(column)
    )
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        query = query.select_from(selectable)

    return execution_engine.engine.execute(query).fetchall()


def _sqlalchemy_map_condition_rows(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """
    Returns all rows of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.
    """
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    selectable = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    table_columns = metrics.get("table.columns")
    column_selector = [sa.column(column_name) for column_name in table_columns]
    query = sa.select(column_selector).where(unexpected_condition)
    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        selectable = get_sqlalchemy_selectable(selectable)
        query = query.select_from(selectable)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] != "COMPLETE":
        query = query.limit(result_format["partial_unexpected_count"])
    try:
        return execution_engine.engine.execute(query).fetchall()
    except OperationalError as oe:
        exception_message: str = f"An SQL execution Exception occurred: {str(oe)}."
        raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
            message=exception_message
        )


def _sqlalchemy_map_condition_query(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Optional[str]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return, along with column the Expectation is run on.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")

    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    column_selector: List[sa.Column] = []

    all_table_columns: List[str] = metrics.get("table.columns")
    unexpected_index_column_names: List[str] = result_format.get(
        "unexpected_index_column_names"
    )
    if unexpected_index_column_names:
        for column_name in unexpected_index_column_names:
            if column_name not in all_table_columns:
                raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                    message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '
                    f"Please check your configuration and try again."
                )

            column_selector.append(sa.column(column_name))

    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))

    unexpected_condition_query_with_selected_columns: sa.select = sa.select(
        column_selector
    ).where(unexpected_condition)
    source_table_and_schema: sa.Table = get_sqlalchemy_source_table_and_schema(
        execution_engine
    )

    source_table_and_schema_as_selectable: Union[
        sa.Table, sa.Select
    ] = get_sqlalchemy_selectable(source_table_and_schema)
    final_select_statement: sa.select = (
        unexpected_condition_query_with_selected_columns.select_from(
            source_table_and_schema_as_selectable
        )
    )

    query_as_string: str = sql_statement_with_post_compile_to_string(
        engine=execution_engine, select_statement=final_select_statement
    )
    return query_as_string


def _sqlalchemy_map_condition_index(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> list[dict[str, Any]] | None:
    """
    Returns indices of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition")

    result_format = metric_value_kwargs["result_format"]
    if "unexpected_index_column_names" not in result_format:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)
    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    domain_kwargs: dict = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    all_table_columns: List[str] = metrics.get("table.columns")

    unexpected_index_column_names: Optional[List[str]] = result_format.get(
        "unexpected_index_column_names"
    )

    column_selector: List[sa.Column] = []
    for column_name in unexpected_index_column_names:
        if column_name not in all_table_columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                message=f'Error: The unexpected_index_column: "{column_name}" in does not exist in SQL Table. '
                f"Please check your configuration and try again."
            )
        column_selector.append(sa.column(column_name))

    # the last column we SELECT is the column the Expectation is being run on
    for column_name in domain_column_name_list:
        column_selector.append(sa.column(column_name))

    domain_records_as_selectable: sa.sql.Selectable = (
        execution_engine.get_domain_records(domain_kwargs=domain_kwargs)
    )
    unexpected_condition_query_with_selected_columns: sa.select = sa.select(
        column_selector
    ).where(unexpected_condition)

    if not MapMetricProvider.is_sqlalchemy_metric_selectable(map_metric_provider=cls):
        domain_records_as_selectable: Union[
            sa.Table, sa.Select
        ] = get_sqlalchemy_selectable(domain_records_as_selectable)

    # since SQL tables can be **very** large, truncate query_result values at 20, or at `partial_unexpected_count`
    final_query: sa.select = (
        unexpected_condition_query_with_selected_columns.select_from(
            domain_records_as_selectable
        ).limit(result_format["partial_unexpected_count"])
    )
    query_result: List[tuple] = execution_engine.engine.execute(final_query).fetchall()

    unexpected_index_list: Optional[List[Dict[str, Any]]] = []

    for row in query_result:
        primary_key_dict: Dict[str, Any] = {}
        # add the actual unexpected value
        all_columns = unexpected_index_column_names + domain_column_name_list
        for index in range(len(all_columns)):
            name: str = all_columns[index]
            primary_key_dict[name] = row[index]
        unexpected_index_list.append(primary_key_dict)

    return unexpected_index_list


def _spark_map_condition_unexpected_count_aggregate_fn(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    return (
        F.sum(F.when(unexpected_condition, 1).otherwise(0)),
        compute_domain_kwargs,
        accessor_domain_kwargs,
    )


def _spark_map_condition_unexpected_count_value(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    # fn_domain_kwargs maybe updated to reflect null filtering
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    return filtered.count()


def _spark_column_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    df = execution_engine.get_domain_records(domain_kwargs=compute_domain_kwargs)

    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_spark_column_map_condition_values).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]

    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        rows = filtered.select(
            F.col(column_name).alias(column_name)
        ).collect()  # note that without the explicit alias, spark will use only the final portion of a nested column as the column name
    else:
        rows = (
            filtered.select(
                F.col(column_name).alias(column_name)
            )  # note that without the explicit alias, spark will use only the final portion of a nested column as the column name
            .limit(result_format["partial_unexpected_count"])
            .collect()
        )
    return [row[column_name] for row in rows]


def _spark_column_map_condition_value_counts(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    if "column" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column" found in provided metric_domain_kwargs, but it is required for a column map metric
(_spark_column_map_condition_value_counts).
"""
        )

    column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
    column_name = get_dbms_compatible_column_names(
        column_names=column_name,
        batch_columns_list=metrics["table.columns"],
    )

    df: pyspark.sql.dataframe.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=compute_domain_kwargs
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]

    value_counts = filtered.groupBy(F.col(column_name).alias(column_name)).count()
    if result_format["result_format"] == "COMPLETE":
        rows = value_counts.collect()
    else:
        rows = value_counts.collect()[: result_format["partial_unexpected_count"]]
    return rows


def _spark_map_condition_rows(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    unexpected_condition, compute_domain_kwargs, accessor_domain_kwargs = metrics.get(
        "unexpected_condition"
    )
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]

    if result_format["result_format"] == "COMPLETE":
        return filtered.collect()

    return filtered.limit(result_format["partial_unexpected_count"]).collect()


def _spark_map_condition_index(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[List[Dict[str, Any]], None]:
    """
    Returns indices of the metric values which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Requires `unexpected_index_column_names` to be part of `result_format` dict to specify primary_key columns
    to return.
    """
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics.get("unexpected_condition", (None, None, None))

    if unexpected_condition is None:
        return None

    result_format = metric_value_kwargs["result_format"]
    if "unexpected_index_column_names" not in result_format:
        return None

    domain_column_name_list: List[str] = list()
    # column map expectations
    if "column" in accessor_domain_kwargs:
        column_name: Union[str, quoted_name] = accessor_domain_kwargs["column"]
        domain_column_name_list.append(column_name)

    # multi-column map expectations
    elif "column_list" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs[
            "column_list"
        ]
        domain_column_name_list = column_list
    # column-map expectations
    elif "column_A" in accessor_domain_kwargs and "column_B" in accessor_domain_kwargs:
        column_list: List[Union[str, quoted_name]] = list()
        column_list.append(accessor_domain_kwargs["column_A"])
        column_list.append(accessor_domain_kwargs["column_B"])
        domain_column_name_list = column_list

    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df: pyspark.sql.dataframe.DataFrame = execution_engine.get_domain_records(
        domain_kwargs=domain_kwargs
    )
    result_format = metric_value_kwargs["result_format"]
    if not result_format.get("unexpected_index_column_names"):
        raise gx_exceptions.MetricResolutionError(
            message="unexpected_indices cannot be returned without 'unexpected_index_column_names'. Please check your configuration.",
            failed_metrics=["unexpected_index_list"],
        )
    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )
    unexpected_index_list: Optional[List[Dict[str, Any]]] = []

    unexpected_index_column_names: List[str] = result_format[
        "unexpected_index_column_names"
    ]
    columns_to_keep: List[str] = [column for column in unexpected_index_column_names]
    columns_to_keep += domain_column_name_list

    # check that column name is in row
    for col_name in columns_to_keep:
        if col_name not in filtered.columns:
            raise gx_exceptions.InvalidMetricAccessorDomainKwargsKeyError(
                f"Error: The unexpected_index_column '{col_name}' does not exist in Spark DataFrame. Please check your configuration and try again."
            )

    if result_format["result_format"] != "COMPLETE":
        filtered.limit(result_format["partial_unexpected_count"])

    for row in filtered.collect():
        dict_to_add: dict = {}
        for col_name in columns_to_keep:
            dict_to_add[col_name] = row[col_name]
        unexpected_index_list.append(dict_to_add)

    return unexpected_index_list


def _spark_map_condition_query(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: Dict,
    metric_value_kwargs: Dict,
    metrics: Dict[str, Any],
    **kwargs,
) -> Union[str, None]:
    """
    Returns query that will return all rows which do not meet an expected Expectation condition for instances
    of ColumnMapExpectation.

    Converts unexpected_condition into a string that can be rendered in DataDocs

    Output will look like:

        df.filter(F.expr( [unexpected_condition] ))

    """
    result_format: dict = metric_value_kwargs["result_format"]
    # We will not return map_condition_query if return_unexpected_index_query = False
    return_unexpected_index_query: bool = result_format.get(
        "return_unexpected_index_query"
    )
    if return_unexpected_index_query is False:
        return None

    (
        unexpected_condition,
        _,
        _,
    ) = metrics.get("unexpected_condition", (None, None, None))

    # unexpected_condition is an F.column object, meaning the str representation is wrapped in Column<> syntax.
    # like Column<'[unexpected_expression]'>
    unexpected_condition_as_string: str = str(unexpected_condition)
    unexpected_condition_filtered: str = unexpected_condition_as_string.replace(
        "Column<'(", ""
    ).replace(")'>", "")
    return f"df.filter(F.expr({unexpected_condition_filtered}))"


def _spark_column_pair_map_condition_values(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [
        column_A_name,
        column_B_name,
    ]
    # noinspection PyPep8Naming
    column_A_name, column_B_name = get_dbms_compatible_column_names(
        column_names=column_names,
        batch_columns_list=metrics["table.columns"],
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        rows = filtered.select(
            [
                F.col(column_A_name).alias(column_A_name),
                F.col(column_B_name).alias(column_B_name),
            ]
        ).collect()
    else:
        rows = (
            filtered.select(
                [
                    F.col(column_A_name).alias(column_A_name),
                    F.col(column_B_name).alias(column_B_name),
                ]
            )
            .limit(result_format["partial_unexpected_count"])
            .collect()
        )

    unexpected_list = [(row[column_A_name], row[column_B_name]) for row in rows]
    return unexpected_list


def _spark_column_pair_map_condition_filtered_row_count(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    # noinspection PyPep8Naming
    column_A_name = accessor_domain_kwargs["column_A"]
    # noinspection PyPep8Naming
    column_B_name = accessor_domain_kwargs["column_B"]

    column_names: List[Union[str, quoted_name]] = [column_A_name, column_B_name]
    verify_column_names_exist(
        column_names=column_names, batch_columns_list=metrics["table.columns"]
    )

    return df.count()


def _spark_multicolumn_map_condition_values(
    cls,
    execution_engine: SqlAlchemyExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return values from the specified domain that match the map-style metric in the metrics dictionary."""
    (
        unexpected_condition,
        compute_domain_kwargs,
        accessor_domain_kwargs,
    ) = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_values).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]

    column_list = get_dbms_compatible_column_names(
        column_names=column_list,
        batch_columns_list=metrics["table.columns"],
    )

    # withColumn is required to transform window functions returned by some metrics to boolean mask
    data = df.withColumn("__unexpected", unexpected_condition)
    filtered = data.filter(F.col("__unexpected") == True).drop(  # noqa: E712
        F.col("__unexpected")
    )

    column_selector = [
        F.col(column_name).alias(column_name) for column_name in column_list
    ]

    domain_values = filtered.select(column_selector)

    result_format = metric_value_kwargs["result_format"]
    if result_format["result_format"] == "COMPLETE":
        domain_values = (
            domain_values.select(column_selector).toPandas().to_dict("records")
        )
    else:
        domain_values = (
            domain_values.select(column_selector)
            .limit(result_format["partial_unexpected_count"])
            .toPandas()
            .to_dict("records")
        )

    return domain_values


def _spark_multicolumn_map_condition_filtered_row_count(
    cls,
    execution_engine: SparkDFExecutionEngine,
    metric_domain_kwargs: dict,
    metric_value_kwargs: dict,
    metrics: Dict[str, Any],
    **kwargs,
):
    """Return record counts from the specified domain that match the map-style metric in the metrics dictionary."""
    _, compute_domain_kwargs, accessor_domain_kwargs = metrics["unexpected_condition"]
    """
    In order to invoke the "ignore_row_if" filtering logic, "execution_engine.get_domain_records()" must be supplied
    with all of the available "domain_kwargs" keys.
    """
    domain_kwargs = dict(**compute_domain_kwargs, **accessor_domain_kwargs)
    df = execution_engine.get_domain_records(domain_kwargs=domain_kwargs)

    if "column_list" not in accessor_domain_kwargs:
        raise ValueError(
            """No "column_list" found in provided metric_domain_kwargs, but it is required for a multicolumn map metric
(_spark_multicolumn_map_condition_filtered_row_count).
"""
        )

    column_list: List[Union[str, quoted_name]] = accessor_domain_kwargs["column_list"]
    verify_column_names_exist(
        column_names=column_list, batch_columns_list=metrics["table.columns"]
    )

    return df.count()


@public_api
class MapMetricProvider(MetricProvider):
    """The base class for defining metrics that are evaluated for every row. An example of a map metric is
    `column_values.null` (which is implemented as a `ColumnMapMetricProvider`, a subclass of `MapMetricProvider`).
    """

    condition_domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    function_domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()
    filter_column_isnull = True

    SQLALCHEMY_SELECTABLE_METRICS: Set[str] = {
        "compound_columns.count",
        "compound_columns.unique",
    }

    @classmethod
    def _register_metric_functions(cls):  # noqa: C901 - 28
        if not (
            hasattr(cls, "function_metric_name")
            or hasattr(cls, "condition_metric_name")
        ):
            return

        for attr, candidate_metric_fn in inspect.getmembers(cls):
            if not hasattr(candidate_metric_fn, "metric_engine"):
                # This is not a metric.
                continue

            metric_fn_type = getattr(candidate_metric_fn, "metric_fn_type")
            if not metric_fn_type:
                # This is not a metric (valid metrics possess exectly one metric function).
                return

            engine = candidate_metric_fn.metric_engine
            if not issubclass(engine, ExecutionEngine):
                raise ValueError(
                    "Metric functions must be defined with an ExecutionEngine as part of registration."
                )

            if metric_fn_type in [
                MetricPartialFunctionTypes.MAP_CONDITION_FN,
                MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
                MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
            ]:
                if not hasattr(cls, "condition_metric_name"):
                    raise ValueError(
                        """A "MapMetricProvider" must have a "condition_metric_name" to have a decorated \
"column_condition_partial" method."""
                    )

                condition_provider = candidate_metric_fn
                # noinspection PyUnresolvedReferences
                metric_name = cls.condition_metric_name
                metric_domain_keys = cls.condition_domain_keys
                metric_value_keys = cls.condition_value_keys
                metric_definition_kwargs = getattr(
                    condition_provider, "metric_definition_kwargs", {}
                )
                domain_type = getattr(
                    condition_provider,
                    "domain_type",
                    metric_definition_kwargs.get(
                        "domain_type", MetricDomainTypes.TABLE
                    ),
                )
                if issubclass(engine, PandasExecutionEngine):
                    register_metric(
                        metric_name=f"{metric_name}.{metric_fn_type.metric_suffix}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        metric_fn_type=metric_fn_type,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_map_condition_unexpected_count,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_map_condition_index,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_map_condition_query,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_pandas_map_condition_rows,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    if domain_type == MetricDomainTypes.COLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_column_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_column_map_condition_value_counts,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.COLUMN_PAIR:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_column_pair_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_column_pair_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.MULTICOLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_multicolumn_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_pandas_multicolumn_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                elif issubclass(engine, SqlAlchemyExecutionEngine):
                    register_metric(
                        metric_name=f"{metric_name}.{metric_fn_type.metric_suffix}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        metric_fn_type=metric_fn_type,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_map_condition_rows,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_map_condition_index,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_sqlalchemy_map_condition_query,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    if metric_fn_type == MetricPartialFunctionTypes.MAP_CONDITION_FN:
                        # Documentation in "MetricProvider._register_metric_functions()" explains registration protocol.
                        if domain_type == MetricDomainTypes.COLUMN:
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=_sqlalchemy_map_condition_unexpected_count_aggregate_fn,
                                metric_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN,
                            )
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=None,
                                metric_fn_type=MetricFunctionTypes.VALUE,
                            )
                        else:
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=_sqlalchemy_map_condition_unexpected_count_value,
                                metric_fn_type=MetricFunctionTypes.VALUE,
                            )
                    elif (
                        metric_fn_type == MetricPartialFunctionTypes.WINDOW_CONDITION_FN
                    ):
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=metric_value_keys,
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_map_condition_unexpected_count_value,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    if domain_type == MetricDomainTypes.COLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_column_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_column_map_condition_value_counts,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.COLUMN_PAIR:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_column_pair_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_column_pair_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.MULTICOLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_multicolumn_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_sqlalchemy_multicolumn_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                elif issubclass(engine, SparkDFExecutionEngine):
                    register_metric(
                        metric_name=f"{metric_name}.{metric_fn_type.metric_suffix}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=condition_provider,
                        metric_fn_type=metric_fn_type,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_map_condition_rows,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_map_condition_index,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    register_metric(
                        metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=(*metric_value_keys, "result_format"),
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=_spark_map_condition_query,
                        metric_fn_type=MetricFunctionTypes.VALUE,
                    )
                    if metric_fn_type == MetricPartialFunctionTypes.MAP_CONDITION_FN:
                        # Documentation in "MetricProvider._register_metric_functions()" explains registration protocol.
                        if domain_type == MetricDomainTypes.COLUMN:
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=_spark_map_condition_unexpected_count_aggregate_fn,
                                metric_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN,
                            )
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=None,
                                metric_fn_type=MetricFunctionTypes.VALUE,
                            )
                        else:
                            register_metric(
                                metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                                metric_domain_keys=metric_domain_keys,
                                metric_value_keys=metric_value_keys,
                                execution_engine=engine,
                                metric_class=cls,
                                metric_provider=_spark_map_condition_unexpected_count_value,
                                metric_fn_type=MetricFunctionTypes.VALUE,
                            )
                    elif (
                        metric_fn_type == MetricPartialFunctionTypes.WINDOW_CONDITION_FN
                    ):
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=metric_value_keys,
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_map_condition_unexpected_count_value,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    if domain_type == MetricDomainTypes.COLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_column_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_column_map_condition_value_counts,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.COLUMN_PAIR:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_column_pair_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_column_pair_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                    elif domain_type == MetricDomainTypes.MULTICOLUMN:
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_multicolumn_map_condition_values,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
                        register_metric(
                            metric_name=f"{metric_name}.{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                            metric_domain_keys=metric_domain_keys,
                            metric_value_keys=(*metric_value_keys, "result_format"),
                            execution_engine=engine,
                            metric_class=cls,
                            metric_provider=_spark_multicolumn_map_condition_filtered_row_count,
                            metric_fn_type=MetricFunctionTypes.VALUE,
                        )
            elif metric_fn_type in [
                MetricPartialFunctionTypes.MAP_FN,
                MetricPartialFunctionTypes.MAP_SERIES,
                MetricPartialFunctionTypes.WINDOW_FN,
            ]:
                if not hasattr(cls, "function_metric_name"):
                    raise ValueError(
                        """A "MapMetricProvider" must have a "function_metric_name" to have a decorated \
"column_function_partial" method."""
                    )

                map_function_provider = candidate_metric_fn
                # noinspection PyUnresolvedReferences
                metric_name = cls.function_metric_name
                metric_domain_keys = cls.function_domain_keys
                metric_value_keys = cls.function_value_keys
                register_metric(
                    metric_name=f"{metric_name}.{metric_fn_type.metric_suffix}",
                    metric_domain_keys=metric_domain_keys,
                    metric_value_keys=metric_value_keys,
                    execution_engine=engine,
                    metric_class=cls,
                    metric_provider=map_function_provider,
                    metric_fn_type=metric_fn_type,
                )

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: Dict[str, MetricConfiguration] = {}

        base_metric_value_kwargs = {
            k: v for k, v in metric.metric_value_kwargs.items() if k != "result_format"
        }

        metric_name: str = metric.metric_name

        metric_suffix: str = (
            f".{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )

        # Documentation in "MetricProvider._register_metric_functions()" explains registration/dependency protocol.
        if metric_name.endswith(metric_suffix):
            has_aggregate_fn: bool = False

            if execution_engine is not None:
                try:
                    _ = get_metric_provider(
                        f"{metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                        execution_engine,
                    )
                    has_aggregate_fn = True
                except gx_exceptions.MetricProviderError:
                    pass

            if has_aggregate_fn:
                dependencies["metric_partial_fn"] = MetricConfiguration(
                    metric_name=f"{metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs=base_metric_value_kwargs,
                )
            else:
                dependencies["unexpected_condition"] = MetricConfiguration(
                    metric_name=f"{metric_name[:-len(metric_suffix)]}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs=base_metric_value_kwargs,
                )

        # MapMetric uses "condition" metric to build "unexpected_count.aggregate_fn" and other listed metrics as well.
        unexpected_condition_dependent_metric_name_suffixes: List[str] = list(
            filter(
                lambda element: metric_name.endswith(element),
                [
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_VALUE_COUNTS.value}",
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}",
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}",
                    f".{SummarizationMetricNameSuffixes.FILTERED_ROW_COUNT.value}",
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}",
                    f".{SummarizationMetricNameSuffixes.UNEXPECTED_ROWS.value}",
                ],
            )
        )
        if len(unexpected_condition_dependent_metric_name_suffixes) == 1:
            metric_suffix = unexpected_condition_dependent_metric_name_suffixes[0]
            if metric_name.endswith(metric_suffix):
                dependencies["unexpected_condition"] = MetricConfiguration(
                    metric_name=f"{metric_name[:-len(metric_suffix)]}.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                    metric_value_kwargs=base_metric_value_kwargs,
                )

        return dependencies

    @staticmethod
    def is_sqlalchemy_metric_selectable(
        map_metric_provider: MetaMetricProvider,
    ) -> bool:
        """
        :param map_metric_provider: object of type "MapMetricProvider", whose SQLAlchemy implementation is inspected
        :return: boolean indicating whether or not the returned value of a method implementing the metric resolves all
        columns -- hence the caller must not use "select_from" clause as part of its own SQLAlchemy query; otherwise an
        unwanted selectable (e.g., table) will be added to "FROM", leading to duplicated and/or erroneous results.
        """
        # noinspection PyUnresolvedReferences
        return (
            hasattr(map_metric_provider, "condition_metric_name")
            and map_metric_provider.condition_metric_name
            in MapMetricProvider.SQLALCHEMY_SELECTABLE_METRICS
        ) or (
            hasattr(map_metric_provider, "function_metric_name")
            and map_metric_provider.function_metric_name
            in MapMetricProvider.SQLALCHEMY_SELECTABLE_METRICS
        )


@public_api
class ColumnMapMetricProvider(MapMetricProvider):
    """Defines metrics that are evaluated for every row for a single column. An example of a column map
    metric is `column_values.null` (which is implemented as a `ColumnMapMetricProvider`, a subclass of
    `MapMetricProvider`).

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
    """

    condition_domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    function_domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies


@public_api
class ColumnPairMapMetricProvider(MapMetricProvider):
    """Defines metrics that are evaluated for every row for a column pair. All column pair metrics require domain
    keys of `column_A` and `column_B`.

    `expect_column_pair_values_to_be_equal` is an example of an Expectation that uses this metric.
    """

    condition_domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    function_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: dict = {
            k: v
            for k, v in metric.metric_domain_kwargs.items()
            if k not in ["column_A", "column_B", "ignore_row_if"]
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies


@public_api
class MulticolumnMapMetricProvider(MapMetricProvider):
    """Defines metrics that are evaluated for every row for a set of columns. All multi-column metrics require the
    domain key `column_list`.

    `expect_compound_columns_to_be_unique` is an example of an Expectation that uses this metric.
    """

    condition_domain_keys: Tuple[str, ...] = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    function_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = tuple()
    function_value_keys = tuple()

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: dict = {
            k: v
            for k, v in metric.metric_domain_kwargs.items()
            if k not in ["column_list", "ignore_row_if"]
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies
