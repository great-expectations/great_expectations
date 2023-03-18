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