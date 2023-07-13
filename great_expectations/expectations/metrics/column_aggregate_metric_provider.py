import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core import ExpectationConfiguration
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sparkdf_execution_engine import (
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics import DeprecatedMetaMetricProvider
from great_expectations.expectations.metrics.metric_provider import (
    metric_partial,
    metric_value,
)
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy


@public_api
def column_aggregate_value(
    engine: Type[ExecutionEngine],
    **kwargs,
):
    """Provides Pandas support for authoring a metric_fn with a simplified signature.

    A column_aggregate_value must provide an aggregate function; it will be executed by Pandas
    to provide a value for validation.

    A metric function that is decorated as a column_aggregate_partial will be called with a specified Pandas column
    and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine: The `ExecutionEngine` used to to evaluate the condition
        **kwargs: Arguments passed to specified function

    Returns:
        An annotated metric_function which will be called with a simplified signature.
    """
    domain_type: MetricDomainTypes = MetricDomainTypes.COLUMN
    if issubclass(engine, PandasExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric_value(engine=PandasExecutionEngine)
            @wraps(metric_fn)
            def inner_func(  # noqa: PLR0913
                cls,
                execution_engine: PandasExecutionEngine,
                metric_domain_kwargs: dict,
                metric_value_kwargs: dict,
                metrics: Dict[str, Any],
                runtime_configuration: dict,
            ):
                filter_column_isnull = kwargs.get(
                    "filter_column_isnull", getattr(cls, "filter_column_isnull", False)
                )

                df, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
                    domain_kwargs=metric_domain_kwargs, domain_type=domain_type
                )

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                if filter_column_isnull:
                    df = df[df[column_name].notnull()]

                return metric_fn(
                    cls,
                    column=df[column_name],
                    **metric_value_kwargs,
                    _metrics=metrics,
                )

            return inner_func

        return wrapper
    else:
        raise ValueError(
            "column_aggregate_value decorator only supports PandasExecutionEngine"
        )


@public_api
def column_aggregate_partial(engine: Type[ExecutionEngine], **kwargs):
    """Provides engine-specific support for authoring a metric_fn with a simplified signature.

    A column_aggregate_partial must provide an aggregate function; it will be executed with the specified engine
    to provide a value for validation.

    A metric function that is decorated as a column_aggregate_partial will be called with the engine-specific column
    type and any value_kwargs associated with the Metric for which the provider function is being declared.

    Args:
        engine: The `ExecutionEngine` used to to evaluate the condition
        partial_fn_type: The metric function type
        domain_type: The domain over which the metric will operate
        **kwargs: Arguments passed to specified function

    Returns:
        An annotated metric_function which will be called with a simplified signature.
    """
    partial_fn_type: MetricPartialFunctionTypes = (
        MetricPartialFunctionTypes.AGGREGATE_FN
    )
    domain_type: MetricDomainTypes = MetricDomainTypes.COLUMN
    if issubclass(engine, SqlAlchemyExecutionEngine):

        def wrapper(metric_fn: Callable):
            @metric_partial(
                engine=SqlAlchemyExecutionEngine,
                partial_fn_type=partial_fn_type,
                domain_type=domain_type,
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

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                sqlalchemy_engine: sa.engine.Engine = execution_engine.engine

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
            def inner_func(  # noqa: PLR0913
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

                column_name: Union[
                    str, sqlalchemy.quoted_name
                ] = accessor_domain_kwargs["column"]

                column_name = get_dbms_compatible_column_names(
                    column_names=column_name,
                    batch_columns_list=metrics["table.columns"],
                )

                column = data[column_name]
                metric_aggregate = metric_fn(
                    cls,
                    column=column,
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


@public_api
class ColumnAggregateMetricProvider(TableMetricProvider):
    """Base class for all Column Aggregate Metrics,
    which define metrics to be calculated in aggregate from a given column.

    An example of this is `column.mean`,
    which returns the mean of a given column.

    Args:
        metric_name (str): A name identifying the metric. Metric Name must be globally unique in
            a great_expectations installation.
        domain_keys (tuple): A tuple of the keys used to determine the domain of the metric.
        value_keys (tuple): A tuple of the keys used to determine the value of the metric.

    In some cases, subclasses of MetricProvider, such as ColumnAggregateMetricProvider, will already
    have correct values that may simply be inherited by Metric classes.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
    """

    domain_keys = (
        "batch_id",
        "table",
        "column",
        "row_condition",
        "condition_parser",
    )
    filter_column_isnull = False

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


class ColumnMetricProvider(
    ColumnAggregateMetricProvider, metaclass=DeprecatedMetaMetricProvider
):
    _DeprecatedMetaMetricProvider__alias = ColumnAggregateMetricProvider
