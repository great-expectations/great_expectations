from __future__ import annotations

import inspect
import logging
import warnings
from typing import (
    TYPE_CHECKING,
    Dict,
    List,
    Optional,
    Tuple,
)

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
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider.column_map_condition_auxilliary_methods import (
    _pandas_column_map_condition_value_counts,
    _pandas_column_map_condition_values,
    _spark_column_map_condition_value_counts,
    _spark_column_map_condition_values,
    _sqlalchemy_column_map_condition_value_counts,
    _sqlalchemy_column_map_condition_values,
)
from great_expectations.expectations.metrics.map_metric_provider.column_pair_map_condition_auxilliary_methods import (
    _pandas_column_pair_map_condition_filtered_row_count,
    _pandas_column_pair_map_condition_values,
    _spark_column_pair_map_condition_filtered_row_count,
    _spark_column_pair_map_condition_values,
    _sqlalchemy_column_pair_map_condition_filtered_row_count,
    _sqlalchemy_column_pair_map_condition_values,
)
from great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable import (
    _is_sqlalchemy_metric_selectable,
)
from great_expectations.expectations.metrics.map_metric_provider.map_condition_auxilliary_methods import (
    _pandas_map_condition_index,
    _pandas_map_condition_query,
    _pandas_map_condition_rows,
    _pandas_map_condition_unexpected_count,
    _spark_map_condition_index,
    _spark_map_condition_query,
    _spark_map_condition_rows,
    _spark_map_condition_unexpected_count_aggregate_fn,
    _spark_map_condition_unexpected_count_value,
    _sqlalchemy_map_condition_index,
    _sqlalchemy_map_condition_query,
    _sqlalchemy_map_condition_rows,
    _sqlalchemy_map_condition_unexpected_count_aggregate_fn,
    _sqlalchemy_map_condition_unexpected_count_value,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_map_condition_auxilliary_methods import (
    _pandas_multicolumn_map_condition_filtered_row_count,
    _pandas_multicolumn_map_condition_values,
    _spark_multicolumn_map_condition_filtered_row_count,
    _spark_multicolumn_map_condition_values,
    _sqlalchemy_multicolumn_map_condition_filtered_row_count,
    _sqlalchemy_multicolumn_map_condition_values,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
)
from great_expectations.expectations.registry import (
    get_metric_provider,
    register_metric,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.metrics import MetaMetricProvider

logger = logging.getLogger(__name__)


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

    @classmethod
    def _register_metric_functions(cls):  # noqa: C901, PLR0912, PLR0915
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
        # deprecated-v0.16.1
        warnings.warn(
            "MapMetricProvider.is_sqlalchemy_metric_selectable is deprecated."
            "You can use the great_expectations.expectations.metrics.map_metric_provider.is_sqlalchemy_metric_selectable._is_sqlalchemy_metric_selectable function, but please note that it is not considered part of the public API, and could change in the future.",
            DeprecationWarning,
        )

        return _is_sqlalchemy_metric_selectable(map_metric_provider)
