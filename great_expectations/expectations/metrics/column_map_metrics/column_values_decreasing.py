import datetime
import warnings
from typing import Any, Dict

import pandas as pd
from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.import_manager import F, Window, sparktypes
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_partial


class ColumnValuesDecreasing(ColumnMapMetricProvider):
    condition_metric_name = "column_values.decreasing"
    condition_value_keys = ("strictly", "parse_strings_as_datetimes")
    default_kwarg_values = {"strictly": False, "parse_strings_as_datetimes": False}

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                'The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n',
                DeprecationWarning,
            )
            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column
        else:
            temp_column = column
        series_diff = temp_column.diff()
        if parse_strings_as_datetimes:
            series_diff[series_diff.isnull()] = datetime.timedelta(seconds=(-1))
            series_diff = pd.to_timedelta(series_diff, unit="S")
        else:
            series_diff[series_diff.isnull()] = -1
        strictly: bool = kwargs.get("strictly") or False
        if strictly:
            if parse_strings_as_datetimes:
                return series_diff.dt.total_seconds() < 0.0
            return series_diff < 0
        else:
            if parse_strings_as_datetimes:
                return series_diff.dt.total_seconds() <= 0.0
            return series_diff <= 0

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[(str, Any)],
        runtime_configuration: Dict,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        parse_strings_as_datetimes: bool = (
            metric_value_kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                'The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in v0.16. As part of the V3 API transition, we\'ve moved away from input transformation. For more information, please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/\n',
                DeprecationWarning,
            )
        column_name = metric_domain_kwargs["column"]
        table_columns = metrics["table.column_types"]
        column_metadata = [
            col for col in table_columns if (col["name"] == column_name)
        ][0]
        if isinstance(
            column_metadata["type"],
            (sparktypes.LongType, sparktypes.DoubleType, sparktypes.IntegerType),
        ):
            compute_domain_kwargs = execution_engine.add_column_row_condition(
                metric_domain_kwargs,
                filter_null=cls.filter_column_isnull,
                filter_nan=True,
            )
        else:
            compute_domain_kwargs = metric_domain_kwargs
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            compute_domain_kwargs, MetricDomainTypes.COLUMN
        )
        column = F.col(column_name)
        if isinstance(
            column_metadata["type"], (sparktypes.TimestampType, sparktypes.DateType)
        ):
            diff = F.datediff(
                column, F.lag(column).over(Window.orderBy(F.lit("constant")))
            )
        else:
            diff = column - F.lag(column).over(Window.orderBy(F.lit("constant")))
            diff = F.when(diff.isNull(), (-1)).otherwise(diff)
        if metric_value_kwargs["strictly"]:
            return (
                F.when((diff >= 0), F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
        else:
            return (
                F.when((diff > 0), F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
