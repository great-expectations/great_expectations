from typing import Optional

import dataprofiler as dp

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

from .data_profiler_profile_metric_provider import DataProfilerProfileMetricProvider


class DataProfilerColumnProfileReport(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.column_profile_report"

    value_keys = ("profile_path",)

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        _, _, accessor_domain_kwargs = execution_engine.get_compute_domain(
            domain_kwargs=metric_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]

        column_name = get_dbms_compatible_column_names(
            column_names=column_name,
            batch_columns_list=metrics["table.columns"],
            execution_engine=execution_engine,
        )

        profile_path = metric_value_kwargs["profile_path"]
        try:
            profile: dp.profilers.profile_builder.StructuredProfiler = dp.Profiler.load(
                profile_path
            )
            profile_report: dict = profile.report(
                report_options={"output_format": "serializable"}
            )
            profile_report_column_data_stats: dict = {
                element["column_name"]: element
                for element in profile_report["data_stats"]
            }
            return profile_report_column_data_stats[column_name]
        except FileNotFoundError:
            raise ValueError(
                "'profile_path' does not point to a valid DataProfiler stored profile."
            )
        except Exception as e:
            raise ge_exceptions.MetricError(
                message=str(e),
            ) from e

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
            metric_dependencies=None,
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
            metric_dependencies=None,
        )
        dependencies["table.row_count"] = MetricConfiguration(
            metric_name="table.row_count",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
            metric_dependencies=None,
        )
        return dependencies
