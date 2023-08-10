from typing import Optional

from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


class DataProfilerTableColumnInfos(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.table_column_infos"

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
        profile_report: dict = metrics["data_profiler.profile_report"]
        profile_report_column_data_stats: dict = {
            element["column_name"]: element for element in profile_report["data_stats"]
        }
        return profile_report_column_data_stats

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
        dependencies["data_profiler.profile_report"] = MetricConfiguration(
            metric_name="data_profiler.profile_report",
            metric_domain_kwargs={},
            metric_value_kwargs=metric.metric_value_kwargs,
        )
        return dependencies
