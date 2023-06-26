from typing import Optional

from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


class DataProfilerProfileNumericColumns(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.profile_numeric_columns"

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
        profile_report = metrics["data_profiler.profile_report"]

        numeric_columns = []
        for col in profile_report["data_stats"]:
            dtype = col["data_type"]
            if dtype == "int" or dtype == "float":
                numeric_columns.append(col["column_name"])

        return numeric_columns

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """
        Returns a dictionary of given metric names and their corresponding configuration, specifying
        the metric types and their respective domains"""
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if metric.metric_name == "data_profiler.profile_numeric_columns":
            dependencies["data_profiler.profile_report"] = MetricConfiguration(
                metric_name="data_profiler.profile_report",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )
        return dependencies
