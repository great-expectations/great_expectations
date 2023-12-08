from typing import Optional

from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.util import (
    get_dbms_compatible_column_names,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


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
        )

        profile_report_column_data_stats: dict = metrics[
            "data_profiler.table_column_infos"
        ]
        return profile_report_column_data_stats[column_name]

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
        dependencies["data_profiler.table_column_infos"] = MetricConfiguration(
            metric_name="data_profiler.table_column_infos",
            metric_domain_kwargs={},
            metric_value_kwargs=metric.metric_value_kwargs,
        )
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies
