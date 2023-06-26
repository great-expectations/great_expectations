import copy
from typing import Optional

from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


class DataProfilerProfilePercentDiff(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.profile_percent_diff"

    value_keys = ("profile_path",)

    @metric_value(PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        numerical_diff_stats = [
            "min",
            "max",
            "sum",
            "mean",
            "median",
            "median_absolute_deviation",
            "variance",
            "stddev",
            "unique_count",
            "unique_ratio",
            "gini_impurity",
            "unalikeability",
            "sample_size",
            "null_count",
        ]

        profile_report = metrics["data_profiler.profile_report"]
        diff_report = metrics["data_profiler.profile_diff"]

        pr_columns = profile_report["data_stats"]
        dr_columns = diff_report["data_stats"]

        percent_delta_data_stats = []
        for pr_col, dr_col in zip(pr_columns, dr_columns):
            pr_stats = pr_col["statistics"]
            dr_stats = dr_col["statistics"]
            percent_delta_col = copy.deepcopy(dr_col)
            percent_delta_stats = {}
            for dr_stat, dr_val in dr_stats.items():
                if dr_stat not in numerical_diff_stats:
                    percent_delta_stats[dr_stat] = dr_val
                    continue
                if dr_val == "unchanged":
                    dr_val = 0
                if dr_stat not in pr_stats:
                    percent_delta_stats[dr_stat] = "ERR_no_original_value"
                    continue
                pr_val = pr_stats[dr_stat]
                percent_change = 0
                if pr_val == 0:
                    percent_change = "ERR_divide_by_zero"  # Div by 0 error
                else:
                    percent_change = dr_val / pr_val
                percent_delta_stats[dr_stat] = percent_change
            percent_delta_col["statistics"] = percent_delta_stats
            percent_delta_data_stats.append(percent_delta_col)

        percent_diff_report = copy.deepcopy(diff_report)
        percent_diff_report["data_stats"] = percent_delta_data_stats
        return percent_diff_report

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

        if metric.metric_name == "data_profiler.profile_percent_diff":
            dependencies["data_profiler.profile_report"] = MetricConfiguration(
                metric_name="data_profiler.profile_report",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )
            dependencies["data_profiler.profile_diff"] = MetricConfiguration(
                metric_name="data_profiler.profile_diff",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )

        return dependencies
