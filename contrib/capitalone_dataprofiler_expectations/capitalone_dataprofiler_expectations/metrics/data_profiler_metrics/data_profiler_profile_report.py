import dataprofiler as dp
from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value


class DataProfilerProfileReport(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.profile_report"

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
        profile_path = metric_value_kwargs["profile_path"]
        try:
            profile: dp.profilers.profile_builder.BaseProfiler = dp.Profiler.load(
                profile_path
            )
            profile_report = profile.report(
                report_options={"output_format": "serializable"}
            )
            profile_report["global_stats"]["profile_schema"] = dict(
                profile_report["global_stats"]["profile_schema"]
            )
            return profile_report
        except FileNotFoundError:
            raise ValueError(
                "'profile_path' does not point to a valid DataProfiler stored profile."
            )
        except Exception as e:
            raise gx_exceptions.MetricError(
                message=str(e),
            ) from e
