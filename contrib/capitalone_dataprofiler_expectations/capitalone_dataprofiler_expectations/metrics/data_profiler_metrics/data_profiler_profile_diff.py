import dataprofiler as dp
from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value


class DataProfilerProfileDiff(DataProfilerProfileMetricProvider):
    metric_name = "data_profiler.profile_diff"

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
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        first_profile = None
        try:
            first_profile_path = metric_value_kwargs["profile_path"]
            first_profile = dp.Profiler.load(first_profile_path)
        except FileNotFoundError:
            raise ValueError(
                "'profile_path' does not point to a valid DataProfiler stored profile."
            )

        profiler_opts = dp.ProfilerOptions()
        profiler_opts.structured_options.multiprocess.is_enabled = False
        new_profile = dp.Profiler(df, options=profiler_opts)

        report_diff = new_profile.diff(
            first_profile
        )  # Results in diff of new_prof - first_prof
        # Values in this report indicate +/- change from old profile
        return report_diff
