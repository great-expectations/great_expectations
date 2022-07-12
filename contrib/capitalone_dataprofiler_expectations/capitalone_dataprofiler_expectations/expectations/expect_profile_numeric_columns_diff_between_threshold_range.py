import os
from typing import Dict

import dataprofiler as dp
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.metrics.metric_provider import (
    MetricDomainTypes,
    MetricProvider,
    metric_value,
)


class DataProfilerMetricProvider(MetricProvider):
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    value_keys = ("profile_path",)


class DataProfilerProfileDiff(DataProfilerMetricProvider):
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


class DataProfilerProfileReport(DataProfilerMetricProvider):
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
            profile = dp.Profiler.load(profile_path)
            profile_report = profile.report(report_options={"output_format": "pretty"})
            return profile_report
        except FileNotFoundError:
            raise ValueError(
                "'profile_path' does not point to a valid DataProfiler stored profile."
            )


class ExpectProfileNumericColumnsDiffBetweenThresholdRange(TableExpectation):
    """
    This expectation takes the difference report between the data it is called on and a DataProfiler profile of the same schema loaded from a provided path.
    This function builds upon the custom table expectations of Great Expectations.
    Each numerical column will be checked against a user provided dictionary of columns paired with dictionaries of statistics containing lower and upper bounds.
    It is expected that a statistics value for a given column is within the specified threshold.

    Args:
        profile_path (str): A path to a saved DataProfiler profile object on the local filesystem.
        limit_check_report_keys (dict): A dict, containing column names as keys and dicts as values that contain statistics as keys and dicts as values containing two keys:
                                        "lower" denoting the lower bound for the threshold range, and "upper" denoting the upper bound for the threshold range.
    validator.expect_profile_numerical_columns_diff_between_threshold_range(
        profile_path = "C:/path_to/my_profile.pkl",
        limit_check_report_keys = {
            "column_one": {
                "min": {"lower": 2.0, "upper": 10.0},
            },
            "*": {
                "*": {"lower": 0, "upper": 100},
            },
        }
    )
    Note: In limit_check_report_keys, "*" in place of a column denotes a general operator in which the value it stores will be applied to every column in the data that has no explicit key.
          "*" in place of a statistic denotes a general operator in which the bounds it stores will be applied to every statistic for the given column that has no explicit key.
    """

    example_profile_data = [
        [2, 5, "10", "ten", 25],
        [4, 10, "20", "twenty", 50],
        [6, 15, "30", "thirty", 75],
        [8, 20, "40", "forty", 100],
        [10, 25, "50", "fifty", 125],
    ]
    example_profile_columns = [
        "by_2",
        "by_5",
        "str_by_10",
        "words_by_10",
        "by_25",
    ]

    df = pd.DataFrame(example_profile_data, columns=example_profile_columns)
    profiler_opts = dp.ProfilerOptions()
    profiler_opts.structured_options.multiprocess.is_enabled = False

    example_profile = dp.Profiler(df, options=profiler_opts)

    profile_path = (
        "/example_profiles/expect_profile_diff_less_than_threshold_profile.pkl"
    )

    dir_path = os.path.dirname(os.path.abspath(__file__))
    profile_path = dir_path + profile_path

    example_profile.save(filepath=profile_path)

    examples = [
        {
            "data": {
                "by_2": [4, 6, 8, 10, 12],
                "by_5": [10, 15, 20, 25, 30],
                "str_by_10": ["20", "30", "40", "50", "60"],
                "words_by_10": ["twenty", "thirty", "forty", "fifty", "sixty"],
                "by_25": [50, 75, 100, 125, 150],
            },
            "tests": [
                {
                    "title": "profile_min_delta_witin_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "*": {
                                "min": {"lower": 0, "upper": 50},
                            },
                        },
                    },
                    "out": {"success": True},
                },
                {
                    "title": "profile_all_stats_beyond_delta_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "*": {"*": {"lower": 0, "upper": 0}},
                            "by_2": {
                                "min": {"lower": -1, "upper": 1},
                            },
                        },
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    metric_dependencies = (
        "data_profiler.profile_diff",
        "data_profiler.profile_report",
    )

    success_keys = (
        "profile_path",
        "limit_check_report_keys",
    )

    default_limit_check_report_keys = {
        "*": {
            "min": {"lower": 0, "upper": 0},
            "max": {"lower": 0, "upper": 0},
            "sum": {"lower": 0, "upper": 0},
            "mean": {"lower": 0, "upper": 0},
            "median": {"lower": 0, "upper": 0},
            "median_absolute_deviation": {"lower": 0, "upper": 0},
            "variance": {"lower": 0, "upper": 0},
            "stddev": {"lower": 0, "upper": 0},
            "unique_count": {"lower": 0, "upper": 0},
            "unique_ratio": {"lower": 0, "upper": 0},
            "gini_impurity": {"lower": 0, "upper": 0},
            "unalikeability": {"lower": 0, "upper": 0},
            "sample_size": {"lower": 0, "upper": 0},
            "null_count": {"lower": 0, "upper": 0},
        }
    }

    numerical_diff_statistics = list(default_limit_check_report_keys["*"].keys())

    default_kwarg_values = {
        "limit_check_report_keys": default_limit_check_report_keys,
    }

    # def get_validation_dependencies(
    #     self,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     execution_engine: Optional[ExecutionEngine] = None,
    #     runtime_configuration: Optional[dict] = None
    #     ) -> dict:
    #     return super().get_validation_dependencies(configuration, execution_engine, runtime_configuration)

    # def validate_configuration(
    #     self, configuration: Optional[ExpectationConfiguration]
    # ) -> None:

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        profile_diff = metrics.get("data_profiler.profile_diff")
        seed_profile = metrics.get("data_profiler.profile_report")
        limit_check_report_keys = configuration.kwargs.get("limit_check_report_keys")

        numerical_columns = []
        for seed_col in seed_profile["data_stats"]:
            dtype = seed_col["data_type"]
            if dtype == "int" or dtype == "float":
                numerical_columns.append(seed_col["column_name"])

        columns = list(profile_diff["global_stats"]["profile_schema"][1].keys())
        data_stats = profile_diff["data_stats"]

        unexpected = {}
        if "*" in limit_check_report_keys:  # Adds columns for general column operator
            general_stats = limit_check_report_keys.pop("*")
            for column in columns:
                if column not in limit_check_report_keys:
                    limit_check_report_keys[column] = general_stats
        for col, stats in limit_check_report_keys.items():
            if col not in numerical_columns:
                continue
            if "*" in stats:  # Adds statistics for general statistic operator
                general_bounds = stats.pop("*")
                for statistic in self.numerical_diff_statistics:
                    if statistic not in stats:
                        stats[statistic] = general_bounds
            if col not in columns:
                unexpected[col] = "Column DNE"
                continue
            col_data_stats = {}
            for data_stat in data_stats:
                if data_stat["column_name"] == col:
                    col_data_stats = data_stat["statistics"]
                    break
            for stat, bounds in stats.items():
                if stat not in col_data_stats:
                    if col not in unexpected:
                        unexpected[col] = {}
                    unexpected[col][stat] = None
                    continue
                diff_val = col_data_stats[stat]
                if diff_val == "unchanged":
                    diff_val = 0
                lower_bound = bounds["lower"]
                upper_bound = bounds["upper"]
                valid = lower_bound < diff_val and diff_val < upper_bound
                if not valid:
                    if col not in unexpected:
                        unexpected[col] = {}
                    unexpected[col][stat] = {
                        "lower": lower_bound,
                        "upper": upper_bound,
                        "delta": diff_val,
                    }

        success = unexpected == {}
        ret_val = {"success": success, "result": {"unexpected": unexpected}}
        return ret_val

    library_metadata = {
        "requirements": ["dataprofiler", "tensorflow", "scikit-learn", "numpy"],
        "maturity": "experimental",  # "concept_only", "experimental", "beta", or "production"
        "tags": [
            "dataprofiler",
            "dataassistance",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@stevensecreti",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    diagnostics_report = (
        ExpectProfileNumericColumnsDiffBetweenThresholdRange().run_diagnostics()
    )
    print(diagnostics_report.generate_checklist())
