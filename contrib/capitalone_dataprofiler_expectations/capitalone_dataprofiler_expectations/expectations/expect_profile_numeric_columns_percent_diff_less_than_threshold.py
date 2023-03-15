import copy
import json
import os
import warnings
from typing import Any, Dict, Optional

import dataprofiler as dp
import pandas as pd
from capitalone_dataprofiler_expectations.expectations.profile_numeric_columns_diff_expectation import (
    ProfileNumericColumnsDiffExpectation,
)
from capitalone_dataprofiler_expectations.expectations.util import (
    is_value_less_than_threshold,
    replace_generic_operator_in_report_keys,
)
from capitalone_dataprofiler_expectations.metrics.data_profiler_metrics.data_profiler_profile_metric_provider import (
    DataProfilerProfileMetricProvider,
)

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.metric_configuration import MetricConfiguration


class DataProfilerProfileNumericColumnsPercentDiffLessThanThreshold(
    DataProfilerProfileMetricProvider
):
    metric_name = (
        "data_profiler.profile_numeric_columns_percent_diff_less_than_threshold"
    )

    value_keys = (
        "profile_path",
        "limit_check_report_keys",
        "numerical_diff_statistics",
    )

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: C901 - 22
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        profile_percent_diff = metrics.get("data_profiler.profile_percent_diff")
        numeric_columns = metrics.get("data_profiler.profile_numeric_columns")
        limit_check_report_keys = metric_value_kwargs["limit_check_report_keys"]
        numerical_diff_statistics = metric_value_kwargs["numerical_diff_statistics"]

        columns = list(profile_percent_diff["global_stats"]["profile_schema"][1].keys())
        data_stats = profile_percent_diff["data_stats"]

        requested_columns = {}
        unavailable_stats = {}
        # Adds columns if generic column key is provided
        # Note: Copy is required for all metric arguments to ensure metric_value_id is identified correctly
        limit_check_report_keys_copy = copy.deepcopy(limit_check_report_keys)
        limit_check_report_keys_copy = replace_generic_operator_in_report_keys(
            limit_check_report_keys_copy, numeric_columns
        )

        for col, stats in limit_check_report_keys_copy.items():
            if col not in numeric_columns:  # Makes sure column requested is numeric
                requested_columns[col] = "Column is Non-Numeric"
                continue

            # adds stats if generic stat key is provided
            numerical_diff_statistics_copy = copy.deepcopy(numerical_diff_statistics)
            stats = replace_generic_operator_in_report_keys(
                stats, numerical_diff_statistics_copy
            )

            if col not in columns:  # Makes sure column exists within profile schema
                requested_columns[col] = "Column requested was not found."
                continue

            col_data_stats = {}
            for data_stat in data_stats:
                if data_stat["column_name"] == col:
                    col_data_stats = data_stat["statistics"]
                    break

            requested_columns[col] = {}
            unavailable_stats[col] = {}
            for stat, threshold in stats.items():
                if stat not in col_data_stats:
                    requested_columns[col][stat] = "Statistic requested was not found."
                    continue
                diff_val = col_data_stats[stat]
                if (
                    diff_val == "ERR_divide_by_zero"
                    or diff_val == "ERR_no_original_value"
                ):
                    unavailable_stats[col][stat] = diff_val
                    continue
                if diff_val == "unchanged":  # In the case there is no delta
                    diff_val = 0
                below_threshold = is_value_less_than_threshold(diff_val, threshold)
                if not below_threshold:
                    requested_columns[col][stat] = {
                        "threshold": threshold,
                        "value_found": diff_val,
                    }
                else:
                    requested_columns[col][stat] = True

        for column in list(unavailable_stats.keys()):
            if unavailable_stats[column] == {}:
                unavailable_stats.pop(column, None)

        if unavailable_stats != {}:
            div_by_zero_stats = []
            no_original_value = []
            for column, stats in unavailable_stats.items():
                current_col = copy.deepcopy(limit_check_report_keys_copy[column])
                for stat, val in stats.items():
                    if val == "ERR_divide_by_zero":
                        div_by_zero_stats.append(column + ": " + stat)
                        current_col.pop(stat, None)
                    elif val == "ERR_no_original_value":
                        no_original_value.append(column + ": " + stat)
                        current_col.pop(stat, None)
                limit_check_report_keys_copy[column] = current_col
            warning = "\nWARNING:\n"
            if len(div_by_zero_stats) > 0:
                warning += "Div By Zero ERROR:\nValue in profile report was 0 for the following column: stat\n"
                for div_by_zero_stat in div_by_zero_stats:
                    warning += "   " + div_by_zero_stat + "\n"
            if len(no_original_value) > 0:
                warning += "Value not Found ERROR:\nStatistic was not found in profile report for the following column: stat\n"
                for no_original_value_string in no_original_value:
                    warning += "   " + no_original_value_string + "\n"
            warning += "\nTo avoid these errors, you should use the replace 'limit_check_report_keys' with the following:\n"
            warning += r"" + json.dumps(limit_check_report_keys_copy, indent=2)
            warning += "\n"
            warnings.warn(warning)
        return requested_columns

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

        if (
            metric.metric_name
            == "data_profiler.profile_numeric_columns_percent_diff_less_than_threshold"
        ):
            dependencies["data_profiler.profile_percent_diff"] = MetricConfiguration(
                metric_name="data_profiler.profile_percent_diff",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )
            dependencies["data_profiler.profile_numeric_columns"] = MetricConfiguration(
                metric_name="data_profiler.profile_numeric_columns",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )

        return dependencies


class ExpectProfileNumericColumnsPercentDiffLessThanThreshold(
    ProfileNumericColumnsDiffExpectation
):
    """
    This expectation takes the percent difference report between the data it is called on and a DataProfiler profile of the same schema loaded from a provided path.
    Each numerical column percent delta will be checked against a user provided dictionary of columns paired with dictionaries of statistics containing a threshold.
    This function builds upon the custom ProfileNumericColumnsDiff Expectation of Capital One's DataProfiler Expectations.
    It is expected that a statistic's percent delta for a given column is less than the specified threshold.

    Args:
        profile_path (str): A path to a saved DataProfiler profile object on the local filesystem.
        limit_check_report_keys (dict[str, dict[str, float]]): A dict, containing column names as keys and dicts as values that contain statistics as keys and thresholds as values
        mostly (float - optional): a value indicating the lower bound percentage of successful values that must be present to evaluate to success=True.
    validator.expect_profile_numeric_columns_percent_diff_less_than_threshold(
        profile_path = "C:/path_to/my_profile.pkl",
        limit_check_report_keys = {
            "column_one": {
                "min": 0.5, #Indicating the threshold for the 'min' statistic in 'column_one' is 50%
            },
            "*": {
                "*": .25, #Indicating the threshold for every statistic in every column is 25%
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

    dir_path = os.path.dirname(os.path.abspath(__file__))  # noqa: PTH120, PTH100
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
                    "title": "profile_min_delta_below_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "*": {
                                "min": 2.0,
                            },
                        },
                    },
                    "out": {"success": True},
                },
                {
                    "title": "single_column_min_delta_below_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "by_2": {
                                "min": 1.01,
                            },
                        },
                    },
                    "out": {"success": True},
                },
                {
                    "title": "single_column_min_delta_equals_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "by_2": {
                                "min": 1.0,
                            },
                        },
                    },
                    "out": {"success": False},
                },
                {
                    "title": "profile_all_stats_above_delta_threshold",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "profile_path": profile_path,
                        "limit_check_report_keys": {
                            "*": {"*": 0.0},
                        },
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    profile_metric = (
        "data_profiler.profile_numeric_columns_percent_diff_less_than_threshold"
    )

    success_keys = (
        "profile_path",
        "limit_check_report_keys",
        "numerical_diff_statistics",
        "mostly",
    )

    default_limit_check_report_keys = {
        "*": {
            "min": 0.0,
            "max": 0.0,
            "sum": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "median_absolute_deviation": 0.0,
            "variance": 0.0,
            "stddev": 0.0,
            "unique_count": 0.0,
            "unique_ratio": 0.0,
            "gini_impurity": 0.0,
            "unalikeability": 0.0,
            "sample_size": 0.0,
            "null_count": 0.0,
        }
    }

    numerical_diff_statistics = list(default_limit_check_report_keys["*"].keys())

    default_kwarg_values = {
        "limit_check_report_keys": default_limit_check_report_keys,
        "numerical_diff_statistics": numerical_diff_statistics,
        "mostly": 1.0,
    }

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
        ExpectProfileNumericColumnsPercentDiffLessThanThreshold().run_diagnostics()
    )
    print(diagnostics_report.generate_checklist())
