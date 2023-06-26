"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""
from typing import Any

import dataprofiler as dp
import numpy as np
import pandas as pd
import tensorflow as tf

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesGreaterThanOrEqualToProfileMin(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.greater_than_or_equal_to_profile_min"

    condition_value_keys = ("profile",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls: Any, column: str, profile: Any, **kwargs) -> np.ndarray:
        columnPresent = (
            column.name
            in profile["global_stats"][
                "profile_schema"
            ]  # checks to ensure column exists
        )
        transpose = np.array(column).T
        if not (columnPresent):  # Err column in user DF not present in input profile
            return transpose != transpose  # Returns 100% unexpected

        index = profile["global_stats"]["profile_schema"][column.name][
            0
        ]  # Gets index of column from profile

        dataType = profile["data_stats"][index]["data_type"]  # Checks datatype
        if dataType != "int" and dataType != "float":  # Err non-numerical column
            return transpose != transpose  # Returns 100% unexpected

        minimum = float(profile["data_stats"][index]["statistics"]["min"])

        return transpose >= minimum

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeEqualToOrGreaterThanProfileMin(ColumnMapExpectation):
    """
    This function builds upon the custom column map expectations of Great Expectations. This function asks a yes/no question of each row in the user-specified column;
    namely, is the value greater than or equal to the minimum value of the respective column within the provided profile report generated from the DataProfiler.

    Args:
        column(str): The column that you want to check.
        profile(dict(str, Any)): The report, which is assumed to contain a column of the same name, previously generated using the DataProfiler.

    df.expect_column_values_to_be_equal_to_or_greater_than_profile_min(
        column,
        profile
    )

    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.

    data = [
        [-36, -25, -44],
        [18, 45, 46],
        [-16, -29, -49],
        [21, 4, 35],
        [-18, -7, -40],
        [22, -4, -37],
        [-17, -21, 11],
        [48, -32, -48],
        [0, -44, 20],
    ]
    cols = ["col_a", "col_b", "col_c"]

    df = pd.DataFrame(data, columns=cols)
    profiler_opts = dp.ProfilerOptions()
    profiler_opts.structured_options.multiprocess.is_enabled = False
    profileObj = dp.Profiler(df, options=profiler_opts)
    profileReport = profileObj.report(report_options={"output_format": "serializable"})
    profileReport["global_stats"]["profile_schema"] = dict(
        profileReport["global_stats"]["profile_schema"]
    )

    examples = [
        {
            "data": {
                "col_a": [-3, 21, 20, 5],
                "col_b": [-7, 41, -47, 12],
                "col_c": [54, -10, 19, 19],
            },
            "tests": [
                {
                    "title": "column_lower_bounded_by_min",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col_a",
                        "profile": profileReport,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "column_has_value_less_than_min",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col_b",
                        "profile": profileReport,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.greater_than_or_equal_to_profile_min"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "profile",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "profile": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "requirements": ["dataprofiler", "tensorflow", "scikit-learn", "numpy"],
        "maturity": "experimental",  # "concept_only", "experimental", "beta", or "production"
        "tags": ["dataprofiler"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@stevensecreti",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    # ExpectColumnValuesToBeEqualToOrGreaterThanProfileMin().print_diagnostic_checklist()
    diagnostics_report = (
        ExpectColumnValuesToBeEqualToOrGreaterThanProfileMin().run_diagnostics()
    )
    print(diagnostics_report.generate_checklist())
