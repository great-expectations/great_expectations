from typing import Optional

import pyspark.sql.functions as F

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import SparkDFExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesMatchSQLDateTimeFormat(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.matches_sql_date_time_format"
    condition_value_keys = ("date_time_format",)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        date_time_format,
        **kwargs,
    ):
        # Below is a simple validation that the provided format can both format and
        # parse a datetime object. %D is an example of a format that can format but not
        # parse, e.g.
        return F.unix_timestamp(column, date_time_format).isNotNull()


# This class defines the Expectation itself
class ExpectColumnValuesToMatchSQLDateTimeFormat(ColumnMapExpectation):
    """Expect the column entries to be strings representing a date or time with a given SQL date/time format."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": ["2019-04-01", "2019-04-02", "2019-04-03", "2019-04-13"],
                "b": ["01/01/2010", "01/01/2011", "01/01/2012", None],
                "c": ["01-01-2019", "01-10-2019", "01/20/2019", "01-30-2019"],
                "d": [1, 2, 3, 4],
                "e": ["2019-04-01", "2019-04-02", "2019-04-03", "2019-04-13"],
                "f": [
                    "1977-05-55T00:00:00",
                    "1980-05-21T13:47:59",
                    "2017-06-12T23:57:59",
                    None,
                ],
            },
            "schemas": {
                "spark": {
                    "a": "StringType",
                    "b": "StringType",
                    "c": "StringType",
                    "d": "IntegerType",
                    "e": "TimestampType",
                    "f": "StringType",
                }
            },
            "tests": [
                {
                    "title": "simple_positive_test",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {
                        "column": "a",
                        "date_time_format": "yyyy-MM-dd",
                    },
                    "out": {"success": True, "unexpected_list": []},
                },
                {
                    "title": "negative_test_wrong_format",
                    "exact_match_out": False,
                    "in": {"column": "a", "date_time_format": "yyyyMMdd"},
                    "out": {
                        "success": False,
                        "unexpected_list": [
                            "2019-04-01",
                            "2019-04-02",
                            "2019-04-03",
                            "2019-04-13",
                        ],
                    },
                },
                {
                    "title": "positive_test_w_nulls",
                    "exact_match_out": False,
                    "in": {"column": "b", "date_time_format": "dd/MM/yyyy"},
                    "out": {"success": True, "unexpected_list": []},
                },
                {
                    "title": "positive_test_w_mostly",
                    "exact_match_out": False,
                    "in": {
                        "column": "c",
                        "date_time_format": "MM-dd-yyyy",
                        "mostly": 0.75,
                    },
                    "out": {"success": True, "unexpected_list": ["01/20/2019"]},
                },
                {
                    "title": "simple_negative_test",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "c", "date_time_format": "MM-dd-yyyy"},
                    "out": {"success": False, "unexpected_list": ["01/20/2019"]},
                },
                {
                    "title": "negative_test_out_of_bounds_value_for_month",
                    "exact_match_out": False,
                    "in": {"column": "a", "date_time_format": "yyyy-dd-MM"},
                    "out": {"success": False, "unexpected_list": ["2019-04-13"]},
                },
                {
                    "title": "negative_test_iso8601",
                    "exact_match_out": False,
                    "in": {"column": "f", "date_time_format": "yyyy-MM-dd'T'HH:mm:ss"},
                    "out": {
                        "success": False,
                        "unexpected_list": ["1977-05-55T00:00:00"],
                    },
                },
                {
                    "title": "test_raising_exception_for_wrong_input_data_type",
                    "exact_match_out": False,
                    "in": {
                        "column": "d",
                        "date_time_format": "MM-dd-yyyy",
                        "catch_exceptions": True,
                    },
                    "out": {
                        "traceback_substring": 'pyspark.errors.exceptions.captured.AnalysisException: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve "unix_timestamp(d, MM-dd-yyyy)" due to data type mismatch: Parameter 1 requires the ("STRING" or "DATE" or "TIMESTAMP" or "TIMESTAMP_NTZ") type'
                    },
                },
                {
                    "title": "test_raising_exception_for_wrong_format",
                    "exact_match_out": False,
                    "in": {
                        "column": "a",
                        "date_time_format": "afbhasbg",
                        "catch_exceptions": True,
                    },
                    "out": {
                        "traceback_substring": "pyspark.errors.exceptions.captured.IllegalArgumentException: Unknown pattern letter"
                    },
                },
                # NOTE: this differs from expect_column_values_to_match_strftime_format
                # which fails when data is already in the datetime format
                {
                    "title": "negative_test_input_already_datetimes",
                    "exact_match_out": False,
                    "suppress_test_for": ["pandas"],
                    "in": {
                        "column": "e",
                        "date_time_format": "yyyy-MM-dd",
                        "catch_exceptions": True,
                    },
                    "out": {
                        "success": True,
                        "unexpected_list": [],
                    },
                },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.matches_sql_date_time_format"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "date_time_format",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental"],
        "contributors": [
            "@cookepm86",
        ],
    }

    def validate_configuration(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        column = configuration.kwargs.get("column")
        date_time_format = configuration.kwargs.get("date_time_format")

        # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            assert column is not None, "column must be specified"
            assert date_time_format is not None, "date_time_format must be specified"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))


if __name__ == "__main__":
    ExpectColumnValuesToMatchSQLDateTimeFormat().print_diagnostic_checklist()
