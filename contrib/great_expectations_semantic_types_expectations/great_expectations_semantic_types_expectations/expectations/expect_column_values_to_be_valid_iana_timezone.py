import json
from typing import Optional

import pytz

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes


def is_valid_timezone(timezone: str) -> bool:
    try:
        pytz.timezone(timezone)
        return True
    except pytz.UnknownTimeZoneError:
        return False


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesIanaTimezone(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.iana_timezone"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        return column.apply(lambda x: is_valid_timezone(x))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):

        tz_udf = F.udf(is_valid_timezone, sparktypes.BooleanType())

        return tz_udf(column)


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidIanaTimezone(ColumnMapExpectation):
    """Expect values in this column to be valid IANA timezone strings.
    A full list of valid timezones can be viewed by `pytz.all_timezones`.
    See https://www.iana.org/time-zones for more information.
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "valid_timezones": [
                    "UTC",
                    "America/New_York",
                    "Australia/Melbourne",
                    "US/Hawaii",
                    "Africa/Sao_Tome",
                ],
                "invalid_timezones": [
                    "America/Calgary",
                    "Europe/Nice",
                    "New York",
                    "Central",
                    "+08:00",
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_timezones",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_timezones"},
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_timezones",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "invalid_timezones"},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.iana_timezone"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

        return True

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ["type-entities", "hackathon", "timezone"],
        "contributors": [
            "@lucasasmith",
        ],
        "requirements": ["pytz"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidIanaTimezone().print_diagnostic_checklist()
