from typing import Optional

from timezonefinder import TimezoneFinder

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesLatLonInTimezone(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.lat_lon_in_timezone"
    condition_value_keys = ("timezone",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, timezone, **kwargs):
        def is_in_timezone(point, timezone):
            try:
                tf = TimezoneFinder()
                detected_timezone = tf.timezone_at(lat=point[0], lng=point[1])
                return detected_timezone == timezone
            except ValueError:
                return False

        return column.apply(lambda x: is_in_timezone(x, timezone))

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, timezone, **kwargs):
        def is_in_timezone(point, timezone):
            try:
                tf = TimezoneFinder()
                detected_timezone = tf.timezone_at(lat=point[0], lng=point[1])
                return detected_timezone == timezone
            except ValueError:
                return False

        tz_udf = F.udf(lambda x: is_in_timezone(x, timezone), sparktypes.BooleanType())

        return tz_udf(column)


# This class defines the Expectation itself
class ExpectColumnValuesToBeLatLonInTimezone(ColumnMapExpectation):
    """Expect each lat lon pair in this column to be a point inside a given timezone.
    Timezone names can be found in https://en.wikipedia.org/wiki/List_of_tz_database_time_zones under 'TZ database name'.
    This works offline, so it isn't 100% accurate as timezones change but it should be enough for most purposes.
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "lat_lon_in_los_angeles_timezone": [
                    (33.570321, -116.884380),
                    (32.699316, -117.063457),
                    (32.699316, -117.063457),
                    (33.598757, -117.721397),
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_valid_timezone",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_los_angeles_timezone",
                        "timezone": "America/Los_Angeles",
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_test_with_invalid_timezone",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_los_angeles_timezone",
                        "timezone": "Etc/UTC",
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.lat_lon_in_timezone"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "timezone",
    )

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
        "tags": ["geospatial", "hackathon-22", "timezone"],
        "contributors": [
            "@mmi333",
        ],
        "requirements": ["timezonefinder"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeLatLonInTimezone().print_diagnostic_checklist()
