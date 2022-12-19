"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)

# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.


class ColumnValuesToBeHexadecimal(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.is_hexadecimal"

    filter_column_isnull = False

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_hex(x):
            if not x:
                return False
            if x is None:
                return False
            if not isinstance(x, str):
                return False
            try:
                int(x, 16)
                return True
            except ValueError:
                return False

        return column.apply(is_hex)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeHexadecimal(ColumnMapExpectation):
    """Expect column values to be valid hexadecimals."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "a": ["3", "aa", "ba", "5A", "60F", "Gh"],
                "b": ["Verify", "String", "3Z", "X", "yy", "sun"],
                "c": ["0", "BB", "21D", "ca", "20", "1521D"],
                "d": ["c8", "ffB", "11x", "apple", "ran", "woven"],
                "e": ["a8", "21", 2.0, "1B", "4AA", "31"],
                "f": ["a8", "41", "ca", 46, "4AA", "31"],
                "g": ["a8", "41", "ca", "", "0", "31"],
                "h": ["a8", "41", "ca", None, "0", "31"],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "a", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [5],
                        "unexpected_list": ["Gh"],
                    },
                },
                {
                    "title": "negative_test_without_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "b"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2, 3, 4, 5],
                        "unexpected_list": ["Verify", "String", "3Z", "X", "yy", "sun"],
                    },
                },
                {
                    "title": "positive_test_without_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "c"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "d", "mostly": 0.6},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 3, 4, 5],
                        "unexpected_list": ["11x", "apple", "ran", "woven"],
                    },
                },
                {
                    "title": "negative_test_with_float",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "e"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2],
                        "unexpected_list": [2.0],
                    },
                },
                {
                    "title": "negative_test_with_int",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "f"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [3],
                        "unexpected_list": [46],
                    },
                },
                {
                    "title": "negative_test_with_empty_value",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "g"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [3],
                        "unexpected_list": [""],
                    },
                },
                {
                    "title": "negative_test_with_none_value",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "h"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [3],
                        "unexpected_list": [None],
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.is_hexadecimal"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
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

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@andrewsx",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeHexadecimal().print_diagnostic_checklist()
