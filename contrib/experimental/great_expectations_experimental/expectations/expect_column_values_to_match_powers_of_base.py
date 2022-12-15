"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import math
from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This method checks to see if the given num is a power of base.
# For example: 32 is a power of 2 since 2^5 = 32.
def is_power_of_n(num, base) -> bool:
    if base in {0, 1}:
        return num == base
    power = int(math.log(num, base) + 0.5)
    return base**power == num


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesMatchPowersOfBase(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.match_powers_of_base"
    condition_value_keys = ("base_integer",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, base_integer, **kwargs):
        return column.apply(lambda x: is_power_of_n(x, base_integer))


# This class defines the Expectation itself
class ExpectColumnValuesToMatchPowersOfBase(ColumnMapExpectation):
    """Expect column values to match powers of Base (Base ** power == column value)."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "mostly_powers_of_two_but_one": [
                    1,
                    2,
                    4,
                    8,
                    16.0,
                    11,
                    67108864,
                    32.00,
                    64,
                    128,
                    256,
                    512,
                    1024,
                    128,
                ],
                "all_powers_of_3": [
                    59049,
                    1,
                    3,
                    729,
                    1594323,
                    81,
                    27,
                    243,
                    9,
                    177147,
                    531441,
                    2187,
                    6561,
                    19683,
                ],
                "all_powers_of_2_increasing_order": [
                    1,
                    2,
                    4,
                    8,
                    16,
                    32,
                    64,
                    128,
                    256,
                    512,
                    1024,
                    2048,
                    4096,
                    8192,
                ],
                "all_powers_of_7": [
                    1,
                    49,
                    678223072849,
                    7,
                    4747561509943,
                    343,
                    823543,
                    2401,
                    40353607,
                    96889010407,
                    16807,
                    4747561509943,
                    1628413597910449,
                    40353607,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly_powers_of_two_but_one",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_powers_of_two_but_one",
                        "base_integer": 2,
                        "mostly": 0.8,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [5],
                        "unexpected_list": [11],
                    },
                },
                {
                    "title": "positive_test_with_all_powers_of_3",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_powers_of_3", "base_integer": 3},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_all_powers_of_7",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_powers_of_7",
                        "base_integer": 7,
                        "mostly": 0.3,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_all_powers_of_2_increasing_order",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_powers_of_2_increasing_order",
                        "base_integer": 2,
                        "mostly": 1,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_powers_of_two",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_powers_of_two_but_one",
                        "base_integer": 2,
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [5],
                        "unexpected_list": [11],
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.match_powers_of_base"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "base_integer",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {"mostly": 1}

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

        base_integer = configuration.kwargs["base_integer"]

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        try:
            # Base cannot be less than zero,
            # Base must be an Integer
            assert base_integer is None or isinstance(base_integer, int)
            assert base_integer >= 0
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "maturity": "beta",  # "experimental", "beta", or "production"
        "tags": ["beta"],  # Tags for this Expectation in the Gallery
        "contributors": [  # GitHub handles for all contributors to this Expectation.
            "@rifatKomodoDragon",  # Don't forget to add your GitHub handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToMatchPowersOfBase().print_diagnostic_checklist()
