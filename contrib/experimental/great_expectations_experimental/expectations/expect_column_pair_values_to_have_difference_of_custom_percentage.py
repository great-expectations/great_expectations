from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnPairMapExpectation
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


class ColumnPairValuesDiffCustomPercentageOrLess(ColumnPairMapMetricProvider):
    """MetricProvider Class for Pair Values Diff Less Than Custom Percentage"""

    condition_metric_name = "column_pair_values.diff_custom_percentage"

    condition_domain_keys = (
        "column_A",
        "column_B",
    )
    condition_value_keys = ("percentage",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, percentage, **kwargs):
        return abs(column_A - column_B) <= abs(column_A * percentage)

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, percentage, **kwargs):
        return F.when(
            F.abs(column_A - column_B) <= F.abs(column_A * percentage), True
        ).otherwise(False)


class ExpectColumnPairValuesToHaveDifferenceOfCustomPercentage(
    ColumnPairMapExpectation
):
    """Expect two columns to have a row-wise difference of three."""

    map_metric = "column_pair_values.diff_custom_percentage"

    # Examples that will be shown in the public gallery.
    examples = [
        {
            "data": {
                "col_a": [3, -3, 10, 2, 3, 2],
                "col_b": [0.5, -3.0, 11.7, 2.3, 2.8, 2.0],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "col_a",
                        "column_B": "col_b",
                        "percentage": 0.2,
                        "mostly": 0.8,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "col_a",
                        "column_B": "col_b",
                        "percentage": 0.1,
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values
    success_keys = (
        "percentage",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "percentage": 0.1,
        "mostly": 1.0,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
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

        mostly = configuration.kwargs["mostly"]
        percentage = configuration.kwargs["percentage"]

        if configuration is None:
            configuration = self.configuration

        # Check if both columns are provided and values of mostly and percentage are correct
        try:
            assert (
                "column_A" in configuration.kwargs
                and "column_B" in configuration.kwargs
            ), "both columns must be provided"
            assert 0 <= mostly <= 1, "Mostly must be between 0 and 1"
            assert percentage >= 0, "Percentage must be positive"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # Metadata for display in the public gallery
    library_metadata = {
        "tags": [
            "basic math",
            "multi-column expectation",
        ],
        "contributors": ["@jorgicol", "@exteli"],
    }


if __name__ == "__main__":
    ExpectColumnPairValuesToHaveDifferenceOfCustomPercentage().print_diagnostic_checklist()
