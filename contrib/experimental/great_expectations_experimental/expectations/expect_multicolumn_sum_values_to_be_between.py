"""
This is a template for creating custom MulticolumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations
"""
import functools
import operator
from typing import Optional

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most MulticolumnMapExpectations, the main business logic for calculation will live in this class.
class MulticolumnValuesSumValuesToBeBetweenMaxAndMin(MulticolumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "multicolumn_values.sum_values_to_be_between_max_and_min"
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = (
        "min_value",
        "max_value",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, min_value, max_value, **kwargs):
        sum_of_columns = column_list.sum(axis=1)
        return (sum_of_columns >= min_value) & (sum_of_columns <= max_value)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_list, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, min_value, max_value, **kwargs):
        columns_to_sum = column_list.columns
        return (
            functools.reduce(operator.add, [F.col(column) for column in columns_to_sum])
            >= F.lit(min_value)
        ) & (
            functools.reduce(operator.add, [F.col(column) for column in columns_to_sum])
            <= F.lit(max_value)
        )


# This class defines the Expectation itself
class ExpectMulticolumnSumValuesToBeBetween(MulticolumnMapExpectation):
    """Expect a sum of values over the columns to be between max and min values.

    min_value <= SUM(col_a, cob_b, cob_c, ...) <= max_value

    Args:
    column_list (list of str): \
        A list of 2 or more integer columns
    min_value (int): \
        A value that the sum of values over the column must be equal to or more than the given value
    max_value (int): \
        A value that the sum of values over the column must be equal to or less than the given value
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [3, 6, 9, 12, 3],
                "col_b": [0, 3, 6, 33, 9],
                "col_c": [1, 5, 6, 27, 3],
            },
            "only_for": ["pandas", "spark"],
            "tests": [
                {
                    "title": "multi_column_sum_to_equal_range_2-set_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_a", "col_b"],
                        "min_value": 1,
                        "max_value": 50,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "multi_column_sum_to_equal_range_3-set_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_a", "col_b", "col_c"],
                        "min_value": 4,
                        "max_value": 72,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "multi_column_sum_to_equal_range_2-set_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_a", "col_b"],
                        "min_value": 10,
                        "max_value": 30,
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "multi_column_sum_to_equal_range_3-set_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_a", "col_b", "col_c"],
                        "min_value": 10,
                        "max_value": 20,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "multicolumn_values.sum_values_to_be_between_max_and_min"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "min_value",
        "max_value",
    )

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
        configuration = configuration or self.configuration

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
        "tags": [
            "multi-column sum to be between min and max" "multi-column expectation"
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@swittchawa",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectMulticolumnSumValuesToBeBetween().print_diagnostic_checklist()
