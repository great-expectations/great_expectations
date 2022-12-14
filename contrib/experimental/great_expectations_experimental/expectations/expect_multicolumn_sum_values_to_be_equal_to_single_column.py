"""
This is a template for creating custom MulticolumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations
"""

from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most MulticolumnMapExpectations, the main business logic for calculation will live in this class.
# <snippet>
class MulticolumnValuesSumValuesEqualToSingleColumn(MulticolumnMapMetricProvider):
    # </snippet>
    # This is the id string that will be used to reference your metric.
    # <snippet>
    condition_metric_name = "multicolumn_values.sum_values_equal_to_single_column"
    # </snippet>
    # These point your metric at the provided keys to facilitate calculation
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ()

    # This method implements the core logic for the PandasExecutionEngine
    # <snippet>
    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        columns_to_sum = column_list[0:-1]
        sqlalchemy_columns_to_sum = columns_to_sum[0]
        if len(columns_to_sum) > 1:
            for column in columns_to_sum[1:]:
                sqlalchemy_columns_to_sum += column
        column_to_equal = column_list[-1]
        return sqlalchemy_columns_to_sum == column_to_equal

    # </snippet>

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column_list, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_list, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
# <snippet>
class ExpectMulticolumnSumValuesToBeEqualToSingleColumn(MulticolumnMapExpectation):
    # </snippet>
    # <snippet>
    """Expect a sum of columns to be equal to other column (in a row perspective).

    This means that for each row, we expect col_a + col_b + ... + col_n-1 == col_n

    Args:
        column_list (list of str): \
            A list of 2 or more integer columns, in which we expect the sum of the first n-1th \
            columns to be equal to the nth column. This means that if one wants to compare \
            between the sum of n-1 columns and the nth column, it needs to put the nth column \
            at the end of the list.
    """
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [3, 6, 0, 1],
                "col_b": [-6, -3, 1, 2],
                "col_c": [1, 0, -1, 3],
                "col_d": [-2, 3, 0, 6],
                "col_e": [3, 6, 0, 1],
                "col_f": [-3, 3, 1, 3],
            },
            "tests": [
                {
                    "title": "columns_to_sum 1-element-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_e"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_sum 2-element-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_b", "col_f"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_sum 3-elements-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_b", "col_c", "col_d"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_sum set is not equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_c", "col_d"]},
                    "out": {
                        "success": False,
                    },
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet>
    map_metric = "multicolumn_values.sum_values_equal_to_single_column"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_list",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

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
    # <snippet>
    library_metadata = {
        "tags": [
            "multi-column expectation",
            "multi-column sum values to be equal to single column",
        ],
        "contributors": ["@AsaFLachisch"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet>
    ExpectMulticolumnSumValuesToBeEqualToSingleColumn().print_diagnostic_checklist()
# </snippet>
