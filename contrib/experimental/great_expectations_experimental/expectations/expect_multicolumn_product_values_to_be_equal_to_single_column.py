import functools
import operator
from typing import Optional

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most MulticolumnMapExpectations, the main business logic for calculation will live in this class.
# <snippet>
class MulticolumnValuesProductEqualToSingleColumn(MulticolumnMapMetricProvider):
    # </snippet>
    # This is the id string that will be used to reference your metric.
    # <snippet>
    condition_metric_name = "multicolumn_values.product_equal_to_single_column"
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
    condition_value_keys = ("additional_value",)

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, additional_value, **kwargs):
        columns_to_multiply = column_list[0:-1]
        column_to_equal = column_list[-1]
        sqlalchemy_columns_to_multiply = columns_to_multiply[0]
        if len(columns_to_multiply) > 1:
            for column in columns_to_multiply[1:]:
                sqlalchemy_columns_to_multiply *= column
        sqlalchemy_columns_to_multiply *= additional_value
        return sqlalchemy_columns_to_multiply == column_to_equal

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, dataframe, additional_value, **kwargs):
        column_list = dataframe.columns
        columns_to_multiply = column_list[:-1]
        column_to_equal = column_list[-1]

        columns_mulitplied = functools.reduce(
            operator.mul, [F.col(column) for column in columns_to_multiply]
        )

        columns_mulitplied *= additional_value

        return columns_mulitplied == F.col(column_to_equal)

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, dataframe, additional_value, **kwargs):
        columns_to_multiply = dataframe.iloc[:, :-1]
        column_to_equal = dataframe.iloc[:, -1]

        pandas_columns_to_multiply = columns_to_multiply.iloc[:, 0]
        if len(columns_to_multiply.columns) > 1:
            for i in range(1, (len(columns_to_multiply.columns))):
                pandas_columns_to_multiply *= columns_to_multiply.iloc[:, i]
        pandas_columns_to_multiply *= additional_value
        return pandas_columns_to_multiply == column_to_equal


# This class defines the Expectation itself
# <snippet>
class ExpectMulticolumnProductToBeEqualToSingleColumn(MulticolumnMapExpectation):
    # </snippet>
    # <snippet>
    """Expect a product of columns to be equal to other column (in a row perspective).

    This means that for each row, we expect col_a * col_b * ... * col_n-1 == col_n

    Args:
        column_list (list of str): \
            A list of 2 or more float columns, in which we expect the product of the first n-1th \
            columns to be equal to the nth column. This means that if one wants to compare \
            between the product of n-1 columns and the nth column, it needs to put the nth column \
            at the end of the list.
        additional_value (optional): \
            A numeric value that is included in the calculation to equal the nth column. \
            The calculation becomes col_a * col_b * ... * col_n-1 * additional_value == col_n
    """
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [3.5, 6.0, 0.0, 1.25],
                "col_b": [-6.0, -3.0, 1.0, 2.5],
                "col_c": [1.0, 0.0, -1.0, 3.0],
                "col_d": [-21.0, 0.0, 0.0, 9.375],
                "col_e": [3.5, 6.0, 0.0, 1.25],
                "col_f": [-21.0, -18.0, 0.0, 3.125],
                "col_g": [7.0, 12.0, 0.0, 2.5],
                "col_h": [-42.0, -36.0, 0.0, 6.25],
                "col_i": [3, -2, 0, 5],
                "col_j": [2, 1, 5, 8],
                "col_k": [12, -4, 0, 80],
            },
            "tests": [
                {
                    "title": "columns_to_multiply 1-element-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_e"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply 2-element-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_b", "col_f"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply 3-elements-set is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_b", "col_c", "col_d"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply set is not equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_c", "col_d"]},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "columns_to_multiply set and additional_value is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_a", "col_b", "col_h"],
                        "additional_value": 2,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply 1-element set and additional_value is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_g"], "additional_value": 2},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply set and additional_value is not equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_a", "col_c", "col_d"]},
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "columns_to_multiply integer set and additional_value is equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["col_i", "col_j", "col_k"],
                        "additional_value": 2,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "columns_to_multiply integer set and additional_value is not equal to column_to_equal",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_i", "col_j", "col_k"]},
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet>``
    map_metric = "multicolumn_values.product_equal_to_single_column"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_list",
        "mostly",
        "additional_value",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {"additional_value": 1.0}

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
    # <snippet>
    library_metadata = {
        "tags": [
            "multi-column expectation",
            "multi-column product values to be equal to single column",
        ],
        "contributors": ["@calvingdu"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet>
    ExpectMulticolumnProductToBeEqualToSingleColumn().print_diagnostic_checklist()
# </snippet>
