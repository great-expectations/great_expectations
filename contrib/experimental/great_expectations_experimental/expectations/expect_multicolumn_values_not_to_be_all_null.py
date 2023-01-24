"""
This is a template for creating custom MulticolumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations
"""

from typing import Optional

import numpy as np
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
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
class MulticolumnValuesNotAllNull(MulticolumnMapMetricProvider):
    # </snippet>
    # This is the id string that will be used to reference your metric.
    # <snippet>
    condition_metric_name = "multicolumn_values.not_all_null"
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
    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        row_wise_cond = column_list.isna().sum(axis=1) < len(column_list)
        return row_wise_cond

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
class ExpectMulticolumnValuesNotToBeAllNull(MulticolumnMapExpectation):
    # </snippet>
    # <snippet>
    """Expect the certain set of columns not to be null at the same time"""
    # </snippet>

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "col_a": [5, 6, 5, 12, -3],
                "col_b": [np.nan, -3, np.nan, np.nan, -9],
                "col_c": [np.nan, 2, np.nan, np.nan, np.nan],
                "col_d": [np.nan, np.nan, np.nan, np.nan, np.nan],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_b", "col_c"], "mostly": 0.4},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["col_b", "col_c", "col_d"], "mostly": 1},
                    "out": {
                        "success": False,
                    },
                },
            ],
            "test_backends": [
                {
                    "backend": "pandas",
                    "dialects": None,
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    # <snippet>
    map_metric = "multicolumn_values.not_all_null"
    # </snippet>

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_list",
        "mostly",
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
    # <snippet>
    library_metadata = {
        "tags": ["null_check"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@liyusa",  # Don't forget to add your github handle here!
        ],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet>
    ExpectMulticolumnValuesNotToBeAllNull().print_diagnostic_checklist()
# </snippet>
