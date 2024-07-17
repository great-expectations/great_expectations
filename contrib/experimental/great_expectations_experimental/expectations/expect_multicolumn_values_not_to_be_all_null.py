from typing import Optional

import numpy as np

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most MulticolumnMapExpectations, the main business logic for calculation will live in this class.
class MulticolumnValuesNotAllNull(MulticolumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.

    condition_metric_name = "multicolumn_values.not_all_null"

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

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        return column_list.notna().any(axis=1)

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        conditions = []
        for column in column_list:
            conditions.append(column.isnot(None))
        return sa.or_(*conditions)

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column_list, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectMulticolumnValuesNotToBeAllNull(MulticolumnMapExpectation):
    """Expect the certain set of columns not to be null at the same time."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "only_for": ["pandas"],
            "data": {
                "no_nulls": [5, 6, 5, 12, -3],
                "some_nulls": [np.nan, -3, np.nan, np.nan, -9],
                "one_non_null": [np.nan, 2, np.nan, np.nan, np.nan],
                "all_nulls": [np.nan, np.nan, np.nan, np.nan, np.nan],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["no_nulls", "some_nulls"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["some_nulls", "one_non_null"],
                        "mostly": 0.4,
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
                        "column_list": ["some_nulls", "one_non_null", "all_nulls"],
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        },
        {
            "only_for": ["sqlite"],
            "data": {
                "no_nulls": [5, 6, 5, 12, -3],
                "some_nulls": [None, -3, None, None, -9],
                "one_non_null": [None, 2, None, None, None],
                "all_nulls": [None, None, None, None, None],
                "one_null": [None, 1, 1, 1, 1],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column_list": ["no_nulls", "some_nulls"]},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["some_nulls", "one_non_null"],
                        "mostly": 0.4,
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
                        "column_list": ["some_nulls", "one_non_null", "all_nulls"],
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "basic_negative_test_2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_list": ["all_nulls", "one_null"],
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.

    map_metric = "multicolumn_values.not_all_null"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "column_list",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {"ignore_row_if": "never"}

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

    library_metadata = {
        "tags": ["null_check"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@liyusa",
            "@itaise",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectMulticolumnValuesNotToBeAllNull().print_diagnostic_checklist()
