"""
This is a template for creating custom ColumnAggregateExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
"""

from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnAggregateExpectations, the main business logic for calculation will live in this class.
class ColumnCountToBeBetween(ColumnAggregateMetricProvider):
    # This is the id string that will be used to reference your Metric.
    metric_name = "column.row_count"

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.notnull().sum()

    # This method defines the business logic for evaluating your Metric when using a SqlAlchemyExecutionEngine
    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError
    #
    # This method defines the business logic for evaluating your Metric when using a SparkDFExecutionEngine
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnCountToBeBetween(ColumnAggregateExpectation):
    """Expect Column Count to be between a certain range"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "y": [0, -2, None, 4, None, 3, -3, 0, 1, None],
            },
            "only_for": ["pandas", "spark", "sqlite", "postgresql"],
            "tests": [
                {
                    "title": "pos_test_total_cnt",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "min_value": 10,
                        "strict_min": False,
                        "max_value": 10,
                        "strict_max": False,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "pos_test_range_cnt",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "min_value": 5,
                        "strict_min": True,
                        "max_value": 8,
                        "strict_max": True,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "neg_test_total_cnt_range",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "min_value": 4,
                        "strict_min": True,
                        "max_value": 9,
                        "strict_max": True,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "neg_test_total_cnt",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "min_value": 10,
                        "strict_min": True,
                        "max_value": 10,
                        "strict_max": True,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "neg_test_negatives",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "min_value": -2,
                        "strict_min": False,
                        "max_value": 3,
                        "strict_max": True,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.row_count",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")

    # This dictionary contains default values for any parameters that should have default values.
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

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        column_count = metrics["column.row_count"]
        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_count > min_value
            else:
                above_min = column_count >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_count < max_value
            else:
                below_max = column_count <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_count}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ['column aggregate expectation', 'column count'],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@data-han",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnCountToBeBetween().print_diagnostic_checklist()
