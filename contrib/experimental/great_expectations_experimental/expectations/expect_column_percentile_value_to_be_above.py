from typing import Dict, Optional

from great_expectations.core.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
)
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnAggregateExpectations, the main business logic for calculation will live in this class.
class ColumnPercentile(ColumnAggregateMetricProvider):
    metric_name = "column.percentile"
    value_keys = ("percentile",)

    # This is the id string that will be used to reference your Metric.

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, percentile, **kwargs):
        # breakpoint()
        return column.quantile(q=percentile / 100)  # numpy method quantile

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


class ExpectColumnPercentileToBeAbove(ColumnAggregateExpectation):
    """Expect column percentile value to be above a given value."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {"x": [1, 2, 3, 4, 5], "y": [0, -1, -2, 4, None]},
            "only_for": ["pandas"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "percentile": 40,
                        "value": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "y",
                        "percentile": 10,
                        "value": 1,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.percentile",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("value", "percentile")

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
        """Validates the given data above the set value for the column given percentile"""

        column_percentile = metrics["column.percentile"]
        given_value = self.get_success_kwargs(configuration).get("value")

        # Checking if the given percentile lies above the threshold

        success = column_percentile >= given_value

        return {
            "success": success,
            "result": {"observed_value": column_percentile},
        }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "column aggregate expectation"
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@kurt1984",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnPercentileToBeAbove().print_diagnostic_checklist()
