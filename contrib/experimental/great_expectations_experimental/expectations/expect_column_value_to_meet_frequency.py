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
class ColumnValueMeetsFrequency(ColumnAggregateMetricProvider):
    # This is the id string that will be used to reference your Metric.
    metric_name = "column.value_meets_frequency"
    # Values passed in to our expectation used in our metric
    value_keys = ('value',)

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, value, **kwargs):
        
        return column.value_counts()[value] / len(column)
    

# This class defines the Expectation itself
class ExpectColumnValueToMeetFrequency(ColumnAggregateExpectation):
    """This expectation checks if value specified in a column meets or exceeds the threshold defined."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
    {
        "data": {"x": ['v1', 'v1', 'v2', 'v3', 'v4'], "y": [0, -1, -2, 4, None]},
        "only_for": ["pandas"],
        "tests": [
            {
                "title": "basic_positive_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "x",
                    "value": 'v1',
                    "threshold": 0.1
                },
                "out": {"success": True},
            },
            {
                "title": "basic_negative_test",
                "exact_match_out": False,
                "include_in_gallery": True,
                "in": {
                    "column": "x",
                    "value": 'v1',
                    "threshold": 0.8
                },
                "out": {"success": False},
            },
        ],
    }
]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.value_meets_frequency",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ('value', 'threshold',) 

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


    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        threshold = configuration['kwargs'].get('threshold')
        value_frequency = metrics.get('column.value_meets_frequency')
        success = value_frequency >= threshold

        return {"success": success, "result": {"observed_value": value_frequency } }
        

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ['Frequency','Threshold','Value Counts','Count'],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@HaebichanGX",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValueToMeetFrequency().print_diagnostic_checklist()