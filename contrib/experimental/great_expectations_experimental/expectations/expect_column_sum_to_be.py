"""
This is a template for creating custom ColumnAggregateExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
"""

from typing import Dict

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import ColumnAggregateExpectation


# This class defines the Expectation itself
class ExpectColumnSumToBe(ColumnAggregateExpectation):
    """Expect the sum of a column to be exactly a value."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {"a": [1, 2, 3, 4, 5]},
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "sum_total": 15},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "sum_total": 14},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.sum",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("sum_total",)

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        actual_value = metrics["column.sum"]
        predicted_value = self._get_success_kwargs().get("sum_total")
        success = actual_value == predicted_value
        return {"success": success, "result": {"observed_value": actual_value}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "column aggregate expectation",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@joshua-stauffer",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnSumToBe().print_diagnostic_checklist()
