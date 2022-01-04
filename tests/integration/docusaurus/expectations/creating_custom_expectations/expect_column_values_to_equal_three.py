import json

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesEqualThree(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.equal_three"

    # Description needed
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3


# This class defines the Expectation itself
class ExpectColumnValuesToEqualThree(ColumnMapExpectation):
    """TODO: Add a docstring here"""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "all_threes": [3, 3, 3, 3, 3, 3, 3, 3, 3, 3],
                "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_threes", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                }
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.equal_three"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys
    # for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            #         "@your_name_here", # Don't forget to add your github handle here!
        ],
        # "package": "experimental_expectations", # This should be auto-populated.
    }


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesToEqualThree().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
