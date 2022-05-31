"""
This is a template for creating custom Parameterized TableQueryExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_query_expectations
"""

from typing import Dict, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    TableQueryExpectation,
)


class ExpectParameterizedQueriedColumnValueFrequencyToMeetThreshold(
    TableQueryExpectation
):
    """Expect the frequency of occurrences of a specified <value> in a queried column to be at least <threshold> percent of values in that column."""

    default_kwarg_values = {
        "query": "SELECT col2, "
        "CAST(COUNT(col2) AS float) / (SELECT COUNT(col2) FROM test) "
        "FROM test "
        "GROUP BY col2",
        "value": "a",
        "threshold": 0.4,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:

        value = configuration["kwargs"].get("value") or self.default_kwarg_values.get(
            "value"
        )
        threshold = configuration["kwargs"].get(
            "threshold"
        ) or self.default_kwarg_values.get("threshold")
        query_result = metrics.get("table.query")
        query_result = dict(query_result)

        success = query_result[value] >= threshold

        return {
            "success": success,
            "result": {"observed_value": query_result[value]},
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": ["a", "a", "b", "b", "a"],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {},
                    "out": {"success": True},
                    "only_for": ["postgresql"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"threshold": 1},
                    "out": {"success": False},
                    "only_for": ["postgresql"],
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based", "parameterized"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectParameterizedQueriedColumnValueFrequencyToMeetThreshold().print_diagnostic_checklist()

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = (
    ExpectParameterizedQueriedColumnValueFrequencyToMeetThreshold().run_diagnostics()
)

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
