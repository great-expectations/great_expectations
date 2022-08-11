"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedPairColumnValuesToHaveDiff(QueryExpectation):
    """Expect the frequency of occurrences of a specified value in a queried column to be at least <threshold> percent of values in that column."""

    metric_dependencies = ("query.pair_column",)

    query = """
            SELECT {column_A} - {column_B} as diff
            FROM {active_batch}
            """

    success_keys = ("column_A", "column_B", "diff", "mostly", "query", "strict")

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column_A": None,
        "column_B": None,
        "diff": 0,
        "mostly": 1,
        "strict": True,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        diff = configuration["kwargs"].get("diff")
        mostly = configuration["kwargs"].get("mostly")

        try:
            assert diff is not None, "'diff' must be specified"
            assert isinstance(diff, (int, float)), "`diff` must be a valid float or int"
            assert (
                isinstance(mostly, (int, float)) and 0 < mostly <= 1
            ), "'mostly' must be 1, a float between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:

        diff = configuration["kwargs"].get("diff")
        mostly = configuration["kwargs"].get("mostly")
        strict = configuration["kwargs"].get("strict")
        query_result = metrics.get("query.pair_column")

        if mostly == 1:
            success = all([(abs(x[0]) == diff) for x in query_result])
        elif mostly != 1 and strict is True:
            success = (
                sum([(abs(x[0]) == diff) for x in query_result]) / len(query_result)
            ) > mostly
        else:
            success = (
                sum([(abs(x[0]) == diff) for x in query_result]) / len(query_result)
            ) >= mostly

        return {
            "success": success,
            "result": {"observed_value": [x[0] for x in query_result]},
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": [2, 3, 3, 4, 2],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "col1",
                        "column_B": "col2",
                        "diff": 1,
                        "mostly": 0.6,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column_A": "col1",
                        "column_B": "col2",
                        "diff": 2,
                        "mostly": 0.4,
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@joegargery"],
    }


if __name__ == "__main__":
    ExpectQueriedPairColumnValuesToHaveDiff().print_diagnostic_checklist()
