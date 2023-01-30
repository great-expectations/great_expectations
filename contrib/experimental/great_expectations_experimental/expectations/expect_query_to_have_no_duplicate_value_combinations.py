"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""


from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueryToHaveNoDuplicateValueCombinations(QueryExpectation):
    """Expect the data points given primary keys via columns to be unique"""

    metric_dependencies = ("query.multiple_columns",)

    query = """
                SELECT {col_1}, {col_2}, COUNT(*) n
                FROM {active_batch}
                GROUP BY {col_1}, {col_2}
                HAVING n > 1
            """

    success_keys = ("query", "columns")

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "columns": None,
        "query": query,
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
        query_result = metrics.get("query.multiple_columns")
        query_result = dict([element.values() for element in query_result])

        columns = configuration["kwargs"].get("columns")
        duplicates = [
            dict(zip(columns + ["no_occurrences"], row)) for row in query_result
        ]

        return {
            "success": not query_result,
            "result": {"observed_value": duplicates},
        }

    examples = [
        {
            "data": {
                "one": ["a", "a", "b"],
                "two": ["x", "x", "y"],
                "three": ["j", "k", "l"],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "columns": ["one", "three"],
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "columns": ["one", "two"],
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
            ],
        },
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": [
            "@CarstenFrommhold",
        ],
    }


if __name__ == "__main__":
    ExpectQueryToHaveNoDuplicateValueCombinations().print_diagnostic_checklist()
