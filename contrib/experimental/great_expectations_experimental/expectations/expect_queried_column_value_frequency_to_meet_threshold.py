"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import List, Optional, Union

from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


class ExpectQueriedColumnValueFrequencyToMeetThreshold(QueryExpectation):
    """Expect the frequency of occurrences of a specified value in a queried column to be at least <threshold> percent of values in that column."""

    column: str
    threshold: Union[float, List[float]]
    value: Union[str, List[str]]

    metric_dependencies = ("query.column",)

    query = """
            SELECT {col},
            CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM {batch})
            FROM {batch}
            GROUP BY {col}
            """

    success_keys = (
        "column",
        "value",
        "threshold",
        "query",
    )

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "value": None,
        "threshold": 1,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        value = configuration["kwargs"].get("value")
        threshold = configuration["kwargs"].get("threshold")

        try:
            assert value is not None, "'value' must be specified"
            assert (
                (isinstance(threshold, (int, float)) and 0 < threshold <= 1)
                or (
                    isinstance(threshold, list)
                    and all(isinstance(x, (int, float)) for x in threshold)
                    and all(0 < x <= 1 for x in threshold)
                    and 0 < sum(threshold) <= 1
                )
            ), "'threshold' must be 1, a float between 0 and 1, or a list of floats whose sum is between 0 and 1"
            if isinstance(threshold, list):
                assert isinstance(value, list) and len(value) == len(
                    threshold
                ), "'value' and 'threshold' must contain the same number of arguments"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        configuration = self.configuration
        value = configuration["kwargs"].get("value")
        threshold = configuration["kwargs"].get("threshold")
        query_result = metrics.get("query.column")
        query_result = dict([element.values() for element in query_result])

        if isinstance(value, list):
            success = all(query_result[value[i]] >= threshold[i] for i in range(len(value)))

            return {
                "success": success,
                "result": {"observed_value": [query_result[value[i]] for i in range(len(value))]},
            }

        success = query_result[value] >= threshold

        return {
            "success": success,
            "result": {"observed_value": query_result[value]},
        }

    examples = [
        {
            "data": [
                {
                    "data": {
                        "col1": [1, 2, 2, 3, 4],
                        "col2": ["a", "a", "b", "b", "a"],
                    },
                },
            ],
            "suppress_test_for": ["bigquery", "trino", "snowflake"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value": "a",
                        "threshold": 0.6,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col1",
                        "value": 2,
                        "threshold": 1,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "multi_value_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value": ["a", "b"],
                        "threshold": [0.6, 0.4],
                    },
                    "out": {"success": True},
                },
                {
                    "title": "multi_value_positive_test_static_data_asset",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value": ["a", "b"],
                        "threshold": [0.6, 0.4],
                        "query": """
                                 SELECT {col},
                                 CAST(COUNT({col}) AS float) / (SELECT COUNT({col}) FROM test)
                                 FROM test
                                 GROUP BY {col}
                                 """,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "multi_value_positive_test_row_condition",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "col2",
                        "value": ["a", "b"],
                        "threshold": [0.5, 0.5],
                        "row_condition": 'col("col1")==2',
                        "condition_parser": "great_expectations",
                    },
                    "out": {"success": True},
                },
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@austiezr", "@mkopec87"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnValueFrequencyToMeetThreshold().print_diagnostic_checklist()
