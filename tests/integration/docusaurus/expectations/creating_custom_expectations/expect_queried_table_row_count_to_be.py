"""
This is an example of a Custom QueryExpectation.
For detailed information on QueryExpectations, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations
"""

from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions.exceptions import (
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


# <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py ExpectQueriedTableRowCountToBe class_def">
class ExpectQueriedTableRowCountToBe(QueryExpectation):
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py docstring">
    """Expect the expect the number of rows returned from a queried table to equal a specified value."""
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py metric_dependencies">
    metric_dependencies = ("query.table",)
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py query">
    query = """
            SELECT COUNT(*)
            FROM {active_batch}
            """
    # </snippet>
    # <snippet name="tests/integration/docusaurus/expectations/creating_custom_expectations/expect_queried_table_row_count_to_be.py success_keys">
    success_keys = (
        "value",
        "query",
    )
    # </snippet>

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "value": None,
        "query": query,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        super().validate_configuration(configuration)
        value = configuration["kwargs"].get("value")

        try:
            assert value is not None, "'value' must be specified"
            assert (
                isinstance(value, int) and value >= 0
            ), "`value` must be an integer greater than or equal to zero"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # <snippet name="expect_queried_table_row_count_to_be.py _validate function">
    # <snippet name="expect_queried_table_row_count_to_be.py _validate function signature">
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        # </snippet>
        metrics = convert_to_json_serializable(data=metrics)
        query_result = list(metrics.get("query.table")[0].values())[0]
        value = configuration["kwargs"].get("value")

        success = query_result == value

        return {
            "success": success,
            "result": {"observed_value": query_result},
        }

    # </snippet>
    # <snippet name="expect_queried_table_row_count_to_be.py examples">
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
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                },
                {
                    "title": "positive_test_static_data_asset",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 5,
                        "query": """
                                 SELECT COUNT(*)
                                 FROM test
                                 """,
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "positive_test_row_condition",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "value": 2,
                        "row_condition": 'col("col1")==2',
                        "condition_parser": "great_expectations__experimental__",
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite", "spark"],
                },
            ],
        },
    ]
    # </snippet>
    # This dictionary contains metadata for display in the public gallery
    # <snippet name="expect_queried_table_row_count_to_be.py library_metadata">
    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@joegargery"],
    }
    # </snippet>


if __name__ == "__main__":
    # <snippet name="expect_queried_table_row_count_to_be.py print_diagnostic_checklist">
    ExpectQueriedTableRowCountToBe().print_diagnostic_checklist()
    # </snippet>

# Note to users: code below this line is only for integration testing -- ignore!

diagnostics = ExpectQueriedTableRowCountToBe().run_diagnostics()

for check in diagnostics["tests"]:
    assert check["test_passed"] is True
    assert check["error_diagnostics"] is None

for check in diagnostics["errors"]:
    assert check is None

for check in diagnostics["maturity_checklist"]["experimental"]:
    if check["message"] == "Passes all linting checks":
        continue
    assert check["passed"] is True
