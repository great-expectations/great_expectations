from typing import Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedColumnToBeUniqueWithCondition(QueryExpectation):
    """Expect column values to be unique, with an filter.
    args:
    *****
    template_dict: dict with the following keys:
    column_to_check - column to check uniqueness on. can be multiple column names separated by comma
    condition - the filter - for boolean column, you can provide just the column name (evaluated to True)"""

    metric_dependencies = ("query.template_values",)

    query = """
            SELECT {column_to_check}, COUNT(1)
            FROM {active_batch}
            WHERE {condition}
            GROUP BY {column_to_check}
            HAVING count(1) > 1
            """

    success_keys = (
        "template_dict"
        "query",
    )

    domain_keys = ("query", "template_dict", "batch_id", "row_condition", "condition_parser")
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "query": query,
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:

        query_result = metrics.get("query.template_values")
        query_result = dict(query_result)
        if not query_result:
            return {"success": True}

        else:
            return {
                "success": False,
                "result": {"observed_value": query_result},
            }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "uuid": [1, 2, 2, 3, 4, 4],
                        "is_open": [True, False, True, True, True, True],
                        "is_open_2": [False, True, False, False, False, True],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_to_check": "uuid", "condition": "is_open_2"}},
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"template_dict": {"column_to_check": "uuid", "condition": "is_open"}},
                    "out": {"success": False},
                    "only_for": ["sqlite"],
                }
            ],
        }
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@itaise"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnToBeUniqueWithCondition().print_diagnostic_checklist()
