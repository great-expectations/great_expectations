from typing import Union

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)
from great_expectations.util import convert_to_json_serializable


class ExpectQueriedColumnListToBeUnique(QueryExpectation):
    """Expect multiple columns (such as a compound key) to be unique.

    Args:
        template_dict: dict with the following keys: \
            column_list (columns to check uniqueness on separated by comma)
    """

    metric_dependencies = ("query.template_values",)
    query = """
            SELECT COUNT(1) FROM (
            SELECT {column_list}, COUNT(1)
            FROM {batch}
            GROUP BY {column_list}
            HAVING count(1) > 1
            )
            """

    success_keys = ("template_dict" "query",)

    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
        "row_condition",
        "condition_parser",
    )
    default_kwarg_values = {
        "catch_exceptions": False,
        "meta": None,
        "query": query,
    }

    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metrics = convert_to_json_serializable(data=metrics)
        num_of_duplicates = list(metrics.get("query.template_values")[0].values())[0]

        if not num_of_duplicates:
            return {
                "info": "The columns are unique - no duplicates found",
                "success": True,
            }

        else:
            return {
                "success": False,
                "result": {
                    "info": f"{num_of_duplicates} Duplicated keys found",
                    "observed_value": num_of_duplicates,
                },
            }

    examples = [
        {
            "data": [
                {
                    "data": {
                        "unique_num": [1, 2, 3, 4, 5, 6],
                        "unique_str": ["a", "b", "c", "d", "e", "f"],
                        "unique_str2": ["a", "b", "c", "d", "e", "f"],
                        "duplicate_num": [1, 1, 1, 1, 1, 1],
                        "duplicate_str": ["a", "a", "b", "c", "d", "e"],
                        "duplicate_str2": ["a", "a", "b", "c", "d", "e"],
                    },
                },
            ],
            "only_for": ["spark", "sqlite", "bigquery", "trino", "redshift"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column_list": "unique_num,unique_str,unique_str2",
                        }
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column_list": "duplicate_num,duplicate_str,duplicate_str2",
                            "row_condition": "1=1",
                            "condition_parser": "great_expectations",
                        }
                    },
                    "out": {"success": False},
                },
                {
                    "title": "passing_condition_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "column_list": "unique_num,unique_str,duplicate_str2",
                            "row_condition": 'col("duplicate_str2")!="a"',
                            "condition_parser": "great_expectations",
                        }
                    },
                    "out": {"success": True},
                },
            ],
        }
    ]

    library_metadata = {
        "tags": ["query-based"],
        "contributors": ["@itaise", "@maayaniti"],
    }


if __name__ == "__main__":
    ExpectQueriedColumnListToBeUnique().print_diagnostic_checklist()
