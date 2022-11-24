from typing import Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectQueriedColumnValuesToExistInSecondTableColumn(QueryExpectation):
    """Expect all values in a specific column to exist in another table's column.
    Args:
        template_dict: a dictionary containing the following keys:
             "first_table_column" - name of the main table column
             "second_table_column" - name of the column to compare to, in the second table
             "second_table_full_name"
             "condition": additional condition added in the where clause, provide "1=1" if not needed.
    """

    library_metadata = {
        "tags": [
            "query-based",
        ],
        "contributors": ["@itaise"],
    }
    metric_dependencies = ("query.template_values",)

    query = """
    select count(1) from (
    SELECT a.{first_table_column}
                    FROM {active_batch} a
                    LEFT JOIN {second_table_full_name} b
                    ON a.{first_table_column}=b.{second_table_column}
                    WHERE b.{second_table_column} IS NULL
                    and {condition}
                    GROUP BY 1
    )
    """

    success_keys = ("template_dict", "query")
    domain_keys = (
        "query",
        "template_dict",
        "batch_id",
    )

    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "query": query,
    }

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ) -> Union[ExpectationValidationResult, dict]:
        success = False

        query_result = metrics.get("query.template_values")
        num_of_missing_rows = query_result[0][0]

        if num_of_missing_rows == 0:
            success = True

        return {
            "success": success,
            "result": {
                "Rows with IDs in first table missing in second table": num_of_missing_rows
            },
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "test",
                    "data": {
                        "msid": ["aaa", "bbb"],
                    },
                },
                {
                    "dataset_name": "test_2",
                    "data": {
                        "msid": ["aaa", "aaa"],
                    },
                },
                {
                    "dataset_name": "test_3",
                    "data": {
                        "msid": [
                            "aaa",
                            "aaa",
                            "aaa",
                            "bbb",
                        ],
                        "date_created": [
                            "2022-02-02",
                            "2022-02-02",
                            "2022-02-02",
                            "2022-02-02",
                        ],
                    },
                },
            ],
            "tests": [
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "second_table_full_name": "test_2",
                            "first_table_column": "msid",
                            "second_table_column": "msid",
                            "condition": "1=1",
                        },
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite"],
                },
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "second_table_full_name": "test_3",
                            "first_table_column": "msid",
                            "second_table_column": "msid",
                            "condition": "date_created > date('2022-01-01')",
                        }
                    },
                    "out": {"success": True},
                    "only_for": ["sqlite"],
                },
            ],
            "test_backends": [
                {
                    "backend": "sqlalchemy",
                    "dialects": ["sqlite"],
                }
            ],
        },
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)


if __name__ == "__main__":
    ExpectQueriedColumnValuesToExistInSecondTableColumn().print_diagnostic_checklist()
