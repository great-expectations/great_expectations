from typing import Any, Dict, List, Optional, Union

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectAllIdsInFirstTableExistInSecondTableCustom(QueryExpectation):
    """Expect the frequency of occurrences of a specified value in a queried column to be at least <threshold>
    percent of values in that column."""

    metric_dependencies = ("query.template_values",)

    query = """
    select count(1) from (
    SELECT a.{first_table_id_column}
                    FROM {active_batch} a
                    LEFT JOIN {second_table_full_name} b
                    ON (a.{first_table_id_column}=b.{second_table_id_column})
                    WHERE b.{second_table_id_column} IS NULL
                    GROUP BY 1
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
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "column": None,
        "value": "dummy_value",
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
                        "msid": [
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "bbb",
                        ],
                    },
                },
                {
                    "dataset_name": "test_2",
                    "data": {
                        "msid": [
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
                            "aaa",
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
                            "first_table_id_column": "msid",
                            "second_table_id_column": "msid",
                        }
                    },
                    "out": {"success": False},
                    "only_for": ["sqlite", "spark"],
                }
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
    ExpectAllIdsInFirstTableExistInSecondTableCustom().print_diagnostic_checklist()
