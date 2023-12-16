from typing import Optional, Union

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)


class ExpectColumnValuesToBePresentInAnotherTable(QueryExpectation):
    """Expect the values in a column to be present in another table.

     This is an Expectation that allows for the validation of referential integrity, that a foreign key exists in another table.

     In the following example, order table has a foreign key to customer table, and referential integrity is preserved, because all of
     the values of CUSTOMER_ID in order_table_1 are present in the CUSTOMER_ID column of customer_table.

    "order_table_1": {
         "ORDER_ID": ["aaa", "bbb", "ccc"],
         "CUSTOMER_ID": [1, 1, 3],
     }
     "customer_table": {
         "CUSTOMER_ID": [1, 2, 3],

     }

     However, in the second example, referential integrity is not preserved, because there are two values (4 and 5) in the CUSTOMER_ID
     column of order_table_2 that are not present in the CUSTOMER_ID column of customer_table.

     "order_table_2": {
         "ORDER_ID": ["ddd", "eee", "fff"],
         "CUSTOMER_ID": [1, 4, 5],
     }
     "customer_table": {
         "CUSTOMER_ID": [1, 2, 3],

     }
     ExpectColumnValuesToBePresentInAnotherTable will PASS for example 1 and FAIL for example 2.

     Args:
         template_dict: dict containing the following keys:
             foreign_key_column: foreign key-column of current table that we want to validate.
             foreign_table: foreign table.
             primary_key_column_in_foreign_table: key column for primary key in foreign table.
    """

    library_metadata = {
        "maturity": "experimental",
        "tags": ["table expectation", "multi-table expectation", "query-based"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [
            "sqlalchemy",
            "snowflake-sqlalchemy",
            "snowflake-connector-python",
        ],
        "has_full_test_suite": False,
        "manually_reviewed_code": True,
    }

    metric_dependencies = ("query.template_values",)
    template_dict: dict
    query = """
        SELECT count(1) FROM (
        SELECT a.{foreign_key_column}
        FROM {active_batch} a
        LEFT JOIN {foreign_table} b
            ON a.{foreign_key_column} = b.{primary_key_column_in_foreign_table}
        WHERE b.{primary_key_column_in_foreign_table} is NULL)
        """
    success_keys = (
        "template_dict",
        "query",
    )
    domain_keys = ("query", "batch_id", "row_condition", "condition_parser")

    default_kwarg_values = {
        "catch_exceptions": False,
        "meta": None,
        "query": query,
    }

    def _validate_template_dict(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        template_dict = configuration.kwargs.get("template_dict")
        if not isinstance(template_dict, dict):
            raise TypeError("template_dict must be supplied as a dict")
        if not all(
            [
                "foreign_key_column" in template_dict,
                "foreign_table" in template_dict,
                "primary_key_column_in_foreign_table" in template_dict,
            ]
        ):
            raise KeyError(
                "The following keys have to be in the template dict: id, foreign_table, foreign_key_column"
            )

    def _validate(
        self,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        configuration = self.configuration
        self._validate_template_dict(configuration)
        as_dict_as_template = metrics.get("query.template_values")[0]
        final_value = list(as_dict_as_template.values())[0]
        return {
            "success": final_value == 0,
            "result": {
                "Rows with IDs that are not present in foreign table": final_value
            },
        }

    examples = [
        {
            "data": [
                {
                    "dataset_name": "order_table_1",
                    "data": {
                        "ORDER_ID": ["aaa", "bbb", "ccc"],
                        "CUSTOMER_ID": [1, 1, 3],
                    },
                },
                {
                    "dataset_name": "customer_table",
                    "data": {
                        "CUSTOMER_ID": [1, 2, 3],
                    },
                },
            ],
            "only_for": ["snowflake", "sqlite"],
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "foreign_key_column": "CUSTOMER_ID",
                            "foreign_table": "customer_table",
                            "primary_key_column_in_foreign_table": "CUSTOMER_ID",
                        }
                    },
                    "out": {"success": True},
                },
            ],
        },
        {
            "data": [
                {
                    "dataset_name": "order_table_2",
                    "data": {
                        "ORDER_ID": ["aaa", "bbb", "ccc"],
                        "CUSTOMER_ID": [1, 5, 6],
                    },
                },
                {
                    "dataset_name": "customer_table",
                    "data": {
                        "CUSTOMER_ID": [1, 2, 3],
                    },
                },
            ],
            "only_for": ["snowflake", "sqlite"],
            "tests": [
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "template_dict": {
                            "foreign_key_column": "CUSTOMER_ID",
                            "foreign_table": "customer_table",
                            "primary_key_column_in_foreign_table": "CUSTOMER_ID",
                        },
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]


if __name__ == "__main__":
    ExpectColumnValuesToBePresentInAnotherTable().print_diagnostic_checklist()
