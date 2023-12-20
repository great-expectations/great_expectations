from typing import List, Optional, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import (
    ExpectationConfiguration,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer


class ExpectColumnValuesToBePresentInAnotherTable(QueryExpectation):
    """Expect the values in a column to be present in another table.

    This is an Expectation that allows for the validation of referential integrity, that a foreign key exists in
    another table.

    In the following example, order table has a foreign key to customer table, and referential integrity is preserved,
    because all the values of CUSTOMER_ID in order_table_1 are present in the CUSTOMER_ID column of customer_table.

    "order_table_1": {
         "ORDER_ID": ["aaa", "bbb", "ccc"],
         "CUSTOMER_ID": [1, 1, 3],
    }
    "customer_table": {
        "CUSTOMER_ID": [1, 2, 3],

    }

    However, in the second example, referential integrity is not preserved, because there are two values (4 and 5) in
    the CUSTOMER_ID column of order_table_2 that are not present in the CUSTOMER_ID column of customer_table.

     "order_table_2": {
         "ORDER_ID": ["ddd", "eee", "fff"],
         "CUSTOMER_ID": [1, 4, 5],
     }
     "customer_table": {
         "CUSTOMER_ID": [1, 2, 3],

     }
    ExpectColumnValuesToBePresentInAnotherTable will PASS for example 1 and FAIL for example 2.

    Also, the `template_dict` parameter we would use for the Expectation on `order_table_1` and `order_table_2` would
    look like the following:

    "template_dict" = {
        "foreign_key_column": "CUSTOMER_ID",
        "foreign_table": "customer_table",
        "primary_key_column_in_foreign_table": "CUSTOMER_ID",
    }

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
        SELECT COUNT(1) FROM (
        SELECT a.{foreign_key_column}
        FROM {active_batch} a
        LEFT JOIN {foreign_table} b
            ON a.{foreign_key_column} = b.{primary_key_column_in_foreign_table}
        WHERE b.{primary_key_column_in_foreign_table} IS NULL)
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

    @classmethod
    @override
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")

        template_dict = configuration.kwargs.get("template_dict")

        template_str = "All values in column $foreign_key_column are present in column $primary_key_column_in_foreign_table of  table $foreign_table. (Observed Value is the number of missing values)."

        params = {
            "foreign_key_column": template_dict["foreign_key_column"],
            "foreign_table": template_dict["foreign_table"],
            "primary_key_column_in_foreign_table": template_dict[
                "primary_key_column_in_foreign_table"
            ],
        }

        return [
            RenderedStringTemplateContent(
                content_block_type="string_template",
                string_template={
                    "template": template_str,
                    "params": params,
                    "styling": styling,
                },
            )
        ]

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
                f"The following keys are missing from the template dict: "
                f"{'foreign_key_column ' if 'foreign_key_column' not in template_dict else ''} "
                f"{'foreign_table ' if 'foreign_table' not in template_dict else ''} "
                f"{'primary_key_column_in_foreign_table ' if 'primary_key_column_in_foreign_table' not in template_dict else ''}"
            )

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        configuration = self.configuration
        self._validate_template_dict(configuration)
        final_value = metrics.get("query.template_values")[0]["COUNT(1)"]
        return ExpectationValidationResult(
            success=(final_value == 0), result={"observed_value": final_value}
        )

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
                    "out": {"success": False, "observed_value": 2},
                },
            ],
        },
    ]


if __name__ == "__main__":
    ExpectColumnValuesToBePresentInAnotherTable().print_diagnostic_checklist()
