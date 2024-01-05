from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Optional, Tuple, Union

import pandas as pd

from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.expectation import (
    ExpectationValidationResult,
    QueryExpectation,
)
from great_expectations.render import (
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.renderer.renderer import renderer

if TYPE_CHECKING:
    from great_expectations.core import ExpectationConfiguration
    from great_expectations.execution_engine import ExecutionEngine

from great_expectations.compatibility.pydantic import (
    root_validator,
)


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

    Args:
        foreign_key_column: foreign key column of current table that we want to validate.
        foreign_table: foreign table name.
        foreign_table_key_column: key column in foreign table.
    """

    metric_dependencies = ("query.template_values",)

    foreign_key_column: str
    foreign_table: str
    foreign_table_key_column: str

    template_dict: dict = {}

    query: str = """
            SELECT a.{foreign_key_column}
            FROM {active_batch} a
            LEFT JOIN {foreign_table} b
                ON a.{foreign_key_column} = b.{foreign_table_key_column}
            WHERE b.{foreign_table_key_column} IS NULL
            """

    library_metadata: ClassVar[dict] = {
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

    success_keys: ClassVar[Tuple[str, ...]] = (
        "template_dict",
        "query",
    )
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "query",
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    examples: ClassVar[List[dict]] = [
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
                        "foreign_key_column": "CUSTOMER_ID",
                        "foreign_table": "customer_table",
                        "foreign_table_key_column": "CUSTOMER_ID",
                    },
                    "out": {
                        "success": True,
                        "result": {
                            "observed_value": "0 missing values.",
                            "unexpected_index_list": [],
                        },
                    },
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
                        "foreign_key_column": "CUSTOMER_ID",
                        "foreign_table": "customer_table",
                        "foreign_table_key_column": "CUSTOMER_ID",
                    },
                    "out": {
                        "success": False,
                        "result": {
                            "observed_value": "2 missing values.",
                            "unexpected_count": 2,
                            "unexpected_index_list": [
                                {"customer_id": "5"},
                                {"customer_id": "6"},
                            ],
                        },
                    },
                },
            ],
        },
    ]

    @root_validator
    def _validate_template_dict(cls, values):
        template_dict: dict = {
            "foreign_key_column": values.get("foreign_key_column"),
            "foreign_table": values.get("foreign_table"),
            "foreign_table_key_column": values.get("foreign_table_key_column"),
        }
        values["template_dict"] = template_dict
        return values

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

        foreign_key_column: str = configuration.kwargs.get("foreign_key_column")
        foreign_table: str = configuration.kwargs.get("foreign_table")
        foreign_table_key_column: str = configuration.kwargs.get(
            "foreign_table_key_column"
        )

        template_str = "All values in column $foreign_key_column are present in column $foreign_table_key_column of table $foreign_table."

        params = {
            "foreign_key_column": foreign_key_column,
            "foreign_table": foreign_table,
            "foreign_table_key_column": foreign_table_key_column,
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

    @classmethod
    @override
    @renderer(renderer_type="renderer.diagnostic.unexpected_table")
    def _diagnostic_unexpected_table_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        if result is None:
            return None

        result_dict: Optional[dict] = result.result

        if result_dict is None:
            return None

        unexpected_index_list: Optional[List[dict]] = result_dict.get(
            "unexpected_index_list"
        )
        # Don't render table if we don't have unexpected_values
        if not unexpected_index_list:
            return None

        unexpected_index_df: pd.DataFrame = pd.DataFrame(
            unexpected_index_list, dtype="string"
        )

        # extract column name from unexpected values
        column_name: str = list(unexpected_index_list[0].keys())[0].upper()
        header_row = [f"Missing Values for {column_name} Column"]

        row_list = []
        for index, row in unexpected_index_df.iterrows():
            unexpected_value = row
            row_list.append(unexpected_value)

        unexpected_table_content_block = RenderedTableContent(
            content_block_type="table",
            table=row_list,  # type: ignore[arg-type]
            header_row=header_row,  # type: ignore[arg-type]
            styling={"body": {"classes": ["table-bordered", "table-sm", "mt-3"]}},
        )
        return [unexpected_table_content_block]

    def _validate(
        self,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> Union[ExpectationValidationResult, dict]:
        unexpected_values = metrics.get("query.template_values")
        final_value = len(unexpected_values)

        return ExpectationValidationResult(
            success=(final_value == 0),
            result={
                "observed_value": f"{final_value} missing value{'s' if final_value != 1 else ''}.",
                "unexpected_index_list": unexpected_values,
            },
        )
