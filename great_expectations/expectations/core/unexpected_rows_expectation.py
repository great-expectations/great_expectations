from __future__ import annotations

from string import Formatter
from typing import TYPE_CHECKING, ClassVar, Tuple, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.expectation import BatchExpectation

if TYPE_CHECKING:
    from great_expectations.core import ExpectationValidationResult
    from great_expectations.execution_engine import ExecutionEngine


class UnexpectedRowsExpectation(BatchExpectation):
    """
    UnexpectedRowsExpectations facilitate the execution of SQL or Spark-SQL queries as the core logic for an Expectation.

    UnexpectedRowsExpectations must implement a `_validate(...)` method containing logic for determining whether data returned by the executed query is successfully validated.
    One is written by default, but can be overridden.
    A successful validation is one where the unexpected_rows_query returns no rows.

    Args:
        unexpected_rows_query (str): A SQL or Spark-SQL query to be executed for validation.
    """  # noqa: E501

    unexpected_rows_query: str

    metric_dependencies: ClassVar[Tuple[str, ...]] = ("unexpected_rows_query.table",)
    success_keys: ClassVar[Tuple[str, ...]] = ("unexpected_rows_query",)
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    @pydantic.validator("unexpected_rows_query")
    def _validate_query(cls, query: str) -> str:
        parsed_fields = [f[1] for f in Formatter().parse(query)]
        if "batch" not in parsed_fields:
            raise ValueError("Query must contain {batch} parameter.")  # noqa: TRY003

        return query

    @override
    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metric_value = metrics["unexpected_rows_query.table"]
        return {
            "success": len(metric_value) == 0,
            "result": {
                "observed_value": len(metric_value),
                "details": {"unexpected_rows": metric_value},
            },
        }
