from __future__ import annotations

import logging
from string import Formatter
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Tuple, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.render.renderer_configuration import (
    CodeBlock,
    CodeBlockLanguage,
    RendererConfiguration,
    RendererValueType,
)

if TYPE_CHECKING:
    from great_expectations.core import ExpectationValidationResult
    from great_expectations.execution_engine import ExecutionEngine


logger = logging.getLogger(__name__)


EXPECTATION_SHORT_DESCRIPTION = (
    "This Expectation will fail validation if the query returns one or more rows. "
    "The WHERE clause defines the fail criteria."
)
UNEXPECTED_ROWS_QUERY_DESCRIPTION = "A SQL or Spark-SQL query to be executed for validation."
SUPPORTED_DATA_SOURCES = [
    "PostgreSQL",
    "Snowflake",
    "SQLite",
]


class UnexpectedRowsExpectation(BatchExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION }

    UnexpectedRowsExpectations facilitate the execution of SQL or Spark-SQL queries \
    as the core logic for an Expectation. UnexpectedRowsExpectations must implement \
    a `_validate(...)` method containing logic for determining whether data returned \
    by the executed query is successfully validated. One is written by default, but \
    can be overridden.

    A successful validation is one where the unexpected_rows_query returns no rows.

    UnexpectedRowsExpectation is a \
    [Batch Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations).

    BatchExpectations are one of the most common types of Expectation.
    They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        unexpected_rows_query (str): {UNEXPECTED_ROWS_QUERY_DESCRIPTION}

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
    """

    unexpected_rows_query: str = pydantic.Field(description=UNEXPECTED_ROWS_QUERY_DESCRIPTION)

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
            batch_warning_message = (
                "unexpected_rows_query should contain the {batch} parameter. "
                "Otherwise data outside the configured batch will be queried."
            )
            # instead of raising a disruptive warning, we print and log info
            # in order to make the user aware of the potential for querying
            # data outside the configured batch
            print(batch_warning_message)
            logger.info(batch_warning_message)

        return query

    class Config:
        title = "Custom Expectation with SQL"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[UnexpectedRowsExpectation]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "data_quality_issues": {
                        "title": "Data Quality Issues",
                        "type": "array",
                        "const": [],
                    },
                    "short_description": {
                        "title": "Short Description",
                        "type": "string",
                        "const": EXPECTATION_SHORT_DESCRIPTION,
                    },
                    "supported_data_sources": {
                        "title": "Supported Data Sources",
                        "type": "array",
                        "const": SUPPORTED_DATA_SOURCES,
                    },
                }
            )

    @classmethod
    @override
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        renderer_configuration.add_param(
            name="unexpected_rows_query", param_type=RendererValueType.STRING
        )
        renderer_configuration.code_block = CodeBlock(
            code_template_str="$unexpected_rows_query",
            language=CodeBlockLanguage.SQL,
        )
        return renderer_configuration

    @override
    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> Union[ExpectationValidationResult, dict]:
        metric_value = metrics["unexpected_rows_query.table"]
        unexpected_row_count = len(metric_value)
        observed_value = f"{unexpected_row_count} unexpected "
        if unexpected_row_count == 1:
            observed_value += "row"
        else:
            observed_value += "rows"
        return {
            "success": unexpected_row_count == 0,
            "result": {
                "observed_value": observed_value,
                "details": {"unexpected_rows": metric_value},
            },
        }
