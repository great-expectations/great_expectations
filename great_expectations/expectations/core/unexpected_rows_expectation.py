from __future__ import annotations

import logging
from string import Formatter
from typing import TYPE_CHECKING, ClassVar, Optional, Tuple, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core._docs_decorators import public_api
from great_expectations.expectations.expectation import BatchExpectation
from great_expectations.render.renderer_configuration import (
    CodeBlock,
    CodeBlockLanguage,
    RendererConfiguration,
    RendererValueType,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine


logger = logging.getLogger(__name__)


EXPECTATION_SHORT_DESCRIPTION = (
    "This Expectation will fail validation if the query returns one or more rows. "
    "The WHERE clause defines the fail criteria."
)
UNEXPECTED_ROWS_QUERY_DESCRIPTION = (
    "A SQL or Spark-SQL query to be executed for validation."
)
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

    metric_dependencies: ClassVar[Tuple[str, ...]] = ("unexpected_rows_query.table",)
    success_keys: ClassVar[Tuple[str, ...]] = ("unexpected_rows_query",)
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )
    default_kwarg_values = {
        "unexpected_rows_query": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("unexpected_rows_query",)

    @override
    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration for the Expectation.

        For this expectation, `configuraton.kwargs` may contain `min_value` and `max_value` whose value is either
        a number or date.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation

        Raises:
            InvalidExpectationConfigurationError: if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        if configuration:
            parsed_fields = [
                f[1]
                for f in Formatter().parse(  # type: ignore[type-var]
                    configuration.kwargs.get("unexpected_rows_query")
                )
            ]
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
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
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
