from __future__ import annotations

import logging
from string import Formatter
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # FIXME CoP
)
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.metadata_types import DataQualityIssues, SupportedDataSources
from great_expectations.expectations.metrics.query_metric_provider import LITERAL_BRACE_HINT
from great_expectations.expectations.model_field_descriptions import FAILURE_SEVERITY_DESCRIPTION
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    RenderedAtomicContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.components import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    CodeBlock,
    CodeBlockLanguage,
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import ExpectationValidationResult
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import ExpectationConfiguration


logger = logging.getLogger(__name__)


EXPECTATION_SHORT_DESCRIPTION = (
    "This Expectation will fail validation if the query returns one or more rows. "
    "The WHERE clause defines the fail criteria."
)
UNEXPECTED_ROWS_QUERY_DESCRIPTION = "A SQL or Spark-SQL query to be executed for validation."
SUPPORTED_DATA_SOURCES = [
    SupportedDataSources.SPARK.value,
    SupportedDataSources.POSTGRESQL.value,
    SupportedDataSources.AURORA.value,
    SupportedDataSources.CITUS.value,
    SupportedDataSources.ALLOY.value,
    SupportedDataSources.NEON.value,
    SupportedDataSources.REDSHIFT.value,
    SupportedDataSources.MYSQL.value,
    SupportedDataSources.BIGQUERY.value,
    SupportedDataSources.SNOWFLAKE.value,
    SupportedDataSources.DATABRICKS.value,
    SupportedDataSources.SQL_SERVER.value,
]
DATA_QUALITY_ISSUES = [DataQualityIssues.SQL.value]


class UnexpectedRowsExpectation(BatchExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

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

    Other Parameters:
        severity (str or None): \
            {FAILURE_SEVERITY_DESCRIPTION} \
            For more detail, see [failure severity](https://docs.greatexpectations.io/docs/cloud/expectations/expectations_overview/#failure-severity).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

    Supported Data Sources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[9]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[10]}](https://docs.greatexpectations.io/docs/application_integration_support/)
    Data Quality Issues:
        {DATA_QUALITY_ISSUES[0]}
    """

    unexpected_rows_query: Union[str, SuiteParameterDict] = pydantic.Field(
        description=UNEXPECTED_ROWS_QUERY_DESCRIPTION
    )

    metric_dependencies: ClassVar[Tuple[str, ...]] = (
        "unexpected_rows_query.table",
        "unexpected_rows_query.row_count",
    )
    success_keys: ClassVar[Tuple[str, ...]] = ("unexpected_rows_query",)
    domain_keys: ClassVar[Tuple[str, ...]] = (
        "batch_id",
        "row_condition",
        "condition_parser",
    )

    @pydantic.validator("unexpected_rows_query")
    def _validate_query(
        cls, query: Union[str, SuiteParameterDict]
    ) -> Union[str, SuiteParameterDict]:
        if isinstance(query, SuiteParameterDict):
            return query

        try:
            parsed_fields = [f[1] for f in Formatter().parse(query)]
        except ValueError as exc:
            # A brace that never pairs is caught here, at construction, rather than when the
            # query is first substituted at validation time. Still a `ValueError`, so pydantic
            # reports it as a validation error as before.
            raise ValueError(  # noqa: TRY003 # message carries the query and the escaping rule
                f"unexpected_rows_query contains an unpaired or malformed brace: {exc}. "
                f"{LITERAL_BRACE_HINT} Query: {query}"
            ) from exc
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

        return query.rstrip("; \t\r\n\v\f")

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
                        "const": DATA_QUALITY_ISSUES,
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

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    @override
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> list[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,  # type: ignore[union-attr] # FIXME CoP
            ["unexpected_rows_query"],
        )

        template_str = "Unexpected rows query: $unexpected_rows_query"

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
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    @override
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )

        unexpected_row_count = (
            result.get("result").get("observed_value") if result is not None else None
        )

        template_str = ""
        if isinstance(unexpected_row_count, (int, float)):
            renderer_configuration.add_param(
                name="observed_value",
                param_type=RendererValueType.NUMBER,
                value=unexpected_row_count,
            )

            template_str = "$observed_value unexpected "
            if unexpected_row_count == 1:
                template_str += "row"
            else:
                template_str += "rows"

        renderer_configuration.template_str = template_str

        value_obj = renderedAtomicValueSchema.load(
            {
                "template": renderer_configuration.template_str,
                "params": renderer_configuration.params.dict(),
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "com.superconductive.rendered.string"},
            }
        )
        return RenderedAtomicContent(
            name=AtomicDiagnosticRendererType.OBSERVED_VALUE,
            value=value_obj,
            value_type="StringValueType",
        )

    @override
    def _validate(
        self,
        metrics: dict,
        runtime_configuration: dict | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> Union[ExpectationValidationResult, dict]:
        result_format_config: str | dict[str, Any] = self._get_result_format(
            runtime_configuration=runtime_configuration
        )
        # result_format can be a string or a dict with a "result_format" key
        if isinstance(result_format_config, dict):
            result_format = result_format_config.get("result_format", ResultFormat.SUMMARY)
        else:
            result_format = result_format_config

        metric_value = metrics["unexpected_rows_query.table"]
        unexpected_row_count = metrics["unexpected_rows_query.row_count"]
        success = unexpected_row_count == 0

        if result_format == ResultFormat.BOOLEAN_ONLY:
            return {"success": success}
        elif result_format == ResultFormat.COMPLETE:
            return {
                "success": success,
                "result": {
                    "observed_value": unexpected_row_count,
                    "details": {"unexpected_rows": metric_value},
                },
            }
        else:
            # BASIC or SUMMARY - don't include details with row data
            return {
                "success": success,
                "result": {
                    "observed_value": unexpected_row_count,
                },
            }
