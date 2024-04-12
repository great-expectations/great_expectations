from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import ordinal, substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnToExist(BatchExpectation):
    """Checks for the existence of a specified column within a table.

    expect_column_to_exist is a \
    [Batch Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations).

    BatchExpectations are one of the most common types of Expectation. They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        column (str): \
            The column name.

    Other Parameters:
        column_index (int or None, optional): \
            If not None, checks the order of the columns. The expectation will fail if the \
            column is not in location column_index (zero-indexed).
        result_format (str or None, optional): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None, optional): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None, optional): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Schema

    Example Data:
                test 	test2
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

    Passing Case:
        Input:
            ExpectColumnToExist(
                column="test",
                column_index=0
        )

        Output:
            {
              "exception_info": {
                "raised_exception": false,
                "exception_traceback": null,
                "exception_message": null
              },
              "meta": {},
              "success": true,
              "result": {}
            }

    Failing Case:
        Input:
            ExpectColumnToExist(
                column="missing_column",
            )

        Output:
            {
              "exception_info": {
                "raised_exception": false,
                "exception_traceback": null,
                "exception_message": null
              },
              "meta": {},
              "success": false,
              "result": {}
            }
    """  # noqa: E501

    column: str
    column_index: Union[int, SuiteParameterDict, None]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    metric_dependencies = ("table.columns",)
    success_keys = (
        "column",
        "column_index",
    )
    domain_keys = (
        "batch_id",
        "table",
    )
    args_keys = ("column", "column_index")

    @classmethod
    @override
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("column_index", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.column_index:
            if renderer_configuration.include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            renderer_configuration.add_param(
                name="column_indexth",
                param_type=RendererValueType.STRING,
                value=ordinal(params.column_index.value),
            )
            if renderer_configuration.include_column_name:
                template_str = "$column must be the $column_indexth field."
            else:
                template_str = "must be the $column_indexth field."

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @override
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> list[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,  # type: ignore[union-attr] # FIXME: could be None
            ["column", "column_index"],
        )

        if params["column_index"] is None:
            if include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            params["column_indexth"] = ordinal(params["column_index"])
            if include_column_name:
                template_str = "$column must be the $column_indexth field."
            else:
                template_str = "must be the $column_indexth field."

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

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        actual_columns = metrics["table.columns"]
        expected_column_name = self._get_success_kwargs().get("column")
        expected_column_index = self._get_success_kwargs().get("column_index")

        if expected_column_index:
            try:
                success = actual_columns[expected_column_index] == expected_column_name
            except IndexError:
                success = False
        else:
            success = expected_column_name in actual_columns

        return {"success": success}
