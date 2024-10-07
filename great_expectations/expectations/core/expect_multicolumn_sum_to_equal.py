from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Literal, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.expectations.expectation import (
    MulticolumnMapExpectation,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_LIST_DESCRIPTION,
    IGNORE_ROW_IF_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.components import LegacyRendererType
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs

logger = logging.getLogger(__name__)

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect that the sum of row values in a specified column list "
    "is the same for each row, and equal to a specified sum total."
)
SUM_TOTAL_DESCRIPTION = "Expected sum of columns"
SUPPORTED_DATA_SOURCES = [
    "Pandas",
    "Spark",
    "SQLite",
    "PostgreSQL",
    "MySQL",
    "MSSQL",
    "Redshift",
    "BigQuery",
    "Snowflake",
]
DATA_QUALITY_ISSUES = ["Data integrity"]


class ExpectMulticolumnSumToEqual(MulticolumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectMulticolumnSumToEqual is a \
    Multicolumn Map Expectation.

    Multicolumn Map Expectations are evaluated for a set of columns and ask a yes/no question about the row-wise relationship between those columns.
    Based on the result, they then calculate the percentage of rows that gave a positive answer.
    If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column_list (tuple or list): {COLUMN_LIST_DESCRIPTION}
        sum_total (int or float): {SUM_TOTAL_DESCRIPTION}

    Other Parameters:
        ignore_row_if (str): \
            "both_values_are_missing", "either_value_is_missing", "neither" \
            {IGNORE_ROW_IF_DESCRIPTION} Default "neither".
        mostly (None or a float between 0 and 1): \
            {MOSTLY_DESCRIPTION} \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2   test3
            0 	1       2       4
            1 	2       -2       7
            2 	4   	4       -3

    Code Examples:
        Passing Case:
            Input:
                ExpectMulticolumnSumToEqual(
                    column_list=["test", "test2", "test3"],
                    sum_total=7,
                    mostly=0.66
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 3,
                    "unexpected_count": 1,
                    "unexpected_percent": 33.33333333333333,
                    "partial_unexpected_list": [
                      {{
                        "test": 4,
                        "test2": 4,
                        "test3": -3
                      }}
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 33.33333333333333,
                    "unexpected_percent_nonmissing": 33.33333333333333
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectMulticolumnSumToEqual(
                    column_list=["test", "test2", "test3"],
                    sum_total=7
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 3,
                    "unexpected_count": 1,
                    "unexpected_percent": 33.33333333333333,
                    "partial_unexpected_list": [
                      {{
                        "test": 4,
                        "test2": 4,
                        "test3": -3
                      }}
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 33.33333333333333,
                    "unexpected_percent_nonmissing": 33.33333333333333
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    sum_total: float = pydantic.Field(description=SUM_TOTAL_DESCRIPTION)
    ignore_row_if: Literal["all_values_are_missing", "any_value_is_missing", "never"] = (
        pydantic.Field(
            default="all_values_are_missing",
            description=IGNORE_ROW_IF_DESCRIPTION,
        )
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "multi-column expectation",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "multicolumn_sum.equal"
    success_keys = ("mostly", "sum_total")
    args_keys = (
        "column_list",
        "sum_total",
    )

    class Config:
        title = "Expect multicolumn sum to equal"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectMulticolumnSumToEqual]) -> None:
            MulticolumnMapExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "data_quality_issues": {
                        "title": "Data Quality Issues",
                        "type": "array",
                        "const": DATA_QUALITY_ISSUES,
                    },
                    "library_metadata": {
                        "title": "Library Metadata",
                        "type": "object",
                        "const": model._library_metadata,
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
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_list", RendererValueType.ARRAY),
            ("sum_total", RendererValueType.NUMBER),
            ("mostly", RendererValueType.NUMBER),
            ("ignore_row_if", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        template_str = ""
        if params.column_list:
            array_param_name = "column_list"
            param_prefix = "column_list_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += "Sum across columns " + cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str += " must be $sum_total, at least $mostly_pct % of the time."
        else:
            template_str += " must be $sum_total."

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> List[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column_list", "sum_total", "mostly"],
        )
        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
        mostly_str = "" if params.get("mostly") is None else ", at least $mostly_pct % of the time"
        sum_total = params.get("sum_total")  # noqa: F841

        column_list_str = ""
        for idx in range(len(params["column_list"]) - 1):
            column_list_str += f"$column_list_{idx!s}, "
            params[f"column_list_{idx!s}"] = params["column_list"][idx]
        last_idx = len(params["column_list"]) - 1
        column_list_str += f"$column_list_{last_idx!s}"
        params[f"column_list_{last_idx!s}"] = params["column_list"][last_idx]
        template_str = f"Sum across columns {column_list_str} must be $sum_total{mostly_str}."
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]
