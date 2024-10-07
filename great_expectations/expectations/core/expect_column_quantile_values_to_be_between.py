from __future__ import annotations

from numbers import Number
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Type, Union

import numpy as np

from great_expectations.compatibility import pydantic
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.expectations.expectation import (
    COLUMN_DESCRIPTION,
    ColumnAggregateExpectation,
    render_suite_parameter_string,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    LegacyDescriptiveRendererType,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererSchema,
    RendererTableValue,
    RendererValueType,
)
from great_expectations.render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.util import isclose

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs
    from great_expectations.validator.validator import (
        ValidationDependencies,
    )


class QuantileRange(pydantic.BaseModel):
    quantiles: List[float]
    value_ranges: List[List[Union[float, int, None]]]


EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the specific provided column quantiles "
    "to be between a minimum value and a maximum value."
)
QUANTILE_RANGES_DESCRIPTION = (
    "Key 'quantiles' is an increasingly ordered list of desired quantile values (floats). "
    "Key 'value_ranges' is a list of 2-value lists that specify a lower and upper bound "
    "(inclusive) for the corresponding quantile (with [min, max] ordering)."
)
ALLOW_RELATIVE_ERROR_DESCRIPTION = (
    "Whether to allow relative error in quantile "
    "communications on backends that support or require it."
)
SUPPORTED_DATA_SOURCES = ["Pandas", "Spark", "SQLite", "PostgreSQL", "MySQL", "MSSQL", "Redshift"]
DATA_QUALITY_ISSUES = ["Numerical data"]


class ExpectColumnQuantileValuesToBeBetween(ColumnAggregateExpectation):
    # noinspection PyUnresolvedReferences
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnQuantileValuesToBeBetween is a \
    Column Aggregate Expectation.

    Column Aggregate Expectations are one of the most common types of Expectation.
    They are evaluated for a single column, and produce an aggregate Metric, such as a mean, standard deviation, number of unique values, column type, etc.
    If that Metric meets the conditions you set, the Expectation considers that data valid.

    ExpectColumnQuantileValuesToBeBetween can be computationally intensive for large datasets.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        quantile_ranges (dictionary with keys 'quantiles' and 'value_ranges'): \
            {QUANTILE_RANGES_DESCRIPTION} The length of the 'quantiles' list and the 'value_ranges' list must be equal.
        allow_relative_error (boolean or string): \
            {ALLOW_RELATIVE_ERROR_DESCRIPTION}

    Other Parameters:
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

    Notes:
        * min_value and max_value are both inclusive.
        * If min_value is None, then max_value is treated as an upper bound only
        * If max_value is None, then min_value is treated as a lower bound only
        * details.success_details field in the result object is customized for this expectation

    See Also:
        [ExpectColumnMinToBeBetween](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
        [ExpectColumnMaxToBeBetween](https://greatexpectations.io/expectations/expect_column_max_to_be_between)
        [ExpectColumnMedianToBeBetween](https://greatexpectations.io/expectations/expect_column_median_to_be_between)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test
            0 	1       1
            1 	2       7
            2 	2       2.5
            3   3       3
            4   3       2
            5   3       5
            6   4       6

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnQuantileValuesToBeBetween(
                    column="test",
                    quantile_ranges={{
                        "quantiles": [0, .333, .667, 1],
                        "value_ranges": [[0,1], [2,3], [3,4], [4,5]]
                    }}
                )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": {{
                      "quantiles": [
                        0,
                        0.333,
                        0.6667,
                        1
                      ],
                      "values": [
                        1,
                        2,
                        3,
                        4
                      ]
                    }},
                    "details": {{
                      "success_details": [
                        true,
                        true,
                        true,
                        true
                      ]
                    }}
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnQuantileValuesToBeBetween(
                    column="test2",
                    quantile_ranges={{
                        "quantiles": [0, .333, .667, 1],
                        "value_ranges": [[0,1], [2,3], [3,4], [4,5]]
                    }}
                )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": {{
                      "quantiles": [
                        0,
                        0.333,
                        0.6667,
                        1
                      ],
                      "values": [
                        1.0,
                        2.5,
                        5.0,
                        7.0
                      ]
                    }},
                    "details": {{
                      "success_details": [
                        true,
                        true,
                        false,
                        false
                      ]
                    }}
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    quantile_ranges: QuantileRange = pydantic.Field(description=QUANTILE_RANGES_DESCRIPTION)
    allow_relative_error: Union[bool, str] = pydantic.Field(
        False,
        description=ALLOW_RELATIVE_ERROR_DESCRIPTION,
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    _library_metadata = library_metadata

    metric_dependencies = ("column.quantile_values",)
    success_keys = (
        "quantile_ranges",
        "allow_relative_error",
    )

    args_keys = (
        "column",
        "quantile_ranges",
        "allow_relative_error",
    )

    class Config:
        title = "Expect column quantile values to be between"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnQuantileValuesToBeBetween]
        ) -> None:
            ColumnAggregateExpectation.Config.schema_extra(schema, model)
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

    @pydantic.validator("quantile_ranges")
    def validate_quantile_ranges(cls, quantile_ranges: QuantileRange) -> Optional[QuantileRange]:
        try:
            assert all(
                True if None in x else x == sorted([val for val in x if val is not None])
                for x in quantile_ranges.value_ranges
            ), "quantile_ranges must consist of ordered pairs"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if len(quantile_ranges.quantiles) != len(quantile_ranges.value_ranges):
            raise ValueError("quantile_values and quantiles must have the same number of elements")  # noqa: TRY003

        return quantile_ranges

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        template_str = "quantiles must be within the following value ranges."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        quantiles: list = renderer_configuration.kwargs.get("quantile_ranges", {}).get(
            "quantiles", []
        )
        value_ranges: list = renderer_configuration.kwargs.get("quantile_ranges", {}).get(
            "value_ranges", []
        )

        header_row = [
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Quantile"
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Min Value"
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Max Value"
            ),
        ]

        renderer_configuration.header_row = header_row

        table = []
        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}
        for quantile, value_range in zip(quantiles, value_ranges):
            quantile_string = quantile_strings.get(quantile, f"{quantile:3.2f}")
            value_range_lower: Union[Number, str] = value_range[0] if value_range[0] else "Any"
            value_rage_lower_type = (
                RendererValueType.NUMBER if value_range[0] else RendererValueType.STRING
            )
            value_range_upper: Union[Number, str] = value_range[1] if value_range[1] else "Any"
            value_range_upper_type = (
                RendererValueType.NUMBER if value_range[0] else RendererValueType.STRING
            )
            table.append(
                [
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=quantile_string,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=value_rage_lower_type),
                        value=value_range_lower,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=value_range_upper_type),
                        value=value_range_upper,
                    ),
                ]
            )

        renderer_configuration.table = table

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.SUMMARY)
    @render_suite_parameter_string
    def _prescriptive_summary(
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
        renderer_configuration = cls._prescriptive_template(
            renderer_configuration=renderer_configuration
        )
        header_row = [value.dict() for value in renderer_configuration.header_row]
        table = []
        for row in renderer_configuration.table:
            table.append([value.dict() for value in row])
        value_obj = renderedAtomicValueSchema.load(
            {
                "header": {
                    "schema": {"type": "StringValueType"},
                    "value": {
                        "template": renderer_configuration.template_str,
                        "params": renderer_configuration.params.dict(),
                    },
                },
                "header_row": header_row,
                "table": table,
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "TableType"},
            }
        )
        return RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.SUMMARY,
            value=value_obj,
            value_type="TableType",
        )

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        _ = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration["kwargs"],
            ["column", "quantile_ranges", "row_condition", "condition_parser"],
        )
        template_str = "quantiles must be within the following value ranges."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str + ", then " + template_str[0].lower() + template_str[1:]
            )
            params.update(conditional_params)

        expectation_string_obj = {
            "content_block_type": "string_template",
            "string_template": {"template": template_str, "params": params},
        }

        quantiles = params["quantile_ranges"]["quantiles"]
        value_ranges = params["quantile_ranges"]["value_ranges"]

        table_header_row = ["Quantile", "Min Value", "Max Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for quantile, value_range in zip(quantiles, value_ranges):
            quantile_string = quantile_strings.get(quantile, f"{quantile:3.2f}")
            table_rows.append(
                [
                    quantile_string,
                    str(value_range[0]) if value_range[0] is not None else "Any",
                    str(value_range[1]) if value_range[1] is not None else "Any",
                ]
            )

        quantile_range_table = RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": table_header_row,
                "table": table_rows,
                "styling": {
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-unbordered",
                            "col-4",
                            "mt-2",
                        ],
                    },
                    "parent": {"styles": {"list-style-type": "none"}},
                },
            }
        )

        return [expectation_string_obj, quantile_range_table]

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        if result.result is None or result.result.get("observed_value") is None:
            return "--"

        quantiles = result.result.get("observed_value", {}).get("quantiles", [])
        value_ranges = result.result.get("observed_value", {}).get("values", [])

        table_header_row = ["Quantile", "Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append(
                [
                    quantile_string if quantile_string else f"{quantile:3.2f}",
                    str(value_ranges[idx]),
                ]
            )

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": table_header_row,
                "table": table_rows,
                "styling": {
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered", "col-4"],
                    }
                },
            }
        )

    @classmethod
    def _atomic_diagnostic_observed_value_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ):
        if (
            renderer_configuration.result.result is None
            or renderer_configuration.result.result.get("observed_value") is None
        ):
            renderer_configuration.template_str = "--"
            return renderer_configuration

        quantiles = renderer_configuration.result.result.get("observed_value", {}).get(
            "quantiles", []
        )
        value_ranges = renderer_configuration.result.result.get("observed_value", {}).get(
            "values", []
        )

        header_row = [
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Quantile"
            ),
            RendererTableValue(schema=RendererSchema(type=RendererValueType.STRING), value="Value"),
        ]

        table = []
        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}
        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile) or f"{quantile:3.2f}"
            table.append(
                [
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=quantile_string,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.NUMBER),
                        value=value_ranges[idx],
                    ),
                ]
            )

        renderer_configuration.header_row = header_row
        renderer_configuration.table = table

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._atomic_diagnostic_observed_value_template(
            renderer_configuration=renderer_configuration,
        )
        if renderer_configuration.template_str:
            value_obj = renderedAtomicValueSchema.load(
                {
                    "template": renderer_configuration.template_str,
                    "params": {},
                    "schema": {"type": "StringValueType"},
                }
            )
            return RenderedAtomicContent(
                name="atomic.diagnostic.observed_value",
                value=value_obj,
                value_type="StringValueType",
            )
        else:
            header_row = [value.dict() for value in renderer_configuration.header_row]
            table = []
            for row in renderer_configuration.table:
                table.append([value.dict() for value in row])
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header_row": header_row,
                    "table": table,
                    "schema": {"type": "TableType"},
                }
            )
            return RenderedAtomicContent(
                name=AtomicDiagnosticRendererType.OBSERVED_VALUE,
                value=value_obj,
                value_type="TableType",
            )

    @classmethod
    @renderer(renderer_type=LegacyDescriptiveRendererType.QUANTILE_TABLE)
    def _descriptive_quantile_table_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        table_rows = []
        quantiles = result.result["observed_value"]["quantiles"]
        quantile_ranges = result.result["observed_value"]["values"]

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append(
                [
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": (
                                quantile_string if quantile_string else f"{quantile:3.2f}"
                            ),
                            "tooltip": {
                                "content": "expect_column_quantile_values_to_be_between \n expect_column_median_to_be_between"  # noqa: E501
                                if quantile == 0.50  # noqa: PLR2004
                                else "expect_column_quantile_values_to_be_between"
                            },
                        },
                    },
                    quantile_ranges[idx],
                ]
            )

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {"template": "Quantiles", "tag": "h6"},
                    }
                ),
                "table": table_rows,
                "styling": {
                    "classes": ["col-3", "mt-1", "pl-1", "pr-1"],
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered"],
                    },
                },
            }
        )

    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine, runtime_configuration
        )
        configuration = self.configuration
        # column.quantile_values expects a "quantiles" key
        validation_dependencies.get_metric_configuration(
            metric_name="column.quantile_values"
        ).metric_value_kwargs["quantiles"] = configuration.kwargs["quantile_ranges"]["quantiles"]
        return validation_dependencies

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        quantile_vals = metrics.get("column.quantile_values")
        quantile_ranges = self.configuration.kwargs.get("quantile_ranges")
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]

        # We explicitly allow "None" to be interpreted as +/- infinity
        comparison_quantile_ranges = [
            [
                -np.inf if lower_bound is None else lower_bound,
                np.inf if upper_bound is None else upper_bound,
            ]
            for (lower_bound, upper_bound) in quantile_value_ranges
        ]
        success_details = [
            isclose(
                operand_a=quantile_vals[idx],
                operand_b=range_[0],
                rtol=1.0e-4,
            )
            or isclose(
                operand_a=quantile_vals[idx],
                operand_b=range_[1],
                rtol=1.0e-4,
            )
            or range_[0] <= quantile_vals[idx] <= range_[1]
            for idx, range_ in enumerate(comparison_quantile_ranges)
        ]

        return {
            "success": np.all(success_details),
            "result": {
                "observed_value": {"quantiles": quantiles, "values": quantile_vals},
                "details": {"success_details": success_details},
            },
        }
