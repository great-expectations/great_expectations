from typing import TYPE_CHECKING, Dict, Optional

import numpy as np

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core.expectation_configuration import parse_result_format
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _format_map_output,
    render_evaluation_parameter_string,
)
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnValuesToNotBeNullAndColumnToNotBeEmpty(ColumnMapExpectation):
    """Expect column values to not be null and column to not be empty.

    To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an
    np.NaN in pandas. Empty strings don't count as null unless they have been coerced to a null type.

    expect_column_values_to_not_be_null_and_column_to_not_be_empty is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    See Also:
        [expect_column_values_to_be_null](https://greatexpectations.io/expectations/expect_column_values_to_be_null)
        [expect_column_values_not_to_be_null](https://greatexpectations.io/expectations/expect_column_values_not_to_be_null)
    """

    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation"],
        "contributors": [
            "@tmilitino",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.nonnull"
    args_keys = ("column",)

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "not_has_null": ["test", "foo", "for", "bar", "boo"],
                "has_null": ["test", "foo", "for", "bar", np.NaN],
            },
            "tests": [
                {
                    "title": "not_null_values",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "not_has_null"},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "has_null",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "has_null"},
                    "out": {
                        "success": False,
                    },
                },
            ],
        },
        {
            "data": {"empty_dataframe": []},
            "tests": [
                {
                    "title": "not_has_values",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "empty_dataframe"},
                    "out": {
                        "success": False,
                    },
                },
            ],
        },
    ]

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates the configuration of an Expectation.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                                  from the configuration attribute of the Expectation instance.

        Raises:
            `InvalidExpectationConfigurationError`: The configuration does not contain the values required by the
                                                                           Expectation."
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

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

        params = renderer_configuration.params

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = (
                "values must not be null, at least $mostly_pct % of the time."
            )
        else:
            template_str = "values must never be null."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            if include_column_name:
                template_str = "$column values must not be null, at least $mostly_pct % of the time."
            else:
                template_str = (
                    "values must not be null, at least $mostly_pct % of the time."
                )
        else:
            if include_column_name:
                template_str = "$column values must never be null."
            else:
                template_str = "values must never be null."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

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

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        result_dict = result.result

        try:
            null_percent = result_dict["unexpected_percent"]
            return (
                num_to_str(100 - null_percent, precision=5, use_locale=True)
                + "% not null"
            )
        except KeyError:
            return "unknown % not null"
        except TypeError:
            return "NaN% not null"

    @classmethod
    @renderer(
        renderer_type=LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_MISSING_COUNT_ROW
    )
    def _descriptive_column_properties_table_missing_count_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Missing (n)",
                        "tooltip": {
                            "content": "expect_column_values_to_not_be_null_and_column_to_not_be_empty"
                        },
                    },
                }
            ),
            result.result["unexpected_count"]
            if "unexpected_count" in result.result
            and result.result["unexpected_count"] is not None
            else "--",
        ]

    @classmethod
    @renderer(
        renderer_type=LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_MISSING_PERCENT_ROW
    )
    def _descriptive_column_properties_table_missing_percent_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Missing (%)",
                        "tooltip": {
                            "content": "expect_column_values_to_not_be_null_and_column_to_not_be_empty"
                        },
                    },
                }
            ),
            f"{result.result['unexpected_percent']:.1f}%"
            if "unexpected_percent" in result.result
            and result.result["unexpected_percent"] is not None
            else "--",
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format = self.get_result_format(
            configuration=configuration, runtime_configuration=runtime_configuration
        )
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        total_count = metrics.get("table.row_count")
        unexpected_count = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )

        if total_count is None or total_count == 0:
            success = False
        else:
            success_ratio = (total_count - unexpected_count) / total_count
            success = success_ratio >= mostly

        nonnull_count = None

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=metrics.get("table.row_count"),
            nonnull_count=nonnull_count,
            unexpected_count=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
            ),
            unexpected_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
            ),
            unexpected_index_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
            ),
            unexpected_index_query=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
            ),
        )


if __name__ == "__main__":
    ExpectColumnValuesToNotBeNullAndColumnToNotBeEmpty().print_diagnostic_checklist(
        show_failed_tests=True
    )
