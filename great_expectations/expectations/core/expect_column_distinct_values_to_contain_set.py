import warnings
from typing import TYPE_CHECKING, Dict, List, Optional

import pandas as pd

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.util import parse_value_set
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    ParamSchemaType,
    RendererConfiguration,
)
from great_expectations.render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import RendererParams


class ExpectColumnDistinctValuesToContainSet(ColumnExpectation):
    """Expect the set of distinct column values to contain a given set.

    expect_column_distinct_values_to_contain_set is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name.
        value_set (set-like): \
            A set of objects used for comparison.

    Keyword Args:
        parse_strings_as_datetimes (boolean or None): If True values provided in value_set will be parsed \
        as datetimes before making comparisons.

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
        [expect_column_distinct_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_be_in_set)
        [expect_column_distinct_values_to_equal_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_equal_set)
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.value_counts",)
    success_keys = (
        "value_set",
        "parse_strings_as_datetimes",
    )

    # Default values
    default_kwarg_values = {
        "value_set": None,
        "parse_strings_as_datetimes": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "value_set",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validating that user has inputted a value set and that configuration has been initialized"""
        super().validate_configuration(configuration)

        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set, dict)
            ), "value_set must be a list or a set"
            if isinstance(configuration.kwargs["value_set"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["value_set"]
                ), 'Evaluation Parameter dict for value_set kwarg must have "$PARAMETER" key'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args = (
            ("column", ParamSchemaType.STRING),
            ("value_set", ParamSchemaType.ARRAY),
            ("parse_strings_as_datetimes", ParamSchemaType.BOOLEAN),
            ("row_condition", ParamSchemaType.STRING),
            ("condition_parser", ParamSchemaType.STRING),
        )
        for name, schema_type in add_param_args:
            renderer_configuration.add_param(name=name, schema_type=schema_type)

        params: RendererParams = renderer_configuration.params
        template_str = ""

        if params.value_set:
            renderer_configuration = cls._add_value_set_params(
                renderer_configuration=renderer_configuration
            )
            value_set_str: str = cls._get_value_set_string(
                renderer_configuration=renderer_configuration
            )
            template_str = f"distinct values must contain this set: {value_set_str}."

            if params.parse_strings_as_datetimes:
                template_str += " Values should be parsed as datetimes."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        if params.row_condition:
            renderer_configuration = cls._add_row_condition_params(
                renderer_configuration=renderer_configuration
            )
            row_condition_str: str = cls._get_row_condition_string(
                renderer_configuration=renderer_configuration
            )
            template_str = f"{row_condition_str}, then {template_str}"

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
    ) -> List[RenderedStringTemplateContent]:
        renderer_configuration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        params = substitute_none_for_missing(
            renderer_configuration.configuration.kwargs,
            [
                "column",
                "value_set",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params[f"v__{str(i)}"] = v

            values_string = " ".join(
                [f"$v__{str(i)}" for i, v in enumerate(params["value_set"])]
            )

        template_str = f"distinct values must contain this set: {values_string}."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

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
                        "styling": renderer_configuration.styling,
                    },
                }
            )
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        parse_strings_as_datetimes = self.get_success_kwargs(configuration).get(
            "parse_strings_as_datetimes"
        )
        observed_value_counts = metrics.get("column.value_counts")
        value_set = self.get_success_kwargs(configuration).get("value_set")

        if parse_strings_as_datetimes:
            # deprecated-v0.13.41
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/
""",
                DeprecationWarning,
            )
            parsed_value_set = parse_value_set(value_set)
            observed_value_counts.index = pd.to_datetime(observed_value_counts.index)
        else:
            parsed_value_set = value_set

        observed_value_set = set(observed_value_counts.index)
        expected_value_set = set(parsed_value_set)

        return {
            "success": observed_value_set.issuperset(expected_value_set),
            "result": {
                "observed_value": sorted(list(observed_value_set)),
                "details": {"value_counts": observed_value_counts},
            },
        }
