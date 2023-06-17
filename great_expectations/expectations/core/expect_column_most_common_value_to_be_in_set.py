from typing import TYPE_CHECKING, Dict, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnMostCommonValueToBeInSet(ColumnAggregateExpectation):
    """Expect the most common value to be within the designated value set.

    expect_column_most_common_value_to_be_in_set is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name
        value_set (set-like): \
            A list of potential values to match

    Keyword Args:
        ties_okay (boolean or None): \
            If True, then the expectation will still succeed if values outside the designated set are as common \
            (but not more common) than designated values

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

    Notes:
        * observed_value field in the result object is customized for this expectation to be a list \
          representing the most common values in the column, which is often a single element... if there \
          is a tie for most common among multiple values, observed_value will contain a single copy of each \
          most common value
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
    metric_dependencies = ("column.most_common_value",)
    success_keys = (
        "value_set",
        "ties_okay",
    )

    # Default values
    default_kwarg_values = {
        "value_set": None,
        "ties_okay": None,
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
        """Validates the configuration for the Expectation.

        For `expect_column_distinct_values_to_contain_set`
        we require that the `configuraton.kwargs` contain a `value_set` key that is either a `list`, `set`,
        or `dict`.

        Args:
            configuration: The ExpectationConfiguration to be validated.

        Raises:
            InvalidExpectationConfigurationError: The configuraton does not contain the values required by the Expectation
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set, dict)
            ), "value_set must be a list or a set"
            if isinstance(configuration.kwargs["value_set"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["value_set"]
                ), 'Evaluation Parameter dict for value_set_kwarg must have "$PARAMETER" key'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("value_set", RendererValueType.ARRAY),
            ("ties_okay", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params
        template_str = ""

        if params.value_set:
            array_param_name = "value_set"
            param_prefix = "v__"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            value_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += (
                f"most common value must belong to this set: {value_set_str}."
            )

            if params.ties_okay:
                template_str += " Values outside this set that are as common (but not more common) are allowed."

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
            ["column", "value_set", "ties_okay", "row_condition", "condition_parser"],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params[f"v__{str(i)}"] = v

            values_string = " ".join(
                [f"$v__{str(i)}" for i, v in enumerate(params["value_set"])]
            )

        template_str = f"most common value must belong to this set: {values_string}."

        if params.get("ties_okay"):
            template_str += " Values outside this set that are as common (but not more common) are allowed."

        if include_column_name:
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
                        "styling": styling,
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
        most_common_value = metrics.get("column.most_common_value")
        value_set = configuration.kwargs.get("value_set") or []
        expected_value_set = set(value_set)
        ties_okay = configuration.kwargs.get("ties_okay")

        intersection_count = len(expected_value_set.intersection(most_common_value))

        if ties_okay:
            success = intersection_count > 0
        else:
            success = len(most_common_value) == 1 and intersection_count == 1

        return {"success": success, "result": {"observed_value": most_common_value}}
