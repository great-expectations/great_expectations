from typing import Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import substitute_none_for_missing


class ExpectColumnValuesToNotMatchLikePatternList(ColumnMapExpectation):
    """Expect the column entries to be strings that do NOT match any of a provided list of like pattern expressions.

    expect_column_values_to_not_match_like_pattern_list is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        like_pattern_list (List[str]): \
            The list of like pattern expressions the column entries should NOT match.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_match_regex \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_values_to_match_regex>`

        :func:`expect_column_values_to_match_regex_list \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_match_regex_list>`
    """

    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.not_match_like_pattern_list"
    success_keys = (
        "like_pattern_list",
        "mostly",
    )
    default_kwarg_values = {
        "like_pattern_list": None,
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }
    args_keys = (
        "column",
        "like_pattern_list",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        try:
            assert (
                "like_pattern_list" in configuration.kwargs
            ), "Must provide like_pattern_list"
            assert isinstance(
                configuration.kwargs.get("like_pattern_list"), (list, dict)
            ), "like_pattern_list must be a list"
            assert isinstance(configuration.kwargs.get("like_pattern_list"), dict) or (
                len(configuration.kwargs.get("like_pattern_list")) > 0
            ), "At least one like_pattern must be supplied in the like_pattern_list."
            if isinstance(configuration.kwargs.get("like_pattern_list"), dict):
                assert "$PARAMETER" in configuration.kwargs.get(
                    "like_pattern_list"
                ), 'Evaluation Parameter dict for like_pattern_list kwarg must have "$PARAMETER" key.'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        language: Optional[str] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs
    ) -> None:
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )
