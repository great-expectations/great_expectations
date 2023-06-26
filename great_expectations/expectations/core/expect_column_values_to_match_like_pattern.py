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

try:
    import sqlalchemy as sa  # noqa: F401, TID251
except ImportError:
    pass


class ExpectColumnValuesToMatchLikePattern(ColumnMapExpectation):
    """Expect the column entries to be strings that match a given like pattern expression.

    expect_column_values_to_match_like_pattern is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

    Args:
        column (str): \
            The column name.
        like_pattern (str): \
            The like pattern expression the column entries should match.

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
        [expect_column_values_to_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)
        [expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)
        [expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)
        [expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)
        [expect_column_values_to_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern_list)
        [expect_column_values_to_not_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern)
        [expect_column_values_to_not_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern_list)
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

    map_metric = "column_values.match_like_pattern"
    success_keys = (
        "mostly",
        "like_pattern",
    )
    default_kwarg_values = {
        "like_pattern": None,
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }
    args_keys = (
        "column",
        "like_pattern",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration for the Expectation.

        For `expect_column_values_to_match_like_pattern`
        we require that the `configuraton.kwargs` contain a `like_pattern` key that is either a `str` or `dict`.

        Args:
            configuration: The ExpectationConfiguration to be validated.

        Raises:
            InvalidExpectationConfigurationError: The configuraton does not contain the values required by the Expectation
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert "like_pattern" in configuration.kwargs, "Must provide like_pattern"
            assert isinstance(
                configuration.kwargs.get("like_pattern"), (str, dict)
            ), "like_pattern must be a string"
            if isinstance(configuration.kwargs.get("like_pattern"), dict):
                assert "$PARAMETER" in configuration.kwargs.get(
                    "like_pattern"
                ), 'Evaluation Parameter dict for like_pattern kwarg must have "$PARAMETER" key.'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> None:
        runtime_configuration = runtime_configuration or {}
        _ = False if runtime_configuration.get("include_column_name") is False else True
        _ = runtime_configuration.get("styling")
        params = substitute_none_for_missing(  # noqa: F841 # unused
            configuration.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )
