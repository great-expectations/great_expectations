import logging
from typing import List, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.expectations.expectation import (
    MulticolumnMapExpectation,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.components import LegacyRendererType
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    num_to_str,
    substitute_none_for_missing,
)

logger = logging.getLogger(__name__)


class ExpectMulticolumnSumToEqual(MulticolumnMapExpectation):
    """Expect that the sum of row values in a specified column list is the same for each row, and equal to a specified sum total.

    expect_multicolumn_sum_to_equal is a \
    [Multicolumn Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_multicolumn_map_expectations).

    Args:
        column_list (tuple or list): Set of columns to be checked
        sum_total (int): expected sum of columns

    Keyword Args:
        ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"

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
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
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

    map_metric = "multicolumn_sum.equal"
    success_keys = ("mostly", "sum_total")
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ignore_row_if": "all_values_are_missing",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column_list",
        "sum_total",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates the configuration for the Expectation.

        For this expectation, `configuration.kwargs` may contain `min_value` and `max_value` whose value is a number.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided,
                it will be pulled from the configuration attribute of the Expectation instance.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required
                by the Expectation.
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @renderer(renderer_type="renderer.prescriptive")
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
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )
        sum_total = params.get("sum_total")  # noqa: F841

        column_list_str = ""
        for idx in range(len(params["column_list"]) - 1):
            column_list_str += f"$column_list_{idx!s}, "
            params[f"column_list_{idx!s}"] = params["column_list"][idx]
        last_idx = len(params["column_list"]) - 1
        column_list_str += f"$column_list_{last_idx!s}"
        params[f"column_list_{last_idx!s}"] = params["column_list"][last_idx]
        template_str = (
            f"Sum across columns {column_list_str} must be $sum_total{mostly_str}."
        )
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
