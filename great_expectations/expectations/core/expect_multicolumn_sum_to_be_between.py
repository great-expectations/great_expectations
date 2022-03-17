from great_expectations.expectations.expectation import MulticolumnMapExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer


class ExpectMulticolumnSumToBeBetween(MulticolumnMapExpectation):
    """
    Expects that the sum of row values will be between a minimum value and a maximum value,
    summing only values in columns specified in column_list.

    Args:
        column_list (tuple or list): Set of columns to be checked
        min_value (comparable type or None): The minimum value for a column entry.
        max_value (comparable type or None): The maximum value for a column entry.
        strict_min (boolean):
            If True, values must be strictly larger than min_value, default=False
        strict_max (boolean):
            If True, values must be strictly smaller than max_value, default=False

    Keyword Args:
        ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.

    Returns:
        An ExpectationSuiteValidationResult
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": [
            "core expectation",
            "column aggregate expectation",
            "needs migration to modular expectations api",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    map_metric = "multicolumn_sum.between"
    success_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )
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
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass

    @classmethod
    @renderer(renderer_type="renderer.diagnostic.observed_value")
    def _diagnostic_observed_value_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        pass
