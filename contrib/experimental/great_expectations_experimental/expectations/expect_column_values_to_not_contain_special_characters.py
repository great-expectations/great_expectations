import string
from typing import Optional

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.pyspark import types
from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

# This class defines a Metric to support your Expectation
# The main business logic for calculation lives here.


class ColumnValuesToNotContainSpecialCharacters(ColumnMapMetricProvider):
    # This is the id string that will be used to reference the metric.
    condition_metric_name = "column_values.not_contain_special_character"

    # condition_value_keys are arguments used to determine the value of the metric.
    condition_value_keys = ("allowed_characters",)

    # This method defines the business logic for evaluating the metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, allowed_characters: list or set, **kwargs):
        def not_contain_special_character(val, *special_characters):
            special_characters = [
                char for char in special_characters if char not in allowed_characters
            ]

            for c in special_characters:
                if c in str(val):
                    return False
            return True

        return column.apply(
            not_contain_special_character, args=(list(string.punctuation))
        )

    # This method defines the business logic for evaluating the metric when using a SparkExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, allowed_characters: list or set, **kwargs):
        def not_contain_special_character(val, *special_characters):
            special_characters = [
                char
                for char in list(string.punctuation)
                if char not in allowed_characters
            ]

            for c in special_characters:
                if c in str(val):
                    return False
            return True

        # Register the UDF
        not_contain_special_character_udf = F.udf(
            not_contain_special_character, types.BooleanType()
        )

        # Apply the UDF to the column
        result_column = F.when(
            not_contain_special_character_udf(column, F.lit(string.punctuation)), True
        ).otherwise(False)
        return result_column


# This class defines the Expectation itself
class ExpectColumnValuesToNotContainSpecialCharacters(ColumnMapExpectation):
    """Expect column entries to not contain special characters.

    Args:
        column (str): \
            The column name

    Keyword Args:
        allowed_characters (list): \
            A list of characters that will be ignored when validating that a column doesn't have special characters

        mostly (None or a float value between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for the Expectation
    examples = [
        {
            "data": {
                "no_special_character": [
                    "maxwell",
                    "neil armstrong",
                    234,
                ],
            },
            "tests": [
                {
                    "title": "positive_test__with_no_special_character",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "no_special_character", "mostly": 1},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
            ],
        },
        {
            "data": {
                "mostly_no_special_character": [
                    "apple@",
                    "pear$!",
                    "%banana%",
                    "maxwell",
                    "neil armstrong",
                    234,
                ],
            },
            "tests": [
                {
                    "title": "negative_test_with_special_character",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_no_special_character", "mostly": 1},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": ["apple@", "pear$!", "%banana%"],
                    },
                },
                {
                    "title": "positive_test_with_allowed_special_character_list",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_no_special_character",
                        "allowed_characters": ["@", "$", "%", "!"],
                        "mostly": 1,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_allowed_special_character_list",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_no_special_character",
                        "allowed_characters": ["@"],
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 2],
                        "unexpected_list": ["pear$!", "%banana%"],
                    },
                },
                {
                    "title": "positive_test_with_allowed_special_character_set",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_no_special_character",
                        "allowed_characters": {"@", "$", "%", "!"},
                        "mostly": 1,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_allowed_special_character_set",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_no_special_character",
                        "allowed_characters": {"@"},
                        "mostly": 1,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 2],
                        "unexpected_list": ["pear$!", "%banana%"],
                    },
                },
            ],
        },
    ]
    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "experimental expectation",
            "column map expectation",
            "special characters",
        ],
        "contributors": ["@jaibirsingh", "@calvingdu"],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in the Metric class above
    map_metric = "column_values.not_contain_special_character"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("mostly", "allowed_characters")

    default_kwarg_values = {"mostly": 1, "allowed_characters": []}

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        return True

    # This method defines a prescriptive Renderer
    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
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
            [
                "column",
                "allowed_characters",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        template_str = "values must not contain special characters"
        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )

            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
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


if __name__ == "__main__":
    ExpectColumnValuesToNotContainSpecialCharacters().print_diagnostic_checklist()
