import json
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from polyglot.detect import Detector

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


def detect_language(val):
    return Detector(val, quiet=True).language.code


class ColumnValuesDetectLanguage(ColumnMapMetricProvider):
    condition_metric_name = "column_values.value_language.equals"
    function_metric_name = "column_values.value_language"

    condition_value_keys = ("language",)

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas_function(cls, column, **kwargs):
        return column.astype(str).map(detect_language)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, language, _metrics, **kwargs):
        column_languages, _, _ = _metrics.get("column_values.value_language.map")
        return column_languages == language

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, language, **kwargs):
        def is_language(val):
            return val == language

        language_udf = F.udf(is_language, sparktypes.BooleanType())

        return language_udf(column)


class ExpectColumnValuesToMatchLanguage(ColumnMapExpectation):
    """Expect column entries to be strings in a given language.
    expect_column_values_to_equal_value_language is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.
    Args:
        column (str): \
            The column name.
        language (str): \
            A ISO 639-1 Code string to use for matching.
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

    Notes:
        * Language identification uses the [`polyglot` package](https://github.com/aboSamoor/polyglot).
        * `polyglot` uses a [GPLv3 license](https://github.com/saffsd/langid.py/blob/master/LICENSE).
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "mostly_english": [
                    "Twinkle, twinkle, little star. How I wonder what you are.",
                    "Up above the world so high, Like a diamond in the sky.",
                    "Twinkle, twinkle, little star. Up above the world so high.",
                    "Brilla brilla pequeña estrella. Cómo me pregunto lo que eres.",
                    None,
                ],
                "mostly_spanish": [
                    "Brilla brilla pequeña estrella. Cómo me pregunto lo que eres.",
                    "Por encima del mundo tan alto, Como un diamante en el cielo.",
                    "Brilla brilla pequeña estrella. Por encima del mundo tan arriba.",
                    "Twinkle, twinkle, little star. How I wonder what you are.",
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly_english",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_english", "language": "en", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": [4, 5],
                    },
                },
                {
                    "title": "negative_test_with_mostly_english",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_spanish", "language": "en", "mostly": 0.6},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [4, 5],
                    },
                },
                {
                    "title": "positive_test_with_mostly_spanish",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_spanish", "language": "es", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": [4, 5],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["nlp", "language"],
        "contributors": ["@mielvds"],
        "package": "experimental_expectations",
        "requirements": ["polyglot"],
    }

    map_metric = "column_values.match_language"
    success_keys = (
        "language",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "language" in configuration.kwargs, "language is required"
            assert isinstance(
                configuration.kwargs["language"], (str, dict)
            ), "language must be a string"
            if isinstance(configuration.kwargs["language"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["language"]
                ), 'Evaluation Parameter dict for language kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @classmethod
    @renderer(renderer_type="question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")
        language = configuration.kwargs.get("language")

        return f'Do at least {mostly * 100}% of values in column "{column}" match the language {language}?'

    @classmethod
    @renderer(renderer_type="answer")
    def _answer_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")
        language = result.expectation_config.kwargs.get("language")
        if result.success:
            return f'At least {mostly * 100}% of values in column "{column}" match the language {language}.'
        else:
            return f'Less than {mostly * 100}% of values in column "{column}" match the language {language}.'

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
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "language", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("language"):
            template_str = "values must match a language but none was specified."
        else:
            template_str = "values must match this language: $language"
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
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
    diagnostics_report = ExpectColumnValuesToMatchLanguage().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
