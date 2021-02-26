import json

import langid  # version 1.1.6

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validator import Validator


class ColumnValuesAreInLanguage(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.are_in_language"
    condition_value_keys = ("language",)

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, language, **kwargs):
        def identify_language(text):
            try:
                language, confidence = langid.classify(text)
            except Exception:
                language, confidence = None, None
            return {
                "label": language,
                "confidence": confidence,
            }

        labels = column.apply(lambda x: identify_language(x)["label"])
        return labels == language

    # # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     return column.in_([3])

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     return column.isin([3])


class ExpectColumnValuesAreInLanguage(ColumnMapExpectation):
    """Expect the column to be in a specified language.

        Args:
            column (str): \
                The column name
            language (str): \
                One of 97 ISO 639-1 language codes, e.g. af, am, an, ar, as, az, be, bg, bn, br, bs, ca, cs, cy, da, \
                de, dz, el, en, eo, es, et, eu, fa, fi, fo, fr, ga, gl, gu, he, hi, hr, ht, hu, hy, id, is, it, ja, \
                jv, ka, kk, km, kn, ko, ku, ky, la, lb, lo, lt, lv, mg, mk, ml, mn, mr, ms, mt, nb, ne, nl, nn, no, \
                oc, or, pa, pl, ps, pt, qu, ro, ru, rw, se, si, sk, sl, sq, sr, sv, sw, ta, te, th, tl, tr, ug, uk, \
                ur, vi, vo, wa, xh, zh, zu

        Notes:
            * Language identification uses the [`langid` package](https://github.com/saffsd/langid.py).
            * `langid` uses a custom, permissive [LICENSE](https://github.com/saffsd/langid.py/blob/master/LICENSE),
              suitable for commercial purposes.
            * Results may be inaccurate for strings shorter than 50 characters.
            * No confidence threshold has been set, so language with the highest confidence will be selected, even if
              confidence is low.
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
        "tags": ["nlp", "hackathon"],
        "contributors": ["@victorwyee"],
        "package": "experimental_expectations",
        "requirements": ["langid>=1.1.6"],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_in_language"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "language",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    #!!! This example renderer should render RenderedStringTemplateContent, not just a string


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesAreInLanguage().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
