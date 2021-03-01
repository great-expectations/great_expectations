from typing import Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.util import render_evaluation_parameter_string

import json
import re  # regular expressions

# !!! This giant block of imports should be something simpler, such as:
from great_expectations import *
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

class ColumnValuesContainSecurePasswords(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.secure_password"
    condition_value_keys = ("min_length", "min_uppercase", "min_lowercase", "min_special", "min_digits",
                            "max_consec_numbers", "max_consec_letters")

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
        def _pandas(cls, column, min_length, min_uppercase, min_lowercase, min_special, min_digits, max_consec_numbers, max_consec_letters, **kwargs):
        def matches_password_requirements(x):
            x = str(x)
            if len(x) < min_length:
                return False
            uppercase_letters = 0
            lowercase_letters = 0
            special_characters = 0
            num_digits = 0
            for char in x:
                if char.isdigit():
                    num_digits += 1
                elif char.isupper():
                    uppercase_letters += 1
                elif char.islower():
                    lowercase_letters += 1
                else:
                    special_characters += 1
            consec_numbers = 0
            consec_letters = 0
            max_numbers = 0
            max_letters = 0
            for char in x:
                if char.isdigit():
                    if consec_letters > 0 and consec_letters > max_letters:
                        max_letters = consec_letters
                    consec_letters = 0
                    consec_numbers += 1
                elif char.isalpha():
                    if consec_numbers > 0 and consec_numbers > max_numbers:
                        max_numbers = consec_numbers
                    consec_numbers = 0
                    consec_letters += 1
                else:
                    if consec_letters > 0 and consec_letters > max_letters:
                        max_letters = consec_letters
                    elif consec_numbers > 0 and consec_numbers > max_numbers:
                        max_numbers = consec_numbers
                    consec_numbers = 0
                    consec_letters = 0
            return not (uppercase_letters < min_uppercase or lowercase_letters < min_lowercase or special_characters < min_special or num_digits < min_digits or max_numbers > max_consec_numbers or max_letters > max_consec_letters)
        return column.apply(lambda x: matches_password_requirements(x) if x else False)


class ExpectColumnValuesToBeSecurePasswords(ColumnMapExpectation):
    """Expect column entries to be strings that match a given regular expression.

    Valid matches can be found \
    anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", \
    "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".

    expect_column_values_to_match_regex is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex (str): \
            The regular expression the column entries should match.

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
        :func:`expect_column_values_to_not_match_regex \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_not_match_regex>`

        :func:`expect_column_values_to_match_regex_list \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_match_regex_list>`

    """

    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation"],
        "package": "experimental_expectations",
        "contributors": [
            "@spencerhardwick",
            "@aworld1",
            "@carolli014",
        ],
        "requirements": [],
    }

    map_metric = "column_values.secure_password"
    success_keys = (
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
        return True

    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        #password = configuration.kwargs.get("password")
        mostly = "{:.2%}".format(float(configuration.kwargs.get("mostly", 1)))

        return f'Are at least {mostly} of all values in column "{column}" secure passwords?'

    @classmethod
    @renderer(renderer_type="renderer.answer")
    def _answer_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        #password = result.expectation_config.kwargs.get("password")
        if result.success:
            return f'All values in column "{column}" are secure passwords.'
        else:
            return f'Not all values in column "{column}" are secure passwords.'

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
            ["column", "mostly", "row_condition", "condition_parser"],
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

    examples = [
        {
            "data": {
                "fail_case_1": ["AAAAAAAA", "asdf5hu!", "a!s4D"],
                "pass_case_1": ["Asd454s!DFG", "asdDS54254!*@", "RTYfgh%^$&38"],
            },
            "tests": [
                {
                    "title": "pass_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "pass_case_1"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "fail_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_1"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [],
                    },
                },
            ],
        }
    ]
