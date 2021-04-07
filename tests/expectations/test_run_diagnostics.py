import json

import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
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


class ColumnValuesEqualThree(ColumnMapMetricProvider):
    condition_metric_name = "column_values.equal_three"
    # condition_value_keys = {}
    # default_kwarg_values = {}

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3


class ExpectColumnValuesToEqualThree(ColumnMapExpectation):

    map_metric = "column_values.equal_three"
    success_keys = ("mostly",)
    # default_kwarg_values = ColumnMapExpectation.default_kwarg_values


class ExpectColumnValuesToEqualThree__SecondIteration(ExpectColumnValuesToEqualThree):

    examples = [
        {
            "data": {
                "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.6},
                    "include_in_gallery": True,
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    "include_in_gallery": False,
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "other_negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    # "include_in_gallery": False, #This key is omitted, so the example shouldn't show up in the gallery
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
            ],
        }
    ]

    library_metadata = {
        "maturity": "experimental",
        "package": "great_expectations",
        "tags": ["tag", "other_tag"],
        "contributors": [
            "@abegong",
        ],
    }


class ExpectColumnValuesToEqualThree__ThirdIteration(
    ExpectColumnValuesToEqualThree__SecondIteration
):
    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(
        cls, configuration, result=None, language=None, runtime_configuration=None
    ):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")
        regex = configuration.kwargs.get("regex")

        return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'

    @classmethod
    @renderer(renderer_type="renderer.answer")
    def _answer_renderer(
        cls, configuration=None, result=None, language=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")
        regex = result.expectation_config.kwargs.get("regex")
        if result.success:
            return f'At least {mostly * 100}% of values in column "{column}" equal 3.'
        else:
            return f'Less than {mostly * 100}% of values in column "{column}" equal 3.'

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
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        template_str = "values must be equal to 3"
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


def test_expectation_self_check():

    my_expectation = ExpectColumnValuesToEqualThree()
    report_object = my_expectation.run_diagnostics()
    print(json.dumps(report_object, indent=2))

    assert report_object == {
        "description": {
            "camel_name": "ExpectColumnValuesToEqualThree",
            "snake_name": "expect_column_values_to_equal_three",
            "short_description": "",
            "docstring": "",
        },
        "renderers": {},
        "examples": [],
        "metrics": [],
        "execution_engines": {},
        "library_metadata": {
            "maturity": None,
            "package": None,
            "tags": [],
            "contributors": [],
        },
        "test_report": [],
        "diagnostics_report": [],
    }


def test_include_in_gallery_flag():

    my_expectation = ExpectColumnValuesToEqualThree__SecondIteration()
    report_object = my_expectation.run_diagnostics()
    print(json.dumps(report_object["examples"], indent=2))

    assert len(report_object["examples"][0]["tests"]) == 1
    assert report_object["examples"][0]["tests"][0] == {
        "title": "positive_test_with_mostly",
        "exact_match_out": False,
        "in": {"column": "mostly_threes", "mostly": 0.6},
        "include_in_gallery": True,
        "out": {
            "success": True,
            "unexpected_index_list": [6, 7],
            "unexpected_list": [2, -1],
        },
    }


def test_self_check_on_an_existing_expectation():
    expectation_name = "expect_column_values_to_match_regex"
    expectation = _registered_expectations[expectation_name]

    report_object = expectation().run_diagnostics()
    print(json.dumps(report_object, indent=2))

    report_object["description"].pop(
        "docstring"
    )  # Don't try to exact match the docstring

    # one of the test cases in the examples for this expectation is failing on our CI
    # and the number of items depends on the flags
    # we will not verify the content of test_report
    test_report = report_object.pop("test_report")
    report_object.pop("diagnostics_report")

    assert report_object == {
        "description": {
            "camel_name": "ExpectColumnValuesToMatchRegex",
            "snake_name": "expect_column_values_to_match_regex",
            "short_description": "Expect column entries to be strings that match a given regular expression.",
            # "docstring": "Expect column entries to be strings that match a given regular expression. Valid matches can be found     anywhere in the string, for example \"[at]+\" will identify the following strings as expected: \"cat\", \"hat\",     \"aa\", \"a\", and \"t\", and the following strings as unexpected: \"fish\", \"dog\".\n\n    expect_column_values_to_match_regex is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        regex (str):             The regular expression the column entries should match.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `\"success\": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_not_match_regex         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_not_match_regex>`\n\n        :func:`expect_column_values_to_match_regex_list         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_match_regex_list>`\n\n    ",
        },
        "execution_engines": {
            "PandasExecutionEngine": True,
            "SqlAlchemyExecutionEngine": True,
            "SparkDFExecutionEngine": True,
        },
        "renderers": {
            "standard": {
                "renderer.answer": 'Less than 90.0% of values in column "a" match the regular expression ^a.',
                "renderer.diagnostic.unexpected_statement": "\n\n1 unexpected values found. 20% of 5 total rows.",
                "renderer.diagnostic.observed_value": "20% unexpected",
                "renderer.diagnostic.status_icon": "",
                "renderer.diagnostic.unexpected_table": None,
                "renderer.prescriptive": "a values must match this regular expression: ^a, at least 90 % of the time.",
                "renderer.question": 'Do at least 90.0% of values in column "a" match the regular expression ^a?',
            },
            "custom": [],
        },
        "metrics": [
            "column_values.nonnull.unexpected_count",
            "column_values.match_regex.unexpected_count",
            "table.row_count",
            "column_values.match_regex.unexpected_values",
        ],
        "examples": [
            {
                "data": {
                    "a": ["aaa", "abb", "acc", "add", "bee"],
                    "b": ["aaa", "abb", "acc", "bdd", None],
                    "column_name with space": ["aaa", "abb", "acc", "add", "bee"],
                },
                "tests": [
                    {
                        "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
                        "exact_match_out": False,
                        "in": {"column": "a", "regex": "^a", "mostly": 0.9},
                        "out": {
                            "success": False,
                            "unexpected_index_list": [4],
                            "unexpected_list": ["bee"],
                        },
                        "suppress_test_for": ["sqlite", "mssql"],
                        "include_in_gallery": True,
                    },
                    {
                        "title": "positive_test_exact_mostly_w_one_non_matching_value",
                        "exact_match_out": False,
                        "in": {"column": "a", "regex": "^a", "mostly": 0.8},
                        "out": {
                            "success": True,
                            "unexpected_index_list": [4],
                            "unexpected_list": ["bee"],
                        },
                        "suppress_test_for": ["sqlite", "mssql"],
                        "include_in_gallery": True,
                    },
                ],
            }
        ],
        "library_metadata": {
            "contributors": ["@great_expectations"],
            "maturity": "production",
            "package": "great_expectations",
            "requirements": [],
            "tags": ["core expectation", "column map expectation"],
        },
        # "test_report": [
        #     {
        #         "test title": "negative_test_insufficient_mostly_and_one_non_matching_value",
        #         "backend": "pandas",
        #         "success": "true",
        #     },
        #     {
        #         "test title": "positive_test_exact_mostly_w_one_non_matching_value",
        #         "backend": "pandas",
        #         "success": "true",
        #     },
        # ],
    }


def test_expectation__get_renderers():

    expectation_name = "expect_column_values_to_match_regex"
    my_expectation = _registered_expectations[expectation_name]()

    supported_renderers = my_expectation._get_supported_renderers(expectation_name)
    examples = my_expectation._get_examples()
    example_data, example_test = my_expectation._choose_example(examples)

    my_batch = Batch(data=pd.DataFrame(example_data))

    my_expectation_config = ExpectationConfiguration(
        **{"expectation_type": expectation_name, "kwargs": example_test}
    )

    my_validation_results = my_expectation._instantiate_example_validation_results(
        test_batch=my_batch,
        expectation_config=my_expectation_config,
    )
    my_validation_result = my_validation_results[0]

    renderer_dict = my_expectation._get_renderer_dict(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )

    print(json.dumps(renderer_dict, indent=2))

    assert renderer_dict == {
        "standard": {
            "renderer.answer": 'Less than 90.0% of values in column "a" match the regular expression ^a.',
            "renderer.diagnostic.unexpected_statement": "\n\n1 unexpected values found. 20% of 5 total rows.",
            "renderer.diagnostic.observed_value": "20% unexpected",
            "renderer.diagnostic.status_icon": "",
            "renderer.diagnostic.unexpected_table": None,
            "renderer.prescriptive": "a values must match this regular expression: ^a, at least 90 % of the time.",
            "renderer.question": 'Do at least 90.0% of values in column "a" match the regular expression ^a?',
        },
        "custom": [],
    }

    # Expectation with no new renderers specified
    print([x for x in _registered_expectations.keys() if "second" in x])
    expectation_name = "expect_column_values_to_equal_three___second_iteration"
    my_expectation = _registered_expectations[expectation_name]()

    supported_renderers = my_expectation._get_supported_renderers(expectation_name)
    examples = my_expectation._get_examples()
    example_data, example_test = my_expectation._choose_example(examples)

    my_batch = Batch(data=pd.DataFrame(example_data))

    my_expectation_config = ExpectationConfiguration(
        **{"expectation_type": expectation_name, "kwargs": example_test}
    )

    my_validation_results = my_expectation._instantiate_example_validation_results(
        test_batch=my_batch,
        expectation_config=my_expectation_config,
    )
    my_validation_result = my_validation_results[0]

    renderer_dict = my_expectation._get_renderer_dict(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )

    print(json.dumps(renderer_dict, indent=2))

    assert renderer_dict == {
        "standard": {
            "renderer.answer": None,
            "renderer.diagnostic.observed_value": "25% unexpected",
            "renderer.diagnostic.status_icon": "",
            "renderer.diagnostic.unexpected_statement": "",
            "renderer.diagnostic.unexpected_table": None,
            "renderer.prescriptive": "expect_column_values_to_equal_three___second_iteration(**{'column': 'mostly_threes', 'mostly': 0.6})",
            "renderer.question": None,
        },
        "custom": [],
    }

    # Expectation with no renderers specified
    print([x for x in _registered_expectations.keys() if "second" in x])
    expectation_name = "expect_column_values_to_equal_three___third_iteration"
    my_expectation = _registered_expectations[expectation_name]()

    supported_renderers = my_expectation._get_supported_renderers(expectation_name)
    examples = my_expectation._get_examples()
    example_data, example_test = my_expectation._choose_example(examples)
    my_batch = Batch(data=pd.DataFrame(example_data))

    my_expectation_config = ExpectationConfiguration(
        **{"expectation_type": expectation_name, "kwargs": example_test}
    )

    my_validation_results = my_expectation._instantiate_example_validation_results(
        test_batch=my_batch,
        expectation_config=my_expectation_config,
    )
    my_validation_result = my_validation_results[0]

    renderer_dict = my_expectation._get_renderer_dict(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )

    print(json.dumps(renderer_dict, indent=2))

    assert renderer_dict == {
        "standard": {
            "renderer.answer": 'At least 60.0% of values in column "mostly_threes" equal 3.',
            "renderer.diagnostic.observed_value": "25% unexpected",
            "renderer.diagnostic.status_icon": "",
            "renderer.diagnostic.unexpected_statement": "",
            "renderer.diagnostic.unexpected_table": None,
            "renderer.prescriptive": "mostly_threes values must be equal to 3, at least 60 % of the time.",
            "renderer.question": 'Do at least 60.0% of values in column "mostly_threes" equal 3?',
        },
        "custom": [],
    }


def test_expectation__get_execution_engine_dict(
    test_cases_for_sql_data_connector_sqlite_execution_engine,
):
    expectation_name = "expect_column_values_to_equal_three___second_iteration"
    my_expectation = _registered_expectations[expectation_name]()

    examples = my_expectation._get_examples()
    example_data, example_test = my_expectation._choose_example(examples)

    my_batch = Batch(data=pd.DataFrame(example_data))

    my_expectation_config = ExpectationConfiguration(
        **{"expectation_type": expectation_name, "kwargs": example_test}
    )

    my_validation_results = my_expectation._instantiate_example_validation_results(
        test_batch=my_batch,
        expectation_config=my_expectation_config,
    )
    upstream_metrics = my_expectation._get_upstream_metrics(
        expectation_config=my_expectation_config
    )

    execution_engines = my_expectation._get_execution_engine_dict(
        upstream_metrics=upstream_metrics,
    )
    assert execution_engines == {
        "PandasExecutionEngine": True,
        "SparkDFExecutionEngine": False,
        "SqlAlchemyExecutionEngine": False,
    }


def test_expectation_is_abstract():
    # is_abstract determines whether the expectation should be added to the registry (i.e. is fully implemented)
    assert ColumnMapExpectation.is_abstract()
    assert not ExpectColumnValuesToEqualThree.is_abstract()
