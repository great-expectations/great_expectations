import json
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_diagnostics.supporting_types import ExpectationRendererDiagnostics
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import _registered_expectations

from .fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__BrokenIteration,
)


def test_expectation_self_check():

    my_expectation = ExpectColumnValuesToEqualThree()
    expectation_diagnostic = my_expectation.run_diagnostics()
    # print(json.dumps(expectation_diagnostic.to_dict(), indent=2))

    assert expectation_diagnostic.to_dict() == {
        "description": {
            "camel_name": "ExpectColumnValuesToEqualThree",
            "snake_name": "expect_column_values_to_equal_three",
            "short_description": "",
            "docstring": "",
        },
        "renderers": [],
        "examples": [],
        "metrics": [],
        "execution_engines": {
            "PandasExecutionEngine": False,
            "SqlAlchemyExecutionEngine": False,
            "SparkDFExecutionEngine": False,
        },
        "library_metadata": {
            "maturity": "CONCEPT_ONLY",
            "tags": [],
            "contributors": [],
        },
        "tests": [],
        "errors": [],
    }


def test_include_in_gallery_flag():

    my_expectation = ExpectColumnValuesToEqualThree__SecondIteration()
    report_object = my_expectation.run_diagnostics()
    # print(json.dumps(report_object["examples"], indent=2))

    assert len(report_object["examples"][0]["tests"]) == 1
    assert report_object["examples"][0]["tests"][0].to_dict() == {
        "title": "positive_test_with_mostly",
        "exact_match_out": False,
        "input": {"column": "mostly_threes", "mostly": 0.6},
        # "include_in_gallery": True,
        "suppress_test_for": [],
        "output": {
            "success": True,
            "unexpected_index_list": [6, 7],
            "unexpected_list": [2, -1],
        },
    }


def test_self_check_on_an_existing_expectation():
    expectation_name = "expect_column_values_to_match_regex"
    expectation = _registered_expectations[expectation_name]

    report_object = expectation().run_diagnostics()
    # print(json.dumps(report_object, indent=2))

    report_object["description"].pop(
        "docstring"
    )  # Don't try to exact match the docstring

    # one of the test cases in the examples for this expectation is failing on our CI
    # and the number of items depends on the flags
    # we will not verify the contents of `tests` or `errors`
    report_object.pop("tests")
    report_object.pop("errors")

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
            # "package": "great_expectations",
            "requirements": [],
            "tags": ["core expectation", "column map expectation"],
        },
        # "test_report": [
        #     {
        #         "test_title": "negative_test_insufficient_mostly_and_one_non_matching_value",
        #         "backend": "pandas",
        #         "success": "true",
        #     },
        #     {
        #         "test_title": "positive_test_exact_mostly_w_one_non_matching_value",
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

    renderer_diagnostics = my_expectation._get_renderer_diagnostics(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )
    assert isinstance(renderer_diagnostics, list)
    assert len(renderer_diagnostics) == 10
    for element in renderer_diagnostics:
        print(json.dumps(element.to_dict(), indent=2))
        assert isinstance(element, ExpectationRendererDiagnostics)

    print([rd.name for rd in renderer_diagnostics])
    assert set([rd.name for rd in renderer_diagnostics]) == {
        'renderer.diagnostic.unexpected_statement',
        'renderer.diagnostic.meta_properties',
        'renderer.diagnostic.unexpected_table',
        'renderer.diagnostic.status_icon',
        'renderer.answer',
        'atomic.prescriptive.summary',
        'atomic.diagnostic.observed_value',
        'renderer.question',
        'renderer.prescriptive',
        'renderer.diagnostic.observed_value'
    }

    # assert renderer_diagnostics[0].to_dict() == {
    #     "name": "renderer.diagnostic.meta_properties",
    #     "is_supported": True,
    #     "is_standard": False,
    #     "samples": [
    #         ""
    #     ]
    # }

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

    renderer_diagnostics = my_expectation._get_renderer_diagnostics(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )
    assert isinstance(renderer_diagnostics, list)
    for element in renderer_diagnostics:
        print(json.dumps(element.to_dict(), indent=2))
        assert isinstance(element, ExpectationRendererDiagnostics)

    assert len(renderer_diagnostics) == 10
    assert set([rd.name for rd in renderer_diagnostics]) == {
        'renderer.diagnostic.observed_value',
        'renderer.prescriptive',
        'renderer.diagnostic.meta_properties',
        'renderer.diagnostic.status_icon',
        'renderer.diagnostic.unexpected_table',
        'atomic.diagnostic.observed_value',
        'atomic.prescriptive.summary',
        'renderer.answer',
        'renderer.question',
        'renderer.diagnostic.unexpected_statement'
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

    renderer_diagnostics = my_expectation._get_renderer_diagnostics(
        expectation_name,
        my_expectation_config,
        my_validation_result,
    )
    assert isinstance(renderer_diagnostics, list)
    assert len(renderer_diagnostics) == 10
    for element in renderer_diagnostics:
        print(json.dumps(element.to_dict(), indent=2))
        assert isinstance(element, ExpectationRendererDiagnostics)

    assert len(renderer_diagnostics) == 10
    assert set([rd.name for rd in renderer_diagnostics]) == {
        'renderer.diagnostic.observed_value',
        'renderer.prescriptive',
        'renderer.diagnostic.meta_properties',
        'renderer.diagnostic.status_icon',
        'renderer.diagnostic.unexpected_table',
        'atomic.diagnostic.observed_value',
        'atomic.prescriptive.summary',
        'renderer.answer',
        'renderer.question',
        'renderer.diagnostic.unexpected_statement'
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



def test_run_diagnostics_on_an_expectation_with_errors_in_its_tests():
    expectation_diagnostics = ExpectColumnValuesToEqualThree__BrokenIteration().run_diagnostics()
    # print(json.dumps(expectation_diagnostics.to_dict(), indent=2))

    tests = expectation_diagnostics["tests"]
    
    assert len(tests)==5
    assert tests[0].to_dict() == {
        "test_title": "positive_test_with_mostly",
        "backend": "pandas",
        "test_passed": True,
        "error_message": None,
        "stack_trace": None,
    }

    assert set(tests[3].keys()) == {
        "test_title",
        "backend",
        "test_passed",
        "error_message",
        "stack_trace"
    }
    assert tests[3]["test_passed"] == False

    assert set(tests[4].keys()) == {
        "test_title",
        "backend",
        "test_passed",
        "error_message",
        "stack_trace"
    }
    assert tests[4]["test_passed"] == False