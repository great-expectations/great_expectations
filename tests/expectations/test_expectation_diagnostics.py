import pytest

from great_expectations.core.expectation_diagnostics.expectation_diagnostics import (
    ExpectationDiagnostics,
)
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
    ExpectationLegacyTestCaseAdapter,
    ExpectationTestCase,
    ExpectationTestDataCases,
    TestData,
)
from great_expectations.core.expectation_diagnostics.supporting_types import (
    AugmentedLibraryMetadata,
    ExpectationDescriptionDiagnostics,
    ExpectationDiagnosticCheckMessage,
    ExpectationDiagnosticMaturityMessages,
    ExpectationExecutionEngineDiagnostics,
    ExpectationRendererDiagnostics,
    ExpectationTestDiagnostics,
    RendererTestDiagnostics,
)

expectation_test_data_case = ExpectationTestDataCases(
    data=TestData(
        **{
            "a": ["aaa", "abb", "acc", "add", "bee"],
            "b": ["aaa", "abb", "acc", "bdd", None],
            "column_name with space": ["aaa", "abb", "acc", "add", "bee"],
        }
    ),
    tests=[
        ExpectationLegacyTestCaseAdapter(
            **{
                "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
                "exact_match_out": False,
                "in": {"column": "a", "regex": "^a", "mostly": 0.9},
                "out": {
                    "success": False,
                    "unexpected_index_list": [4],
                    "unexpected_list": ["bee"],
                },
                "suppress_test_for": ["sqlite", "mssql"],
            }
        ),
        ExpectationLegacyTestCaseAdapter(
            **{
                "title": "positive_test_exact_mostly_w_one_non_matching_value",
                "exact_match_out": False,
                "in": {"column": "a", "regex": "^a", "mostly": 0.8},
                "out": {
                    "success": True,
                    "unexpected_index_list": [4],
                    "unexpected_list": ["bee"],
                },
                "suppress_test_for": ["sqlite", "mssql"],
            }
        ),
    ],
)

edr = ExpectationDiagnostics(
    description=ExpectationDescriptionDiagnostics(
        **{
            "camel_name": "ExpectColumnValuesToMatchRegex",
            "snake_name": "expect_column_values_to_match_regex",
            "short_description": "Expect column entries to be strings that match a given regular expression.",
            "docstring": 'Expect column entries to be strings that match a given regular expression.\n    \n    Valid matches can be found     anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat",     "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".\n\n    expect_column_values_to_match_regex is a     :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine\n    .column_map_expectation>`.\n\n    Args:\n        column (str):             The column name.\n        regex (str):             The regular expression the column entries should match.\n\n    Keyword Args:\n        mostly (None or a float between 0 and 1):             Return `"success": True` if at least mostly fraction of values match the expectation.             For more detail, see :ref:`mostly`.\n\n    Other Parameters:\n        result_format (str or None):             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.\n            For more detail, see :ref:`result_format <result_format>`.\n        include_config (boolean):             If True, then include the expectation config as part of the result object.             For more detail, see :ref:`include_config`.\n        catch_exceptions (boolean or None):             If True, then catch exceptions and include them as part of the result object.             For more detail, see :ref:`catch_exceptions`.\n        meta (dict or None):             A JSON-serializable dictionary (nesting allowed) that will be included in the output without             modification. For more detail, see :ref:`meta`.\n\n    Returns:\n        An ExpectationSuiteValidationResult\n\n        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and\n        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.\n\n    See Also:\n        :func:`expect_column_values_to_not_match_regex         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_not_match_regex>`\n\n        :func:`expect_column_values_to_match_regex_list         <great_expectations.execution_engine.execution_engine.ExecutionEngine\n        .expect_column_values_to_match_regex_list>`\n\n    ',
        }
    ),
    library_metadata=AugmentedLibraryMetadata(
        **{
            "maturity": "PRODUCTION",
            # "package": "great_expectations",
            "tags": [
                "arrows",
                "design",
                "flows",
            ],
            "contributors": ["@shinnyshinshin", "@abegong"],
            "requirements": [],
            "library_metadata_passed_checks": True,
        }
    ),
    renderers=[
        ExpectationRendererDiagnostics(
            name="renderer.prescriptive",
            is_supported=True,
            is_standard=True,
            samples=[
                RendererTestDiagnostics(
                    test_title="basic_positive_test",
                    rendered_successfully=True,
                    renderered_str="a values must match this regular expression: ^a, at least 90 % of the time.",
                )
            ],
        )
    ],
    examples=[expectation_test_data_case],
    gallery_examples=[expectation_test_data_case],
    metrics=[],
    execution_engines={
        "PandasExecutionEngine": True,
        "SqlAlchemyExecutionEngine": True,
        "SparkDFExecutionEngine": True,
    },
    tests=[],
    errors=[],
    maturity_checklist=ExpectationDiagnosticMaturityMessages(
        experimental=[
            ExpectationDiagnosticCheckMessage(
                message="AAA",
                passed=True,
            ),
            ExpectationDiagnosticCheckMessage(
                message="BBB",
                passed=False,
            ),
        ],
        beta=[],
        production=[
            ExpectationDiagnosticCheckMessage(
                message="CCC",
                passed=False,
                sub_messages=[
                    ExpectationDiagnosticCheckMessage(
                        message="ddd",
                        passed=True,
                    ),
                    ExpectationDiagnosticCheckMessage(
                        message="eee",
                        passed=False,
                    ),
                ],
            ),
        ],
    ),
)


def test__convert_checks_into_output_message():
    assert (
        edr._convert_checks_into_output_message(
            class_name="ExpectColumnValuesToEqualThree",
            maturity_messages=edr.maturity_checklist,
        )
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
 ✔ AAA
   BBB
   CCC
    ✔ ddd
      eee
"""
    )


def test__count_unexpected_test_cases___with_everything_passing():
    tests = [
        ExpectationTestDiagnostics(
            test_title="positive_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
        ExpectationTestDiagnostics(
            test_title="negative_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
        ExpectationTestDiagnostics(
            test_title="other_negative_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
    ]
    assert edr._count_unexpected_test_cases(tests) == 0


def test__count_unexpected_test_cases__with_one_failure():
    tests = [
        ExpectationTestDiagnostics(
            test_title="positive_test_with_mostly",
            backend="pandas",
            test_passed=False,
        ),
        ExpectationTestDiagnostics(
            test_title="negative_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
        ExpectationTestDiagnostics(
            test_title="other_negative_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
    ]
    assert edr._count_unexpected_test_cases(tests) == 1


def test__count_unexpected_test_cases__with_an_error():
    tests = [
        ExpectationTestDiagnostics(
            test_title="positive_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
        ExpectationTestDiagnostics(
            test_title="negative_test_with_mostly",
            backend="pandas",
            test_passed=True,
        ),
        ExpectationTestDiagnostics(
            test_title="other_negative_test_with_mostly",
            backend="pandas",
            test_passed=False,
        ),
        ExpectationTestDiagnostics(
            test_title="test_that_will_error_out",
            backend="pandas",
            test_passed=False,
            error_message='Error: The column "column_that_doesnt_exist" in BatchData does not exist.',
            stack_trace='Traceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py", line 368, in resolve_metrics\n    **metric_provider_kwargs\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/metric_provider.py", line 55, in inner_func\n    return metric_fn(*args, **kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/map_metric_provider.py", line 327, in inner_func\n    message=f\'Error: The column "{column_name}" in BatchData does not exist.\'\ngreat_expectations.exceptions.exceptions.InvalidMetricAccessorDomainKwargsKeyError: Error: The column "column_that_doesnt_exist" in BatchData does not exist.\n\nDuring handling of the above exception, another exception occurred:\n\nTraceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py", line 1286, in _get_test_results\n    test=exp_test["test"],\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/self_check/util.py", line 1904, in evaluate_json_test_cfe\n    result = getattr(validator, expectation_type)(**runtime_kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 347, in inst_expectation\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 308, in inst_expectation\n    runtime_configuration=basic_runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py", line 830, in validate\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 572, in graph_validate\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 523, in graph_validate\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 750, in resolve_validation_graph\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 720, in resolve_validation_graph\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 1634, in _resolve_metrics\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py", line 372, in resolve_metrics\n    message=str(e), failed_metrics=(metric_to_resolve,)\ngreat_expectations.exceptions.exceptions.MetricResolutionError: Error: The column "column_that_doesnt_exist" in BatchData does not exist.\n',
        ),
        ExpectationTestDiagnostics(
            test_title="another_test_that_will_error_out",
            backend="pandas",
            test_passed=False,
            error_message='Error: The column "broken_column" in BatchData does not exist.',
            stack_trace='Traceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py", line 368, in resolve_metrics\n    **metric_provider_kwargs\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/metric_provider.py", line 55, in inner_func\n    return metric_fn(*args, **kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/map_metric_provider.py", line 327, in inner_func\n    message=f\'Error: The column "{column_name}" in BatchData does not exist.\'\ngreat_expectations.exceptions.exceptions.InvalidMetricAccessorDomainKwargsKeyError: Error: The column "broken_column" in BatchData does not exist.\n\nDuring handling of the above exception, another exception occurred:\n\nTraceback (most recent call last):\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py", line 1286, in _get_test_results\n    test=exp_test["test"],\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/self_check/util.py", line 1904, in evaluate_json_test_cfe\n    result = getattr(validator, expectation_type)(**runtime_kwargs)\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 347, in inst_expectation\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 308, in inst_expectation\n    runtime_configuration=basic_runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py", line 830, in validate\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 572, in graph_validate\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 523, in graph_validate\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 750, in resolve_validation_graph\n    raise err\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 720, in resolve_validation_graph\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py", line 1634, in _resolve_metrics\n    runtime_configuration=runtime_configuration,\n  File "/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py", line 372, in resolve_metrics\n    message=str(e), failed_metrics=(metric_to_resolve,)\ngreat_expectations.exceptions.exceptions.MetricResolutionError: Error: The column "broken_column" in BatchData does not exist.\n',
        ),
    ]
    assert edr._count_unexpected_test_cases(tests) == 3


def test__count_positive_and_negative_example_cases():
    assert edr._count_positive_and_negative_example_cases(
        [expectation_test_data_case]
    ) == (1, 1)


def test__check_core_logic_for_at_least_one_execution_engine__with_all_execution_engines():
    assert ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
        execution_engines=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=True,
            SparkDFExecutionEngine=True,
            SqlAlchemyExecutionEngine=True,
        ),
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has core logic and passes tests on at least one Execution Engine",
            "passed": True,
        }
    )


def test__check_core_logic_for_at_least_one_execution_engine__with_no_execution_engines():
    assert ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
        execution_engines=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=False,
            SparkDFExecutionEngine=False,
            SqlAlchemyExecutionEngine=False,
        ),
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has core logic and passes tests on at least one Execution Engine",
            "passed": False,
        }
    )


def test__check_core_logic_for_at_least_one_execution_engine__with_only_pandas():
    assert ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
        execution_engines=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=True,
            SparkDFExecutionEngine=False,
            SqlAlchemyExecutionEngine=False,
        ),
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has core logic and passes tests on at least one Execution Engine",
            "passed": True,
        }
    )


def test__check_core_logic_for_at_least_one_execution_engine__with_only_sql():
    assert ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
        execution_engines=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=False,
            SparkDFExecutionEngine=True,
            SqlAlchemyExecutionEngine=False,
        ),
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has core logic and passes tests on at least one Execution Engine",
            "passed": True,
        }
    )


def test__check_core_logic_for_at_least_one_execution_engine__with_only_spark():
    assert ExpectationDiagnostics._check_core_logic_for_at_least_one_execution_engine(
        execution_engines=ExpectationExecutionEngineDiagnostics(
            PandasExecutionEngine=False,
            SparkDFExecutionEngine=False,
            SqlAlchemyExecutionEngine=True,
        ),
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has core logic and passes tests on at least one Execution Engine",
            "passed": True,
        }
    )


def test__check_example_cases__with_enough_test_cases_but_all_failing():
    assert ExpectationDiagnostics._check_example_cases(
        examples=[expectation_test_data_case],
        tests=[
            ExpectationTestDiagnostics(
                test_title="positive_test_exact_mostly_w_one_non_matching_value",
                backend="pandas",
                test_passed=False,
            ),
            ExpectationTestDiagnostics(
                test_title="negative_test_insufficient_mostly_and_one_non_matching_value",
                backend="pandas",
                test_passed=False,
            ),
        ],
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has at least one positive and negative example case, and all test cases pass",
            "passed": False,
        }
    )


def test__check_example_cases__with_enough_test_cases_but_some_failing():
    assert ExpectationDiagnostics._check_example_cases(
        examples=[expectation_test_data_case],
        tests=[
            ExpectationTestDiagnostics(
                test_title="positive_test_exact_mostly_w_one_non_matching_value",
                backend="pandas",
                test_passed=True,
            ),
            ExpectationTestDiagnostics(
                test_title="negative_test_insufficient_mostly_and_one_non_matching_value",
                backend="pandas",
                test_passed=False,
            ),
        ],
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has at least one positive and negative example case, and all test cases pass",
            "passed": False,
        }
    )


def test__check_example_cases__with_enough_test_cases_and_no_failing():
    assert ExpectationDiagnostics._check_example_cases(
        examples=[expectation_test_data_case],
        tests=[
            ExpectationTestDiagnostics(
                test_title="positive_test_exact_mostly_w_one_non_matching_value",
                backend="pandas",
                test_passed=True,
            ),
            ExpectationTestDiagnostics(
                test_title="negative_test_insufficient_mostly_and_one_non_matching_value",
                backend="pandas",
                test_passed=True,
            ),
        ],
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has at least one positive and negative example case, and all test cases pass",
            "passed": True,
        }
    )


def test__check_example_cases__with_enough_not_enough_test_cases_but_no_failing():
    expectation_test_data_case = ExpectationTestDataCases(
        data=TestData(
            **{
                "a": ["aaa", "abb", "acc", "add", "bee"],
                "b": ["aaa", "abb", "acc", "bdd", None],
                "column_name with space": ["aaa", "abb", "acc", "add", "bee"],
            }
        ),
        tests=[
            ExpectationLegacyTestCaseAdapter(
                **{
                    "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
                    "exact_match_out": False,
                    "in": {"column": "a", "regex": "^a", "mostly": 0.9},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [4],
                        "unexpected_list": ["bee"],
                    },
                    "suppress_test_for": ["sqlite", "mssql"],
                }
            ),
        ],
    )

    assert ExpectationDiagnostics._check_example_cases(
        examples=[expectation_test_data_case],
        tests=[
            ExpectationTestDiagnostics(
                test_title="negative_test_insufficient_mostly_and_one_non_matching_value",
                backend="pandas",
                test_passed=True,
            ),
        ],
    ) == ExpectationDiagnosticCheckMessage(
        **{
            "message": "Has at least one positive and negative example case, and all test cases pass",
            "passed": False,
        }
    )
