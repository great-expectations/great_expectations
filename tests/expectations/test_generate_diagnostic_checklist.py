import json

from .fixtures.expect_column_values_to_equal_three import (
    ExpectColumnValuesToEqualThree,
    ExpectColumnValuesToEqualThree__SecondIteration,
    ExpectColumnValuesToEqualThree__ThirdIteration,
    ExpectColumnValuesToEqualThree__BrokenIteration,
)


def test__convert_checks_into_output_message():
    checks = [
        {
            "message": "AAA",
            "passed": True,
        },
        {
            "message": "BBB",
            "passed": False,
        },
        {
            "message": "CCC",
            "passed": False,
            "sub_messages": [
                {
                    "message": "ddd",
                    "passed": True,
                },
                {
                    "message": "eee",
                    "passed": False,
                },
            ],
        },
    ]

    assert (
        ExpectColumnValuesToEqualThree()._convert_checks_into_output_message(checks)
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
 ✔ AAA
   BBB
   CCC
    ✔ ddd
      eee
"""
    )


def test__count_unexpected_cases_and_get_sub_messages__with_everything_passing():
    test_report = [
        {
            "test title": "positive_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true",
        },
        {
            "test title": "negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true",
        },
        {
            "test title": "other_negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true",
        },
    ]
    print(
        ExpectColumnValuesToEqualThree()._count_unexpected_cases_and_get_sub_messages(
            test_report
        )
    )
    assert (
        ExpectColumnValuesToEqualThree()._count_unexpected_cases_and_get_sub_messages(
            test_report
        )
        == (
            0,
            [
                {"message": "positive_test_with_mostly", "passed": True},
                {"message": "negative_test_with_mostly", "passed": True},
                {"message": "other_negative_test_with_mostly", "passed": True},
            ],
        )
    )


def test__count_unexpected_cases_and_get_sub_messages__with_one_failure():
    test_report = [
        {
            "test title": "positive_test_with_mostly",
            "backend": "pandas",
            "test_passed": "false",
        },
        {
            "test title": "negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true",
        },
        {
            "test title": "other_negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true",
        },
    ]
    print(
        ExpectColumnValuesToEqualThree()._count_unexpected_cases_and_get_sub_messages(
            test_report
        )
    )
    assert (
        ExpectColumnValuesToEqualThree()._count_unexpected_cases_and_get_sub_messages(
            test_report
        )
        == (
            1,
            [
                {"message": "positive_test_with_mostly", "passed": False},
                {"message": "negative_test_with_mostly", "passed": True},
                {"message": "other_negative_test_with_mostly", "passed": True},
            ],
        )
    )

def test__count_unexpected_cases_and_get_sub_messages__with_an_error():
    test_report = [
        {
            "test title": "positive_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true"
        },
        {
            "test title": "negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "true"
        },
        {
            "test title": "other_negative_test_with_mostly",
            "backend": "pandas",
            "test_passed": "false"
        },
        {
            "test title": "test_that_will_error_out",
            "backend": "pandas",
            "test_passed": "false",
            "error_message": "Error: The column \"column_that_doesnt_exist\" in BatchData does not exist.",
            "stack_trace": "Traceback (most recent call last):\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py\", line 368, in resolve_metrics\n    **metric_provider_kwargs\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/metric_provider.py\", line 55, in inner_func\n    return metric_fn(*args, **kwargs)\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/map_metric_provider.py\", line 327, in inner_func\n    message=f'Error: The column \"{column_name}\" in BatchData does not exist.'\ngreat_expectations.exceptions.exceptions.InvalidMetricAccessorDomainKwargsKeyError: Error: The column \"column_that_doesnt_exist\" in BatchData does not exist.\n\nDuring handling of the above exception, another exception occurred:\n\nTraceback (most recent call last):\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py\", line 1286, in _get_test_results\n    test=exp_test[\"test\"],\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/self_check/util.py\", line 1904, in evaluate_json_test_cfe\n    result = getattr(validator, expectation_type)(**runtime_kwargs)\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 347, in inst_expectation\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 308, in inst_expectation\n    runtime_configuration=basic_runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py\", line 830, in validate\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 572, in graph_validate\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 523, in graph_validate\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 750, in resolve_validation_graph\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 720, in resolve_validation_graph\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 1634, in _resolve_metrics\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py\", line 372, in resolve_metrics\n    message=str(e), failed_metrics=(metric_to_resolve,)\ngreat_expectations.exceptions.exceptions.MetricResolutionError: Error: The column \"column_that_doesnt_exist\" in BatchData does not exist.\n"
        },
        {
            "test title": "another_test_that_will_error_out",
            "backend": "pandas",
            "test_passed": "false",
            "error_message": "Error: The column \"broken_column\" in BatchData does not exist.",
            "stack_trace": "Traceback (most recent call last):\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py\", line 368, in resolve_metrics\n    **metric_provider_kwargs\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/metric_provider.py\", line 55, in inner_func\n    return metric_fn(*args, **kwargs)\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/metrics/map_metric_provider.py\", line 327, in inner_func\n    message=f'Error: The column \"{column_name}\" in BatchData does not exist.'\ngreat_expectations.exceptions.exceptions.InvalidMetricAccessorDomainKwargsKeyError: Error: The column \"broken_column\" in BatchData does not exist.\n\nDuring handling of the above exception, another exception occurred:\n\nTraceback (most recent call last):\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py\", line 1286, in _get_test_results\n    test=exp_test[\"test\"],\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/self_check/util.py\", line 1904, in evaluate_json_test_cfe\n    result = getattr(validator, expectation_type)(**runtime_kwargs)\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 347, in inst_expectation\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 308, in inst_expectation\n    runtime_configuration=basic_runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/expectations/expectation.py\", line 830, in validate\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 572, in graph_validate\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 523, in graph_validate\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 750, in resolve_validation_graph\n    raise err\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 720, in resolve_validation_graph\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/validator/validator.py\", line 1634, in _resolve_metrics\n    runtime_configuration=runtime_configuration,\n  File \"/Users/abe/Documents/superconductive/tools/great_expectations/great_expectations/execution_engine/execution_engine.py\", line 372, in resolve_metrics\n    message=str(e), failed_metrics=(metric_to_resolve,)\ngreat_expectations.exceptions.exceptions.MetricResolutionError: Error: The column \"broken_column\" in BatchData does not exist.\n"
        }
    ]
    assert (
        ExpectColumnValuesToEqualThree()._count_unexpected_cases_and_get_sub_messages(
            test_report
        )
        == (
            3,
            [
                {"message": "positive_test_with_mostly", "passed": True},
                {"message": "negative_test_with_mostly", "passed": True},
                {"message": "other_negative_test_with_mostly", "passed": False},
                {"message": "test_that_will_error_out", "passed": False},
                {"message": "another_test_that_will_error_out", "passed": False},
            ],
        )
    )


def test__count_positive_and_negative_example_cases():
    assert (
        ExpectColumnValuesToEqualThree()._count_positive_and_negative_example_cases()
        == (0, 0)
    )
    assert ExpectColumnValuesToEqualThree__SecondIteration()._count_positive_and_negative_example_cases() == (
        1,
        2,
    )


def test_generate_diagnostic_checklist__first_iteration():
    output_message = ExpectColumnValuesToEqualThree().generate_diagnostic_checklist()
    print(output_message)

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree:
   library_metadata object exists
   Has a docstring, including a one-line short description
   Has at least one positive and negative example case, and all test cases pass
   Core logic exists and passes tests on at least one Execution Engine
"""
    )


def test_generate_diagnostic_checklist__second_iteration():
    output_message = (
        ExpectColumnValuesToEqualThree__SecondIteration().generate_diagnostic_checklist()
    )
    print(output_message)

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree__SecondIteration:
 ✔ library_metadata object exists
 ✔ Has a docstring, including a one-line short description
    ✔ "Expect values in this column to equal the number three."
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""
    )


def test_generate_diagnostic_checklist__third_iteration():
    output_message = (
        ExpectColumnValuesToEqualThree__ThirdIteration().generate_diagnostic_checklist()
    )
    print(output_message)

    assert (
        output_message
        == """\
Completeness checklist for ExpectColumnValuesToEqualThree__ThirdIteration:
 ✔ library_metadata object exists
   Has a docstring, including a one-line short description
 ✔ Has at least one positive and negative example case, and all test cases pass
 ✔ Core logic exists and passes tests on at least one Execution Engine
"""
    )
