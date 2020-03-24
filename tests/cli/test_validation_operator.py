# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_validation_operator_list_with_zero_validation_operators(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context._project_config.validation_operators = {}
    context._save_project_config()
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Validation Operators found" in result.output

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_one_validation_operator(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    expected_result = """\
1 Validation Operator found:[0m
[0m
 - [36mname:[0m action_list_operator[0m
   [36mclass_name:[0m ActionListValidationOperator[0m
   [36maction_list:[0m store_validation_result (StoreValidationResultAction) => store_evaluation_params (StoreEvaluationParametersAction) => update_data_docs (UpdateDataDocsAction)[0m"""

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_multiple_validation_operators(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)
    context = DataContext(project_dir)
    context.add_validation_operator(
        "my_validation_operator",
        {
            "class_name": "WarningAndFailureExpectationSuitesValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction"
                    }
                },
                {
                    "name": "store_evaluation_params",
                    "action": {
                        "class_name": "StoreEvaluationParametersAction"
                    }
                },
                {
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction"
                    }
                }
            ],
            "base_expectation_suite_name": "new-years-expectations",
            "slack_webhook": "https://hooks.slack.com/services/dummy"
        }
    )
    context._save_project_config()
    expected_result = """\
2 Validation Operators found:[0m
[0m
 - [36mname:[0m action_list_operator[0m
   [36mclass_name:[0m ActionListValidationOperator[0m
   [36maction_list:[0m store_validation_result (StoreValidationResultAction) => store_evaluation_params (StoreEvaluationParametersAction) => update_data_docs (UpdateDataDocsAction)[0m
[0m
 - [36mname:[0m my_validation_operator[0m
   [36mclass_name:[0m WarningAndFailureExpectationSuitesValidationOperator[0m
   [36maction_list:[0m store_validation_result (StoreValidationResultAction) => store_evaluation_params (StoreEvaluationParametersAction) => update_data_docs (UpdateDataDocsAction)[0m
   [36mbase_expectation_suite_name:[0m new-years-expectations[0m
   [36mslack_webhook:[0m https://hooks.slack.com/services/dummy[0m"""

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)