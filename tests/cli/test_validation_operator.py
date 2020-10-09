import json
import os

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_validation_operator_run_interactive_golden_path(
    caplog, data_context_simple_expectation_suite, filesystem_csv_2
):
    """
    Interactive mode golden path - pass an existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context_simple_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--name",
            "default",
            "--suite",
            "default",
        ],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Validation failed" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_run_interactive_pass_non_existing_expectation_suite(
    caplog, data_context_parameterized_expectation_suite, filesystem_csv_2
):
    """
    Interactive mode: pass an non-existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context_parameterized_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--name",
            "default",
            "--suite",
            "this.suite.does.not.exist",
        ],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Could not find a suite named" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_run_interactive_pass_non_existing_operator_name(
    caplog, data_context_parameterized_expectation_suite, filesystem_csv_2
):
    """
    Interactive mode: pass an non-existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context_parameterized_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--name",
            "this_val_op_does_not_exist",
            "--suite",
            "my_dag_node.default",
        ],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Could not find a validation operator" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_run_noninteractive_golden_path(
    caplog, data_context_simple_expectation_suite, filesystem_csv_2
):
    """
    Non-nteractive mode golden path - use the --validation_config_file argument to pass the path
    to a valid validation config file
    """
    not_so_empty_data_context = data_context_simple_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    csv_path = os.path.join(filesystem_csv_2, "f1.csv")

    validation_config = {
        "validation_operator_name": "default",
        "batches": [
            {
                "batch_kwargs": {
                    "path": csv_path,
                    "datasource": "mydatasource",
                    "reader_method": "read_csv",
                },
                "expectation_suite_names": ["default"],
            }
        ],
    }
    validation_config_file_path = os.path.join(
        root_dir, "uncommitted", "validation_config_1.json"
    )
    with open(validation_config_file_path, "w") as f:
        json.dump(validation_config, f)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--validation_config_file",
            validation_config_file_path,
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Validation failed" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_run_noninteractive_validation_config_file_does_not_exist(
    caplog, data_context_parameterized_expectation_suite, filesystem_csv_2
):
    """
    Non-nteractive mode. Use the --validation_config_file argument to pass the path
    to a validation config file that does not exist.
    """
    not_so_empty_data_context = data_context_parameterized_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    validation_config_file_path = os.path.join(
        root_dir, "uncommitted", "validation_config_1.json"
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--validation_config_file",
            validation_config_file_path,
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Failed to process the --validation_config_file argument" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_run_noninteractive_validation_config_file_does_is_misconfigured(
    caplog, data_context_parameterized_expectation_suite, filesystem_csv_2
):
    """
    Non-nteractive mode. Use the --validation_config_file argument to pass the path
    to a validation config file that is misconfigured - one of the batches does not
    have expectation_suite_names attribute
    """
    not_so_empty_data_context = data_context_parameterized_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    csv_path = os.path.join(filesystem_csv_2, "f1.csv")

    validation_config = {
        "validation_operator_name": "default",
        "batches": [
            {
                "batch_kwargs": {
                    "path": csv_path,
                    "datasource": "mydatasource",
                    "reader_method": "read_csv",
                },
                "wrong_attribute_expectation_suite_names": ["my_dag_node.default1"],
            }
        ],
    }
    validation_config_file_path = os.path.join(
        root_dir, "uncommitted", "validation_config_1.json"
    )
    with open(validation_config_file_path, "w") as f:
        json.dump(validation_config, f)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "validation-operator",
            "run",
            "-d",
            root_dir,
            "--validation_config_file",
            validation_config_file_path,
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert (
        "is misconfigured: Each batch must have a list of expectation suite names"
        in stdout
    )
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_one_operator(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")


def test_validation_operator_list_with_zero_validation_operators(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context._project_config.validation_operators = {}
    context._save_project_config()
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli,
        "validation-operator list -d {}".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Validation Operators found" in result.output

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_one_validation_operator(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    expected_result = """[33mHeads up! This feature is Experimental. It may change. Please give us your feedback![0m[0m
1 Validation Operator found:[0m
[0m
 - [36mname:[0m action_list_operator[0m
   [36mclass_name:[0m ActionListValidationOperator[0m
   [36maction_list:[0m store_validation_result (StoreValidationResultAction) => store_evaluation_params (StoreEvaluationParametersAction) => update_data_docs (UpdateDataDocsAction)[0m"""

    result = runner.invoke(
        cli,
        "validation-operator list -d {}".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # _capture_ansi_codes_to_file(result)
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_multiple_validation_operators(
    caplog, empty_data_context
):
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
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
            "base_expectation_suite_name": "new-years-expectations",
            "slack_webhook": "https://hooks.slack.com/services/dummy",
        },
    )
    context._save_project_config()
    expected_result = """[33mHeads up! This feature is Experimental. It may change. Please give us your feedback![0m[0m
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
        cli,
        "validation-operator list -d {}".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    # _capture_ansi_codes_to_file(result)
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)


def _capture_ansi_codes_to_file(result):
    """
    Use this to capture the ANSI color codes when updating snapshots.
    NOT DEAD CODE.
    """
    with open("ansi.txt", "w") as f:
        f.write(result.output.strip())
