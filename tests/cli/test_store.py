import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.exceptions import InvalidConfigurationYamlError
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_store_list_with_zero_stores(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context._project_config.stores = {}
    context._save_project_config()
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "store list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Your configuration file is not a valid yml file likely due to a yml syntax error"
        in result.output.strip()
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_store_list_with_one_store(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    del (context._project_config.stores)["validations_store"]
    del (context._project_config.stores)["evaluation_parameter_store"]
    context._project_config.validations_store_name = "expectations_store"
    context._project_config.evaluation_parameter_store_name = "expectations_store"
    context._save_project_config()

    runner = CliRunner(mix_stderr=False)

    expected_result = """\
1 Store found:[0m
[0m
 - [36mname:[0m expectations_store[0m
   [36mclass_name:[0m ExpectationsStore[0m
   [36mstore_backend:[0m[0m
     [36mclass_name:[0m TupleFilesystemStoreBackend[0m
     [36mbase_directory:[0m expectations/[0m"""

    result = runner.invoke(
        cli, "store list -d {}".format(project_dir), catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_store_list_with_multiple_stores(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    expected_result = """\
3 Stores found:[0m
[0m
 - [36mname:[0m expectations_store[0m
   [36mclass_name:[0m ExpectationsStore[0m
   [36mstore_backend:[0m[0m
     [36mclass_name:[0m TupleFilesystemStoreBackend[0m
     [36mbase_directory:[0m expectations/[0m
[0m
 - [36mname:[0m validations_store[0m
   [36mclass_name:[0m ValidationsStore[0m
   [36mstore_backend:[0m[0m
     [36mclass_name:[0m TupleFilesystemStoreBackend[0m
     [36mbase_directory:[0m uncommitted/validations/[0m
[0m
 - [36mname:[0m evaluation_parameter_store[0m
   [36mclass_name:[0m EvaluationParameterStore[0m"""

    result = runner.invoke(
        cli, "store list -d {}".format(project_dir), catch_exceptions=False,
    )
    print(result.output)
    assert result.exit_code == 0
    assert result.output.strip() == expected_result

    assert_no_logging_messages_or_tracebacks(caplog, result)
