# -*- coding: utf-8 -*-
import os
import subprocess

from click.testing import CliRunner
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

# TODO When deprecating tap, many of these tests can be simplified to test the toolkit functions


def test_tap_help_output(caplog,):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["tap"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        """Commands:
  new  Create a new tap file for easy deployments."""
        in result.stdout
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_tap_new_with_filename_not_ending_in_py_raises_helpful_error(
    caplog, empty_data_context
):
    """
    We call the "tap new" command with a bogus filename

    The command should:
    - exit with a clear error message
    """
    context = empty_data_context
    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"tap new sweet_suite tap -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "Tap filename must end in .py. Please correct and re-run" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_tap_new_on_context_with_no_datasources(caplog, empty_data_context):
    """
    We call the "tap new" command on a data context that has no datasources
    configured.

    The command should:
    - exit with a clear error message
    """
    root_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"tap new not_a_suite tap.py -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "No datasources found in the context" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_tap_new_with_non_existant_suite(caplog, empty_data_context):
    """
    We call the "tap new" command on a data context that has a datasource
    configured and no suites.

    The command should:
    - exit with a clear error message
    """
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    root_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"tap new not_a_suite tap.py -d {root_dir}", catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "Could not find a suite named `not_a_suite`" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_tap_new_on_context_with_2_datasources_with_no_datasource_option_prompts_user(
    caplog, empty_data_context
):
    """
    We call the "tap new" command on a data context that has 2 datasources
    configured.

    The command should:
    - prompt the user to choose a datasource
    """
    empty_data_context.add_datasource(
        "1_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    empty_data_context.add_datasource(
        "2_my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    root_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"tap new not_a_suite tap.py -d {root_dir}",
        input="1\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "Select a datasource" in stdout
    assert result.exit_code == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_tap_new_on_context_builds_runnable_tap_file(
    caplog, empty_data_context, filesystem_csv
):
    """
    We call the "tap new" command on a data context that has 2 datasources
    configured.

    The command should:
    - prompt the user to choose a datasource
    - create the tap file

    This test then runs the tap file to verify it is runnable.
    """
    context = empty_data_context
    root_dir = context.root_directory
    csv = os.path.join(filesystem_csv, "f1.csv")

    context.add_datasource(
        "1_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    context.create_expectation_suite("sweet_suite")
    suite = context.get_expectation_suite("sweet_suite")
    context.save_expectation_suite(suite)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"tap new sweet_suite tap.py -d {root_dir}",
        input=f"{csv}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    print(stdout)

    assert "Enter the path (relative or absolute) of a data file" in stdout
    assert "A new tap has been generated" in stdout
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)

    tap_file = os.path.abspath(os.path.join(root_dir, "..", "tap.py"))
    # In travis on osx, python may not execute from the build dir
    cmdstring = f"python {tap_file}"
    if os.environ.get("TRAVIS_OS_NAME") == "osx":
        build_dir = os.environ.get("TRAVIS_BUILD_DIR")
        print(os.listdir(build_dir))
        cmdstring = f"python3 {tap_file}"
    print("about to run: " + cmdstring)
    print(os.curdir)
    print(os.listdir(os.curdir))
    print(os.listdir(os.path.abspath(os.path.join(root_dir, ".."))))
    status, output = subprocess.getstatusoutput(cmdstring)
    assert status == 0
    assert output == "Validation Succeeded!"


def test_tap_new_on_context_builds_runnable_tap_file_that_fails_validation(
    caplog, empty_data_context, filesystem_csv
):
    """
    We call the "tap new" command on a data context that has 1 datasource
    configured with a suite that will fail.

    The command should:
    - prompt the user to choose a datasource
    - create the tap file

    This test then runs the tap file to verify it is runnable and fails
    correctly.
    """
    context = empty_data_context
    root_dir = context.root_directory
    csv = os.path.join(filesystem_csv, "f1.csv")

    context.add_datasource(
        "1_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    context.create_expectation_suite("sweet_suite")
    suite = context.get_expectation_suite("sweet_suite")
    context.save_expectation_suite(suite)
    batch_kwargs = {"datasource": "1_datasource", "path": csv}
    batch = context.get_batch(batch_kwargs, suite)
    # Make an expectation that will fail and save it.
    batch.expect_table_column_count_to_equal(9999)
    batch.save_expectation_suite(discard_failed_expectations=False)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"tap new sweet_suite tap.py -d {root_dir}",
        input=f"{csv}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "Enter the path (relative or absolute) of a data file" in stdout
    assert "A new tap has been generated" in stdout
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)
    # In travis on osx, python may not execute from the build dir
    tap_file = os.path.abspath(os.path.join(root_dir, "..", "tap.py"))
    cmdstring = f"python {tap_file}"
    if os.environ.get("TRAVIS_OS_NAME") == "osx":
        build_dir = os.environ.get("TRAVIS_BUILD_DIR")
        cmdstring = f"PYTHONPATH={build_dir} " + cmdstring

    status, output = subprocess.getstatusoutput(f"python {tap_file}")
    assert status == 1
    assert output == "Validation Failed!"


def test_tap_new_on_context_with_1_datasources_with_no_datasource_option_prompts_user_and_generates_runnable_tap_file(
    caplog, empty_data_context, filesystem_csv
):
    """
    We call the "tap new" command on a data context that has 1 datasources
    configured.

    The command should:
    - NOT prompt the user to choose a datasource
    - create the tap file

    This test then runs the tap file to verify it is runnable.
    """
    context = empty_data_context
    root_dir = context.root_directory
    csv = os.path.join(filesystem_csv, "f1.csv")

    context.add_datasource(
        "1_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )
    context.create_expectation_suite("sweet_suite")
    suite = context.get_expectation_suite("sweet_suite")
    context.save_expectation_suite(suite)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"tap new sweet_suite tap.py -d {root_dir}",
        input=f"{csv}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "Select a datasource" not in stdout
    assert "A new tap has been generated" in stdout
    assert result.exit_code == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)

    tap_file = os.path.abspath(os.path.join(root_dir, "..", "tap.py"))
    status, output = subprocess.getstatusoutput(f"python {tap_file}")
    assert status == 0
    assert output == "Validation Succeeded!"
