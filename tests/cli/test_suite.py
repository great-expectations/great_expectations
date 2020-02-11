# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import os

import pytest
from click.testing import CliRunner
from six import PY2

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_suite_help_output(caplog,):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["suite"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        """\
Commands:
  edit  Generate a Jupyter notebook for editing an existing expectation suite.
  new   Create a new expectation suite."""
        in result.stdout
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_new_one_datasource_without_generator_without_suite_name_argument(
    caplog, empty_data_context, filesystem_csv_2
):
    """
    We call the "suite new" command without the suite name argument

    The data context has one datasource, so the command does not prompt us to choose.
    The datasource has no generator configured, so we are prompted only to enter the path
    (and not to choose from the generator's list of available data assets).

    We enter the path of the file we want the command to use as the batch to create the
    expectation suite.

    The command should prompt us to enter the name of the expectation suite that will be
    created.
    """
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    root_dir = project_root_dir
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--no-view"],
        input="{0:s}\nmy_new_suite\n\n".format(
            os.path.join(filesystem_csv_2, "f1.csv")
        ),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Enter the path" in stdout
    assert "Name the new expectation suite [warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Profiling" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_new_multiple_datasources_with_generator_without_suite_name_argument(
    caplog, site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite new" command without the suite name argument

    The data context has two datasources - we choose one of them. It has a generator
    configured. We choose to use the generator and select a generator asset from the list.

    The command should prompt us to enter the name of the expectation suite that will be
    created.
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--no-view"],
        input="2\n1\n1\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select data source" in stdout
    assert "Which data would you like to use" in stdout
    assert "Name the new expectation suite [warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Profiling" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_new_multiple_datasources_with_generator_with_suite_name_argument(
    caplog, site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite new" command with the suite name argument

    The data context has two datasources - we choose one of them. It has a generator
    configured. We choose to use the generator and select a generator asset from the list.
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite", "--no-view"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select data source" in stdout
    assert "Which data would you like to use" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Profiling" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'foo_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_without_suite_name_raises_error(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, "suite edit", catch_exceptions=False)
    assert result.exit_code == 2
    assert 'Error: Missing argument "SUITE".' in result.stderr


def test_suite_edit_with_invalid_json_batch_kwargs_raises_helpful_error(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo", "-d", project_dir, "--batch_kwargs", "'{foobar}'"],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "Please check that your batch_kwargs are valid JSON." in stdout
    # assert "Expecting value" in stdout # The exact message in the exception varies in Py 2, Py 3
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_with_batch_kwargs_unable_to_load_a_batch_raises_helpful_error(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory

    context = DataContext(project_dir)
    context.create_expectation_suite("foo")
    context.add_datasource("source", class_name="PandasDatasource")

    runner = CliRunner(mix_stderr=False)
    batch_kwargs = '{"table": "fake", "datasource": "source"}'
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo", "-d", project_dir, "--batch_kwargs", batch_kwargs],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "To continue editing this suite" not in stdout
    assert "Please check that your batch_kwargs are able to load a batch." in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_with_non_existent_suite_name_raises_error(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory
    assert not empty_data_context.list_expectation_suites()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        "suite edit not_a_real_suite -d {}".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Could not find a suite named `not_a_real_suite`. Please check the name and try again"
        in result.output
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_with_non_existent_datasource_shows_helpful_error_message(
    caplog, empty_data_context
):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")
    assert context.list_expectation_suites()[0].expectation_suite_name == "foo"

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        "suite edit foo -d {} --datasource not_real".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Unable to load datasource `not_real` -- no configuration found or invalid configuration."
        in result.output
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_multiple_datasources_with_generator_with_no_additional_args(
    caplog, site_builder_data_context_with_html_store_titanic_random,
):
    """
    Here we verify that the "suite edit" command helps the user to specify the batch
    kwargs when it is called without the optional arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our test
    will edit - this step is a just a setup.

    We call the "suite edit" command without any optional arguments. This means that
    the command will help us specify the batch kwargs interactively.

    The data context has two datasources - we choose one of them. It has a generator
    configured. We choose to use the generator and select a generator asset from the list.
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite", "--no-view"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "A new Expectation suite 'foo_suite' was added to your project" in stdout

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo_suite", "-d", root_dir, "--no-jupyter"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Select data source" in stdout
    assert "Which data would you like to use" in stdout
    assert "To continue editing this suite, run" in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "foo_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_multiple_datasources_with_generator_with_batch_kwargs_arg(
    caplog, site_builder_data_context_with_html_store_titanic_random,
):
    """
    Here we verify that when the "suite edit" command is called with batch_kwargs arg
    that specifies the batch that will be used as a sample for editing the suite,
    the command processes the batch_kwargs correctly and skips all the prompts
    that help users to specify the batch (when called without batch_kwargs).

    First, we call the "suite new" command to create the expectation suite our test
    will edit - this step is a just a setup.

    We call the "suite edit" command without any optional arguments. This means that
    the command will help us specify the batch kwargs interactively.

    The data context has two datasources - we choose one of them. It has a generator
    configured. We choose to use the generator and select a generator asset from the list.
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite", "--no-view"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "A new Expectation suite 'foo_suite' was added to your project" in stdout

    batch_kwargs = {
        "datasource": "random",
        "path": str(
            os.path.join(
                os.path.abspath(os.path.join(root_dir, os.pardir)),
                "data",
                "random",
                "f1.csv",
            )
        ),
    }
    batch_kwargs_arg_str = json.dumps(batch_kwargs)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "suite",
            "edit",
            "foo_suite",
            "-d",
            root_dir,
            "--no-jupyter",
            "--batch_kwargs",
            batch_kwargs_arg_str,
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select data source" not in stdout
    assert "Which data would you like to use" not in stdout
    assert "To continue editing this suite, run" in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "foo_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(condition=PY2, reason="different error messages in py2")
def test_suite_edit_on_exsiting_suite_one_datasources_with_batch_kwargs_without_datasource_raises_helpful_error(
    caplog, titanic_data_context,
):
    """
    Given:
    - the suite foo exists
    - the a datasource exists
    - and the users runs this
    great_expectations suite edit foo --batch_kwargs '{"path": "data/10k.csv"}'

    Then:
    - The user should see a nice error and the program halts before notebook compilation.
    '"""
    project_dir = titanic_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")

    runner = CliRunner(mix_stderr=False)
    batch_kwargs = {"path": "../data/Titanic.csv"}
    result = runner.invoke(
        cli,
        [
            "suite",
            "edit",
            "foo",
            "-d",
            project_dir,
            "--batch_kwargs",
            json.dumps(batch_kwargs),
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "Please check that your batch_kwargs are able to load a batch." in stdout
    assert "Unable to load datasource `None`" in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_on_exsiting_suite_one_datasources_with_datasource_arg_and_batch_kwargs(
    caplog, titanic_data_context,
):
    """
    Given:
    - the suite foo exists
    - the a datasource bar exists
    - and the users runs this
    great_expectations suite edit foo --datasource bar --batch_kwargs '{"path": "data/10k.csv"}'

    Then:
    - The user gets a working notebook
    """
    project_dir = titanic_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")

    runner = CliRunner(mix_stderr=False)
    batch_kwargs = {"path": os.path.join(project_dir, "../", "data", "Titanic.csv")}
    result = runner.invoke(
        cli,
        [
            "suite",
            "edit",
            "foo",
            "-d",
            project_dir,
            "--batch_kwargs",
            json.dumps(batch_kwargs),
            "--datasource",
            "mydatasource",
            "--no-jupyter",
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0
    assert "To continue editing this suite, run" in stdout

    expected_notebook_path = os.path.join(project_dir, "uncommitted", "foo.ipynb")
    assert os.path.isfile(expected_notebook_path)
    expected_suite_path = os.path.join(project_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_one_datasources_no_generator_with_no_additional_args(
    caplog, empty_data_context, filesystem_csv_2
):
    """
    Here we verify that the "suite edit" command helps the user to specify the batch
    kwargs when it is called without the optional arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our test
    will edit - this step is a just a setup.

    We call the "suite edit" command without any optional arguments. This means that
    the command will help us specify the batch kwargs interactively.

    The data context has one datasource. The datasource has no generators
    configured. The command prompts us to enter the file path.
    """
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    root_dir = project_root_dir
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--no-view"],
        input="{0:s}\nmy_new_suite\n\n".format(
            os.path.join(filesystem_csv_2, "f1.csv")
        ),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "my_new_suite", "-d", root_dir, "--no-jupyter"],
        input="{0:s}\n\n".format(os.path.join(filesystem_csv_2, "f1.csv")),
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Select data source" not in stdout
    assert "Which data would you like to use" not in stdout
    assert "Enter the path" in stdout
    assert "To continue editing this suite, run" in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "my_new_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)
    assert_no_logging_messages_or_tracebacks(caplog, result)
