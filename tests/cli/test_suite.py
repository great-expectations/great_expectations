# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import os

import mock
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.core import ExpectationSuite
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_suite_help_output(caplog,):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["suite"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        """\
Commands:
  edit  Generate a Jupyter notebook for editing an existing Expectation Suite.
  list  Lists available Expectation Suites.
  new   Create a new Expectation Suite."""
        in result.stdout
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_on_context_with_no_datasources(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    We call the "suite new" command on a data context that has no datasources
    configured.

    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    project_root_dir = empty_data_context.root_directory

    root_dir = project_root_dir
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["suite", "new", "-d", root_dir], catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "No datasources found in the context" in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_enter_existing_suite_name_as_arg(
    mock_webbrowser, mock_subprocess, caplog, data_context
):
    """
    We call the "suite new" command with the name of an existing expectation
    suite in the --suite argument

    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """

    not_so_empty_data_context = data_context
    project_root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(project_root_dir, "uncommitted"))

    root_dir = project_root_dir
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "my_dag_node.default", "--no-view"],
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "already exists. If you intend to edit the suite" in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_answer_suite_name_prompts_with_name_of_existing_suite(
    mock_webbrowser, mock_subprocess, caplog, data_context, filesystem_csv_2
):
    """
    We call the "suite new" command without the suite name argument

    The command should:

    - prompt us to enter the name of the expectation suite that will be
    created. We answer the prompt with the name of an existing expectation suite.
    - display an error message and let us retry until we answer
    with a name that is not "taken".
    - create an example suite
    - NOT open jupyter
    - open DataDocs to the new example suite page
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir],
        input=f"{csv_path}\nmy_dag_node.default\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "already exists. If you intend to edit the suite" in stdout
    assert "Enter the path" in stdout
    assert "Name the new expectation suite [f1.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout
    assert "open a notebook for you now" not in stdout

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_subprocess.call_count == 0
    assert mock_webbrowser.call_count == 1

    foo = os.path.join(
        root_dir, "uncommitted/data_docs/local_site/validations/my_new_suite/"
    )
    assert f"file://{foo}" in mock_webbrowser.call_args[0][0]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_empty_suite_creates_empty_suite(
    mock_webbroser, mock_subprocess, caplog, data_context, filesystem_csv_2
):
    """
    Running "suite new --empty" should:
    - make an empty suite
    - open jupyter
    - NOT open data docs
    """
    project_root_dir = data_context.root_directory
    os.mkdir(os.path.join(project_root_dir, "uncommitted"))
    root_dir = project_root_dir
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    csv = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--empty", "--suite", "foo"],
        input=f"{csv}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Enter the path" in stdout
    assert "Name the new expectation suite" not in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        not in stdout
    )
    assert "Generating example Expectation Suite..." not in stdout
    assert "The following Data Docs sites were built" not in stdout
    assert "A new Expectation suite 'foo' was added to your project" in stdout
    assert (
        "Because you requested an empty suite, we'll open a notebook for you now to edit it!"
        in stdout
    )

    expected_suite_path = os.path.join(root_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)

    expected_notebook = os.path.join(root_dir, "uncommitted", "foo.ipynb")
    assert os.path.isfile(expected_notebook)

    context = DataContext(root_dir)
    assert "foo" in context.list_expectation_suite_names()
    suite = context.get_expectation_suite("foo")
    assert suite.expectations == []
    citations = suite.get_citations()
    citations[0].pop("citation_date")
    assert citations[0] == {
        "batch_kwargs": {
            "datasource": "mydatasource",
            "path": csv,
            "reader_method": "read_csv",
        },
        "batch_markers": None,
        "batch_parameters": None,
        "comment": "New suite added via CLI",
    }

    assert mock_subprocess.call_count == 1
    call_args = mock_subprocess.call_args[0][0]
    assert call_args[0] == "jupyter"
    assert call_args[1] == "notebook"
    assert expected_notebook in call_args[2]

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_empty_suite_creates_empty_suite_with_no_jupyter(
    mock_webbroser, mock_subprocess, caplog, data_context, filesystem_csv_2
):
    """
    Running "suite new --empty --no-jupyter" should:
    - make an empty suite
    - NOT open jupyter
    - NOT open data docs
    """
    project_root_dir = data_context.root_directory
    os.mkdir(os.path.join(project_root_dir, "uncommitted"))
    root_dir = project_root_dir
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    csv = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--empty", "--suite", "foo", "--no-jupyter"],
        input=f"{csv}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Enter the path" in stdout
    assert "Name the new expectation suite" not in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        not in stdout
    )
    assert "Generating example Expectation Suite..." not in stdout
    assert "The following Data Docs sites were built" not in stdout
    assert "A new Expectation suite 'foo' was added to your project" in stdout
    assert "open a notebook for you now" not in stdout

    expected_suite_path = os.path.join(root_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)

    expected_notebook = os.path.join(root_dir, "uncommitted", "foo.ipynb")
    assert os.path.isfile(expected_notebook)

    context = DataContext(root_dir)
    assert "foo" in context.list_expectation_suite_names()
    suite = context.get_expectation_suite("foo")
    assert suite.expectations == []
    citations = suite.get_citations()
    citations[0].pop("citation_date")
    assert citations[0] == {
        "batch_kwargs": {
            "datasource": "mydatasource",
            "path": csv,
            "reader_method": "read_csv",
        },
        "batch_markers": None,
        "batch_parameters": None,
        "comment": "New suite added via CLI",
    }

    assert mock_subprocess.call_count == 0
    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_one_datasource_without_generator_without_suite_name_argument(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context, filesystem_csv_2
):
    """
    We call the "suite new" command without the suite name argument

    The command should:

    - NOT prompt us to choose a datasource (because there is only one)
    - prompt us only to enter the path (The datasource has no generator
     configured and not to choose from the generator's list of available data
     assets).
    - We enter the path of the file we want the command to use as the batch to
    create the expectation suite.
    - prompt us to enter the name of the expectation suite that will be
    created
    - open Data Docs
    - NOT open jupyter
    """
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
    )

    context = empty_data_context
    project_root_dir = context.root_directory

    root_dir = project_root_dir
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir],
        input="{0:s}\nmy_new_suite\n\n".format(
            os.path.join(filesystem_csv_2, "f1.csv")
        ),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Enter the path" in stdout
    assert "Name the new expectation suite [f1.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]["site_url"]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_multiple_datasources_with_generator_without_suite_name_argument(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite new" command without the suite name argument

    - The data context has two datasources - we choose one of them.
    - It has a generator configured. We choose to use the generator and select a
    generator asset from the list.
    - The command should prompt us to enter the name of the expectation suite
    that will be created.
    - open Data Docs
    - NOT open jupyter
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir],
        input="1\n1\n1\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert (
        """Select a datasource
    1. random
    2. titanic"""
        in stdout
    )
    assert (
        """Which data would you like to use?
    1. f1 (file)
    2. f2 (file)"""
        in stdout
    )
    assert "Name the new expectation suite [f1.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]["site_url"]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_multiple_datasources_with_generator_with_suite_name_argument(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite new" command with the suite name argument

    - The data context has two datasources - we choose one of them.
    - It has a generator configured. We choose to use the generator and select
    a generator asset from the list.
    - open Data Docs
    - NOT open jupyter
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select a datasource" in stdout
    assert "Which data would you like to use" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites were built" in stdout
    assert "A new Expectation suite 'foo_suite' was added to your project" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html" in obs_urls[0]["site_url"]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_edit_without_suite_name_raises_error():
    """This is really only testing click missing arguments"""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, "suite edit", catch_exceptions=False)
    assert result.exit_code == 2
    assert (
        'Error: Missing argument "SUITE".' in result.stderr
        or "Error: Missing argument 'SUITE'." in result.stderr
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_invalid_json_batch_kwargs_raises_helpful_error(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo", "-d", project_dir, "--batch-kwargs", "'{foobar}'"],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "Please check that your batch_kwargs are valid JSON." in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_batch_kwargs_unable_to_load_a_batch_raises_helpful_error(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    project_dir = empty_data_context.root_directory

    context = DataContext(project_dir)
    context.create_expectation_suite("foo")
    context.add_datasource("source", class_name="PandasDatasource")

    runner = CliRunner(mix_stderr=False)
    batch_kwargs = '{"table": "fake", "datasource": "source"}'
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo", "-d", project_dir, "--batch-kwargs", batch_kwargs],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "To continue editing this suite" not in stdout
    assert "Please check that your batch_kwargs are able to load a batch." in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_suite_name_raises_error(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    project_dir = empty_data_context.root_directory
    assert not empty_data_context.list_expectation_suites()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        "suite edit not_a_real_suite -d {}".format(project_dir),
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Could not find a suite named `not_a_real_suite`." in result.output
    assert "by running `great_expectations suite list`" in result.output

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_datasource_shows_helpful_error_message(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("foo")
    assert context.list_expectation_suites()[0].expectation_suite_name == "foo"

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"suite edit foo -d {project_dir} --datasource not_real",
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert (
        "Unable to load datasource `not_real` -- no configuration found or invalid configuration."
        in result.output
    )

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_generator_with_no_additional_args_with_suite_without_citations(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
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

    The command should:
    - NOT open Data Docs
    - open jupyter
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0
    mock_webbrowser.reset_mock()
    mock_subprocess.reset_mock()

    # remove the citations from the suite
    context = DataContext(root_dir)
    suite = context.get_expectation_suite("foo_suite")
    assert isinstance(suite, ExpectationSuite)
    suite.meta.pop("citations")
    context.save_expectation_suite(suite)

    # Actual testing really starts here
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo_suite", "-d", root_dir,],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "A batch of data is required to edit the suite" in stdout
    assert "Select a datasource" in stdout
    assert "Which data would you like to use" in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "foo_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_generator_with_no_additional_args_with_suite_containing_citations(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
):
    """
    Here we verify that the "suite edit" command uses the batch kwargs found in
    citations in the existing suite when it is called without the optional
    arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our
    test will edit - this step is a just a setup.

    We call the "suite edit" command without any optional arguments.

    The command should:
    - NOT open Data Docs
    - NOT open jupyter
    """
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo_suite"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()
    mock_webbrowser.reset_mock()
    assert result.exit_code == 0
    context = DataContext(root_dir)
    suite = context.get_expectation_suite("foo_suite")
    assert isinstance(suite, ExpectationSuite)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "foo_suite", "-d", root_dir],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Select a datasource" not in stdout
    assert "Which data would you like to use" not in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "foo_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_generator_with_batch_kwargs_arg(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
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

    The command should:
    - NOT open Data Docs
    - open jupyter
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
    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()
    mock_webbrowser.reset_mock()
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
            "--batch-kwargs",
            batch_kwargs_arg_str,
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Select a datasource" not in stdout
    assert "Which data would you like to use" not in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "foo_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_on_exsiting_suite_one_datasources_with_batch_kwargs_without_datasource_raises_helpful_error(
    mock_webbrowser, mock_subprocess, caplog, titanic_data_context,
):
    """
    Given:
    - the suite foo exists
    - the a datasource exists
    - and the users runs this
    great_expectations suite edit foo --batch-kwargs '{"path": "data/10k.csv"}'

    Then:
    - The user should see a nice error and the program halts before notebook
    compilation.
    - NOT open Data Docs
    - NOT open jupyter
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
            "--batch-kwargs",
            json.dumps(batch_kwargs),
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "Please check that your batch_kwargs are able to load a batch." in stdout
    assert "Unable to load datasource `None`" in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_on_exsiting_suite_one_datasources_with_datasource_arg_and_batch_kwargs(
    mock_webbrowser, mock_subprocess, caplog, titanic_data_context,
):
    """
    Given:
    - the suite foo exists
    - the a datasource bar exists
    - and the users runs this
    great_expectations suite edit foo --datasource bar --batch-kwargs '{"path": "data/10k.csv"}'

    Then:
    - The user gets a working notebook
    - NOT open Data Docs
    - open jupyter
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
            "--batch-kwargs",
            json.dumps(batch_kwargs),
            "--datasource",
            "mydatasource",
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert stdout == ""
    assert result.exit_code == 0

    expected_notebook_path = os.path.join(project_dir, "uncommitted", "foo.ipynb")
    assert os.path.isfile(expected_notebook_path)
    expected_suite_path = os.path.join(project_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_one_datasources_no_generator_with_no_additional_args_and_no_citations(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context, filesystem_csv_2
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
        ["suite", "new", "-d", root_dir],
        input="{0:s}\nmy_new_suite\n\n".format(
            os.path.join(filesystem_csv_2, "f1.csv")
        ),
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()
    mock_webbrowser.reset_mock()
    assert result.exit_code == 0
    assert "A new Expectation suite 'my_new_suite' was added to your project" in stdout

    # remove the citations from the suite
    context = DataContext(project_root_dir)
    suite = context.get_expectation_suite("my_new_suite")
    suite.meta.pop("citations")
    context.save_expectation_suite(suite)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "my_new_suite", "-d", root_dir],
        input="{0:s}\n\n".format(os.path.join(filesystem_csv_2, "f1.csv")),
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Select a datasource" not in stdout
    assert "Which data would you like to use" not in stdout
    assert "Enter the path" in stdout

    expected_notebook_path = os.path.join(root_dir, "uncommitted", "my_new_suite.ipynb")
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_list_with_zero_suites(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "suite list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Expectation Suites found" in result.output

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_list_with_one_suite(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "suite list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "1 Expectation Suite found" in result.output
    assert "a.warning" in result.output
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_suite_list_with_multiple_suites(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")
    context.create_expectation_suite("b.warning")
    context.create_expectation_suite("c.warning")

    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "suite list -d {}".format(project_dir), catch_exceptions=False,
    )
    output = result.output
    assert result.exit_code == 0
    assert "3 Expectation Suites found:" in output
    assert "a.warning" in output
    assert "b.warning" in output
    assert "c.warning" in output

    assert_no_logging_messages_or_tracebacks(caplog, result)
