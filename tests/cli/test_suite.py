import json
import os
from unittest import mock

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.core import ExpectationSuite
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_suite_help_output(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["suite"], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        """Commands:
  delete    Delete an expectation suite from the expectation store.
  demo      Create a new demo Expectation Suite.
  edit      Generate a Jupyter notebook for editing an existing Expectation...
  list      Lists available Expectation Suites.
  new       Create a new empty Expectation Suite.
  scaffold  Scaffold a new Expectation Suite."""
        in result.stdout
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_demo_on_context_with_no_datasources(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context
):
    """
    We call the "suite demo" command on a data context that has no datasources
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
        cli, ["suite", "demo", "-d", root_dir], catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert "No datasources found in the context" in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_demo_enter_existing_suite_name_as_arg(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    data_context_parameterized_expectation_suite,
):
    """
    We call the "suite demo" command with the name of an existing expectation
    suite in the --suite argument

    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """

    not_so_empty_data_context = data_context_parameterized_expectation_suite
    project_root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(project_root_dir, "uncommitted"))

    context = DataContext(project_root_dir)
    existing_suite_name = "my_dag_node.default"
    assert context.list_expectation_suite_names() == [existing_suite_name]

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "suite",
            "demo",
            "-d",
            project_root_dir,
            "--suite",
            existing_suite_name,
            "--no-view",
        ],
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 1
    assert (
        f"An expectation suite named `{existing_suite_name}` already exists." in stdout
    )
    assert (
        f"If you intend to edit the suite please use `great_expectations suite edit {existing_suite_name}`"
        in stdout
    )

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_demo_answer_suite_name_prompts_with_name_of_existing_suite(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    data_context_parameterized_expectation_suite,
    filesystem_csv_2,
):
    """
    We call the "suite demo" command without the suite name argument

    The command should:

    - prompt us to enter the name of the expectation suite that will be
    created. We answer the prompt with the name of an existing expectation suite.
    - display an error message and let us retry until we answer
    with a name that is not "taken".
    - create an example suite
    - NOT open jupyter
    - open DataDocs to the new example suite page
    """
    not_so_empty_data_context = data_context_parameterized_expectation_suite
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")

    existing_suite_name = "my_dag_node.default"
    context = DataContext(root_dir)
    assert context.list_expectation_suite_names() == [existing_suite_name]

    result = runner.invoke(
        cli,
        ["suite", "demo", "-d", root_dir],
        input=f"{csv_path}\n{existing_suite_name}\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert (
        f"An expectation suite named `{existing_suite_name}` already exists." in stdout
    )
    assert (
        f"If you intend to edit the suite please use `great_expectations suite edit {existing_suite_name}`"
        in stdout
    )
    assert "Enter the path" in stdout
    assert "Name the new Expectation Suite [f1.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites will be built" in stdout
    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'my_new_suite' here"
        in stdout
    )
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
def test_suite_new_creates_empty_suite(
    mock_webbroser,
    mock_subprocess,
    caplog,
    data_context_parameterized_expectation_suite,
    filesystem_csv_2,
):
    """
    Running "suite new" should:
    - make an empty suite
    - open jupyter
    - NOT open data docs
    """
    project_root_dir = data_context_parameterized_expectation_suite.root_directory
    os.mkdir(os.path.join(project_root_dir, "uncommitted"))
    root_dir = project_root_dir
    os.chdir(root_dir)
    runner = CliRunner(mix_stderr=False)
    csv = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo"],
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
    assert (
        "Great Expectations will create a new Expectation Suite 'foo' and store it here"
        in stdout
    )
    assert (
        "Because you requested an empty suite, we'll open a notebook for you now to edit it!"
        in stdout
    )

    expected_suite_path = os.path.join(root_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)

    expected_notebook = os.path.join(root_dir, "uncommitted", "edit_foo.ipynb")
    assert os.path.isfile(expected_notebook)

    context = DataContext(root_dir)
    assert "foo" in context.list_expectation_suite_names()
    suite = context.get_expectation_suite("foo")
    assert suite.expectations == []
    citations = suite.get_citations()
    citations[0].pop("citation_date")
    assert citations[0] == {
        "batch_kwargs": {
            "data_asset_name": "f1",
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
def test_suite_new_empty_with_no_jupyter(
    mock_webbroser,
    mock_subprocess,
    caplog,
    data_context_parameterized_expectation_suite,
    filesystem_csv_2,
):
    """
    Running "suite new --no-jupyter" should:
    - make an empty suite
    - NOT open jupyter
    - NOT open data docs
    """
    os.mkdir(
        os.path.join(
            data_context_parameterized_expectation_suite.root_directory, "uncommitted"
        )
    )
    root_dir = data_context_parameterized_expectation_suite.root_directory
    runner = CliRunner(mix_stderr=False)
    csv = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "new", "-d", root_dir, "--suite", "foo", "--no-jupyter"],
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
    assert (
        "Great Expectations will create a new Expectation Suite 'foo' and store it here"
        in stdout
    )
    assert "open a notebook for you now" not in stdout

    expected_suite_path = os.path.join(root_dir, "expectations", "foo.json")
    assert os.path.isfile(expected_suite_path)

    expected_notebook = os.path.join(root_dir, "uncommitted", "edit_foo.ipynb")
    assert os.path.isfile(expected_notebook)

    context = DataContext(root_dir)
    assert "foo" in context.list_expectation_suite_names()
    suite = context.get_expectation_suite("foo")
    assert suite.expectations == []
    citations = suite.get_citations()
    citations[0].pop("citation_date")
    assert citations[0] == {
        "batch_kwargs": {
            "data_asset_name": "f1",
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
def test_suite_demo_one_datasource_without_generator_without_suite_name_argument(
    mock_webbrowser, mock_subprocess, caplog, empty_data_context, filesystem_csv_2
):
    """
    We call the "suite demo" command without the suite name argument

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
    root_dir = context.root_directory
    context = DataContext(root_dir)
    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["suite", "demo", "-d", root_dir],
        input=f"{csv_path}\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Enter the path" in stdout
    assert "Name the new Expectation Suite [f1.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )
    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'my_new_suite' here:"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites will be built" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
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
def test_suite_demo_multiple_datasources_with_generator_without_suite_name_argument(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite demo" command without the suite name argument

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
        ["suite", "demo", "-d", root_dir],
        input="1\n1\n1\nmy_new_suite\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert (
        """Select a datasource
    1. mydatasource
    2. random
    3. titanic"""
        in stdout
    )
    assert (
        """Which data would you like to use?
    1. random (directory)
    2. titanic (directory)"""
        in stdout
    )

    assert "Name the new Expectation Suite [random.warning]" in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations"
        in stdout
    )

    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'my_new_suite' here:"
        in stdout
    )
    assert "Generating example Expectation Suite..." in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites will be built" in stdout

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 2
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "my_new_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 2
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_demo_multiple_datasources_with_generator_with_suite_name_argument(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    site_builder_data_context_with_html_store_titanic_random,
):
    """
    We call the "suite demo" command with the suite name argument

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
        ["suite", "demo", "-d", root_dir, "--suite", "foo_suite"],
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
    assert "The following Data Docs sites will be built" in stdout
    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'foo_suite' here:"
        in stdout
    )

    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 2
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )

    expected_index_path = os.path.join(
        root_dir, "uncommitted", "data_docs", "local_site", "index.html"
    )
    assert os.path.isfile(expected_index_path)

    expected_suite_path = os.path.join(root_dir, "expectations", "foo_suite.json")
    assert os.path.isfile(expected_suite_path)

    assert mock_webbrowser.call_count == 2
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
        ["suite", "demo", "-d", root_dir, "--suite", "foo_suite"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 2
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

    expected_notebook_path = os.path.join(
        root_dir, "uncommitted", "edit_foo_suite.ipynb"
    )
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
        ["suite", "demo", "-d", root_dir, "--suite", "foo_suite"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert mock_webbrowser.call_count == 2
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

    expected_notebook_path = os.path.join(
        root_dir, "uncommitted", "edit_foo_suite.ipynb"
    )
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
        ["suite", "demo", "-d", root_dir, "--suite", "foo_suite", "--no-view"],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()
    mock_webbrowser.reset_mock()
    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'foo_suite' here:"
        in stdout
    )

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

    expected_notebook_path = os.path.join(
        root_dir, "uncommitted", "edit_foo_suite.ipynb"
    )
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

    expected_notebook_path = os.path.join(project_dir, "uncommitted", "edit_foo.ipynb")
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
        ["suite", "demo", "-d", root_dir],
        input="{:s}\nmy_new_suite\n\n".format(os.path.join(filesystem_csv_2, "f1.csv")),
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert mock_webbrowser.call_count == 1
    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()
    mock_webbrowser.reset_mock()
    assert result.exit_code == 0
    assert (
        "Great Expectations will store these expectations in a new Expectation Suite 'my_new_suite' here:"
        in stdout
    )

    # remove the citations from the suite
    context = DataContext(project_root_dir)
    suite = context.get_expectation_suite("my_new_suite")
    suite.meta.pop("citations")
    context.save_expectation_suite(suite)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "edit", "my_new_suite", "-d", root_dir],
        input="{:s}\n\n".format(os.path.join(filesystem_csv_2, "f1.csv")),
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    stdout = result.stdout
    assert "Select a datasource" not in stdout
    assert "Which data would you like to use" not in stdout
    assert "Enter the path" in stdout

    expected_notebook_path = os.path.join(
        root_dir, "uncommitted", "edit_my_new_suite.ipynb"
    )
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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_zero_suites(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    project_dir = empty_data_context_stats_enabled.root_directory
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, f"suite delete not_a_suite -d {project_dir}", catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "No expectation suites found in the project" in result.output

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call({"event": "cli.suite.delete", "event_payload": {}, "success": False}),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_non_existent_suite(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    context = empty_data_context_stats_enabled
    project_dir = context.root_directory
    suite = context.create_expectation_suite("foo")
    context.save_expectation_suite(suite)
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, f"suite delete not_a_suite -d {project_dir}", catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "No expectation suite named not_a_suite found" in result.output

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call({"event": "cli.suite.delete", "event_payload": {}, "success": False}),
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_one_suite(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    project_dir = empty_data_context_stats_enabled.root_directory
    context = DataContext(project_dir)
    suite = context.create_expectation_suite("a.warning")
    context.save_expectation_suite(suite)
    mock_emit.reset_mock()

    suite_dir = os.path.join(project_dir, "expectations", "a")
    suite_path = os.path.join(suite_dir, "warning.json")
    assert os.path.isfile(suite_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, "suite delete a.warning -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "Deleted the expectation suite named: a.warning" in result.output

    # assert not os.path.isdir(suite_dir)
    assert not os.path.isfile(suite_path)

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call({"event": "cli.suite.delete", "event_payload": {}, "success": True}),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_on_context_with_no_datasource_raises_error(
    mock_subprocess, mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command on a context with no datasource

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a scaffold fail message
    """
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["suite", "scaffold", "foop", "-d", root_dir], catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert (
        "No datasources found in the context. To add a datasource, run `great_expectations datasource new`"
        in stdout
    )

    assert mock_subprocess.call_count == 0
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_scaffold_on_existing_suite_raises_error(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command with an existing suite

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a scaffold fail message
    """
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    suite = context.create_expectation_suite("foop")
    context.save_expectation_suite(suite)
    assert context.list_expectation_suite_names() == ["foop"]
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["suite", "scaffold", "foop", "-d", root_dir], catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "An expectation suite named `foop` already exists." in stdout
    assert (
        "If you intend to edit the suite please use `great_expectations suite edit foop`."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_creates_notebook_and_opens_jupyter(
    mock_subprocess, mock_emit, caplog, titanic_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command

    The command should:
    - create a new notebook
    - open the notebook in jupyter
    - send a DataContext init success message
    - send a scaffold success message
    """
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    suite_name = "foop"
    expected_notebook_path = os.path.join(
        root_dir, context.GE_EDIT_NOTEBOOK_DIR, f"scaffold_{suite_name}.ipynb"
    )
    assert not os.path.isfile(expected_notebook_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "scaffold", suite_name, "-d", root_dir],
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0
    assert os.path.isfile(expected_notebook_path)

    assert mock_subprocess.call_count == 1
    assert mock_subprocess.call_args_list == [
        mock.call(["jupyter", "notebook", expected_notebook_path])
    ]
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": True}
        ),
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_creates_notebook_with_no_jupyter_flag(
    mock_subprocess, mock_emit, caplog, titanic_data_context_stats_enabled
):
    """
    We call the "suite scaffold --no-jupyter"

    The command should:
    - create a new notebook
    - NOT open the notebook in jupyter
    - tell the user to open the notebook
    - send a DataContext init success message
    - send a scaffold success message
    """
    context = titanic_data_context_stats_enabled
    root_dir = context.root_directory
    suite_name = "foop"
    expected_notebook_path = os.path.join(
        root_dir, context.GE_EDIT_NOTEBOOK_DIR, f"scaffold_{suite_name}.ipynb"
    )
    assert not os.path.isfile(expected_notebook_path)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["suite", "scaffold", suite_name, "-d", root_dir, "--no-jupyter"],
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0
    assert os.path.isfile(expected_notebook_path)
    assert (
        f"To continue scaffolding this suite, run `jupyter notebook {expected_notebook_path}`"
        in stdout
    )

    assert mock_subprocess.call_count == 0
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": True}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)
