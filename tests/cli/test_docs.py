import os

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

try:
    from unittest import mock
except ImportError:
    from unittest import mock


def test_docs_help_output(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["docs"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "build  Build Data Docs for a project." in result.stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_view(
    mock_webbrowser, caplog, site_builder_data_context_with_html_store_titanic_random
):
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["docs", "build", "-d", root_dir], input="\n", catch_exceptions=False
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert "Building" in stdout
    assert "The following Data Docs sites will be built:" in stdout
    assert "great_expectations/uncommitted/data_docs/local_site/index.html" in stdout

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 2
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )
    local_site_dir = os.path.join(root_dir, "uncommitted/data_docs/local_site/")

    assert os.path.isdir(os.path.join(local_site_dir))
    assert os.path.isfile(os.path.join(local_site_dir, "index.html"))
    assert os.path.isdir(os.path.join(local_site_dir, "expectations"))
    assert os.path.isdir(os.path.join(local_site_dir, "validations"))
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_no_view(
    mock_webbrowser, caplog, site_builder_data_context_with_html_store_titanic_random
):
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["docs", "build", "--no-view", "-d", root_dir],
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0
    assert "Building" in stdout
    assert "The following Data Docs sites will be built:" in stdout
    assert "great_expectations/uncommitted/data_docs/local_site/index.html" in stdout

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 2
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )
    local_site_dir = os.path.join(root_dir, "uncommitted/data_docs/local_site/")

    assert os.path.isdir(os.path.join(local_site_dir))
    assert os.path.isfile(os.path.join(local_site_dir, "index.html"))
    assert os.path.isdir(os.path.join(local_site_dir, "expectations"))
    assert os.path.isdir(os.path.join(local_site_dir, "validations"))
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_docs_build_assume_yes(
    caplog, site_builder_data_context_with_html_store_titanic_random
):
    root_dir = site_builder_data_context_with_html_store_titanic_random.root_directory

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["docs", "build", "--no-view", "--assume-yes", "-d", root_dir],
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Would you like to proceed? [Y/n]:" not in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)
