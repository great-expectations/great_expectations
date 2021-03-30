import os

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)

try:
    from unittest import mock
except ImportError:
    from unittest import mock


def test_docs_help_output(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["--v3-api", "docs"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "build  Build Data Docs for a project." in result.stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_view(
    mock_webbrowser,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_stats_enabled_config_version_3,
):
    context = titanic_data_context_stats_enabled_config_version_3
    root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api docs build",
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert "Would you like to proceed?" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites will be built:" in stdout
    assert "local_site" in stdout
    assert "great_expectations/uncommitted/data_docs/local_site/index.html" in stdout

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.open_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.docs.build",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )
    site_dir = os.path.join(
        root_dir, context.GE_UNCOMMITTED_DIR, "data_docs", "local_site"
    )
    assert os.path.isdir(site_dir)
    # Note the fixture has no expectations or validations - only check the index
    assert os.path.isfile(os.path.join(site_dir, "index.html"))

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_no_view(
    mock_webbrowser,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_stats_enabled_config_version_3,
):
    context = titanic_data_context_stats_enabled_config_version_3
    root_dir = context.root_directory

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api docs build --no-view",
        input="\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0

    assert "Would you like to proceed?" in stdout
    assert "Building" in stdout
    assert "The following Data Docs sites will be built:" in stdout
    assert "local_site" in stdout
    assert "great_expectations/uncommitted/data_docs/local_site/index.html" in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.docs.build",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    assert (
        "great_expectations/uncommitted/data_docs/local_site/index.html"
        in obs_urls[0]["site_url"]
    )
    site_dir = os.path.join(
        root_dir, context.GE_UNCOMMITTED_DIR, "data_docs", "local_site"
    )
    assert os.path.isdir(site_dir)
    # Note the fixture has no expectations or validations - only check the index
    assert os.path.isfile(os.path.join(site_dir, "index.html"))

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_assume_yes(
    mock_webbrowser,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_stats_enabled_config_version_3,
):
    context = titanic_data_context_stats_enabled_config_version_3

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api --assume-yes docs build --no-view",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 0

    assert "Would you like to proceed? [Y/n]:" not in stdout
    # This assertion is extra assurance since this test is too permissive if we change the confirmation message
    assert "[Y/n]" not in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event_payload": {},
                "event": "data_context.build_data_docs",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.docs.build",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_docs_list_with_no_sites(
    caplog, monkeypatch, titanic_data_context_no_data_docs
):
    context = titanic_data_context_no_data_docs

    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api docs list",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "No Data Docs sites found" in stdout

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_docs_list(
    mock_emit, caplog, monkeypatch, titanic_data_context_stats_enabled_config_version_3
):
    context = titanic_data_context_stats_enabled_config_version_3
    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api docs list",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "1 Data Docs site configured:" in stdout
    assert "local_site" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.list",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@pytest.fixture
def context_with_site_built(titanic_data_context_stats_enabled_config_version_3):
    context = titanic_data_context_stats_enabled_config_version_3
    context.build_data_docs()
    obs_urls = context.get_docs_sites_urls()
    assert len(obs_urls) == 1
    expected_index_path = os.path.join(
        context.root_directory,
        context.GE_UNCOMMITTED_DIR,
        "data_docs",
        "local_site",
        "index.html",
    )
    assert os.path.isfile(expected_index_path)
    return context


@pytest.mark.parametrize(
    "invocation,expected_error",
    [
        (
            "--v3-api docs clean",
            "Please specify either --all to remove all sites or a specific site using --site_name",
        ),
        (
            "--v3-api docs clean --site-name local_site --all",
            "Please specify either --all to remove all sites or a specific site using --site_name",
        ),
        (
            "--v3-api docs clean --site-name fake_site",
            "The specified site name `fake_site` does not exist in this project.",
        ),
    ],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_docs_clean_raises_helpful_errors(
    mock_emit, invocation, expected_error, caplog, monkeypatch, context_with_site_built
):
    """
    Test that helpful error messages are raised when:
        - invalid combinations of the --all and --site-name flags are used
        - invalid site names are specified
    """
    context = context_with_site_built
    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        invocation,
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 1
    assert expected_error in stdout
    assert "Cleaned data docs" not in stdout
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.clean",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]
    expected_index_path = os.path.join(
        context.root_directory,
        context.GE_UNCOMMITTED_DIR,
        "data_docs",
        "local_site",
        "index.html",
    )
    assert os.path.isfile(expected_index_path)

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@pytest.mark.parametrize(
    "invocation",
    [
        "--v3-api docs clean --all",
        "--v3-api docs clean -a",
        "--v3-api docs clean --site-name local_site",
        "--v3-api docs clean -s local_site",
    ],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_docs_clean_valid_site_name_and_all_flag_combinations_clean_sites(
    mock_emit, invocation, caplog, monkeypatch, context_with_site_built
):
    context = context_with_site_built
    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        invocation,
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "Cleaned data docs" in stdout
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.clean",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    expected_index_path = os.path.join(
        context.root_directory,
        context.GE_UNCOMMITTED_DIR,
        "data_docs",
        "local_site",
        "index.html",
    )
    assert not os.path.isfile(expected_index_path)

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )
