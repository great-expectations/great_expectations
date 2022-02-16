import os
from unittest import mock

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context import BaseDataContext
from tests.cli.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)


def test_docs_help_output(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["--v3-api", "docs"], catch_exceptions=False)
    assert result.exit_code == 0
    assert "build  Build Data Docs for a project." in result.stdout
    assert "list   List known Data Docs sites." in result.stdout
    assert "clean  Remove all files from a Data Docs site." in result.stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.parametrize(
    "invocation, cli_input, expected_stdout, expected_browser_call_count",
    [
        ("--v3-api docs build", "\n", "Would you like to proceed? [Y/n]", 1),
        ("--v3-api --assume-yes docs build", None, "", 1),
        (
            "--v3-api docs build --site-name local_site",
            "\n",
            "Would you like to proceed? [Y/n]",
            1,
        ),
        ("--v3-api --assume-yes docs build --site-name local_site", None, "", 1),
        (
            "--v3-api docs build -sn local_site",
            "\n",
            "Would you like to proceed? [Y/n]",
            1,
        ),
        ("--v3-api --assume-yes docs build -sn local_site", None, "", 1),
        # Answer "n" to prompt
        ("--v3-api docs build", "n\n", "Would you like to proceed? [Y/n]", 0),
        (
            "--v3-api docs build --site-name local_site",
            "n\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        (
            "--v3-api docs build -sn local_site",
            "n\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        # All the same but with --no-view
        ("--v3-api docs build --no-view", "\n", "Would you like to proceed? [Y/n]", 0),
        ("--v3-api --assume-yes docs build --no-view", None, "", 0),
        (
            "--v3-api docs build --site-name local_site --no-view",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        (
            "--v3-api --assume-yes docs build --site-name local_site --no-view",
            None,
            "",
            0,
        ),
        (
            "--v3-api docs build -sn local_site --no-view",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        ("--v3-api --assume-yes docs build -sn local_site --no-view", None, "", 0),
        # All the same but with --nv
        ("--v3-api docs build -nv", "\n", "Would you like to proceed? [Y/n]", 0),
        ("--v3-api --assume-yes docs build -nv", None, "", 0),
        (
            "--v3-api docs build --site-name local_site -nv",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        (
            "--v3-api --assume-yes docs build --site-name local_site -nv",
            None,
            "",
            0,
        ),
        (
            "--v3-api docs build -sn local_site -nv",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
        ),
        ("--v3-api --assume-yes docs build -sn local_site -nv", None, "", 0),
    ],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_happy_paths_build_site_on_single_site_context(
    mock_webbrowser,
    mock_emit,
    invocation,
    cli_input,
    expected_stdout,
    expected_browser_call_count,
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
        invocation,
        input=cli_input,
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "The following Data Docs sites will be built" in stdout
    assert "local_site" in stdout
    if not cli_input == "n\n":
        assert "Building Data Docs..." in stdout
        assert "Done building Data Docs" in stdout
    assert expected_stdout in stdout
    assert mock_webbrowser.call_count == expected_browser_call_count

    expected_usage_stats_messages = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.build.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    if not cli_input == "n\n":
        expected_usage_stats_messages.append(
            mock.call(
                {
                    "event_payload": {},
                    "event": "data_context.build_data_docs",
                    "success": True,
                }
            ),
        )
    if expected_browser_call_count == 1:
        expected_usage_stats_messages.append(
            mock.call(
                {
                    "event_payload": {},
                    "event": "data_context.open_data_docs",
                    "success": True,
                }
            ),
        )
    build_docs_end_event_payload = {"api_version": "v3"}
    if cli_input == "n\n":
        build_docs_end_event_payload.update({"cancelled": True})
    expected_usage_stats_messages.append(
        mock.call(
            {
                "event": "cli.docs.build.end",
                "event_payload": build_docs_end_event_payload,
                "success": True,
            }
        ),
    )
    assert mock_emit.call_args_list == expected_usage_stats_messages

    context = DataContext(root_dir)
    obs_urls = context.get_docs_sites_urls()

    assert len(obs_urls) == 1
    if not cli_input == "n\n":
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


@pytest.fixture
def context_with_two_sites(titanic_data_context_stats_enabled_config_version_3):
    context = titanic_data_context_stats_enabled_config_version_3
    config = context.get_config_with_variables_substituted()
    config.data_docs_sites["team_site"] = {
        "class_name": "SiteBuilder",
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": "uncommitted/data_docs/team_site/",
        },
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    }
    temp_context = BaseDataContext(config, context_root_dir=context.root_directory)
    new_context = DataContext(context.root_directory)
    new_context.set_config(temp_context.get_config_with_variables_substituted())
    new_context._save_project_config()
    assert new_context.get_site_names() == ["local_site", "team_site"]
    return new_context


@pytest.mark.parametrize(
    "invocation, cli_input, expected_stdout, expected_browser_call_count, expected_built_site_names",
    [
        (
            "--v3-api docs build",
            "\n",
            "Would you like to proceed? [Y/n]",
            2,
            ["local_site", "team_site"],
        ),
        ("--v3-api --assume-yes docs build", None, "", 2, ["local_site", "team_site"]),
        (
            "--v3-api docs build --site-name team_site",
            "\n",
            "Would you like to proceed? [Y/n]",
            1,
            ["team_site"],
        ),
        (
            "--v3-api --assume-yes docs build --site-name team_site",
            None,
            "",
            1,
            ["team_site"],
        ),
        (
            "--v3-api docs build -sn team_site",
            "\n",
            "Would you like to proceed? [Y/n]",
            1,
            ["team_site"],
        ),
        ("--v3-api --assume-yes docs build -sn team_site", None, "", 1, ["team_site"]),
        # Answer "n" to prompt
        (
            "--v3-api docs build",
            "n\n",
            "Would you like to proceed? [Y/n]",
            0,
            [],
        ),
        (
            "--v3-api docs build --site-name team_site",
            "n\n",
            "Would you like to proceed? [Y/n]",
            0,
            [],
        ),
        (
            "--v3-api docs build -sn team_site",
            "n\n",
            "Would you like to proceed? [Y/n]",
            0,
            [],
        ),
        # All the same but with --no-view
        (
            "--v3-api docs build --no-view",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
            ["local_site", "team_site"],
        ),
        (
            "--v3-api --assume-yes docs build --no-view",
            None,
            "",
            0,
            ["local_site", "team_site"],
        ),
        (
            "--v3-api docs build --site-name team_site --no-view",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
            ["team_site"],
        ),
        (
            "--v3-api --assume-yes docs build --site-name team_site --no-view",
            None,
            "",
            0,
            ["team_site"],
        ),
        (
            "--v3-api docs build -sn team_site --no-view",
            "\n",
            "Would you like to proceed? [Y/n]",
            0,
            ["team_site"],
        ),
        (
            "--v3-api --assume-yes docs build -sn team_site --no-view",
            None,
            "",
            0,
            ["team_site"],
        ),
    ],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_docs_build_happy_paths_build_site_on_multiple_site_context(
    mock_webbrowser,
    mock_emit,
    invocation,
    cli_input,
    expected_stdout,
    expected_browser_call_count,
    expected_built_site_names,
    caplog,
    monkeypatch,
    context_with_two_sites,
):
    context = context_with_two_sites
    assert context.get_site_names() == ["local_site", "team_site"]

    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        invocation,
        input=cli_input,
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "The following Data Docs sites will be built" in stdout
    if not cli_input == "n\n":
        assert "Building Data Docs..." in stdout
        assert "Done building Data Docs" in stdout
    assert expected_stdout in stdout
    assert mock_webbrowser.call_count == expected_browser_call_count

    expected_usage_stats_messages = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.build.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]
    if not cli_input == "n\n":
        expected_usage_stats_messages.append(
            mock.call(
                {
                    "event_payload": {},
                    "event": "data_context.build_data_docs",
                    "success": True,
                }
            ),
        )
    for _ in range(expected_browser_call_count):
        expected_usage_stats_messages.append(
            mock.call(
                {
                    "event_payload": {},
                    "event": "data_context.open_data_docs",
                    "success": True,
                }
            ),
        )

    build_docs_end_event_payload = {"api_version": "v3"}
    if cli_input == "n\n":
        build_docs_end_event_payload.update({"cancelled": True})

    expected_usage_stats_messages.append(
        mock.call(
            {
                "event": "cli.docs.build.end",
                "event_payload": build_docs_end_event_payload,
                "success": True,
            }
        ),
    )
    assert mock_emit.call_args_list == expected_usage_stats_messages

    context = DataContext(root_dir)
    for expected_site_name in expected_built_site_names:
        assert expected_site_name in stdout
        site_dir = os.path.join(
            root_dir, context.GE_UNCOMMITTED_DIR, "data_docs", expected_site_name
        )
        assert os.path.isdir(site_dir)
        # Note the fixture has no expectations or validations - only check the index
        assert os.path.isfile(os.path.join(site_dir, "index.html"))

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

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.docs.list.end",
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
    "invocation,expected_error,expected_usage_event_begin,expected_usage_event_end",
    [
        (
            "--v3-api docs clean",
            "Please specify either --all to clean all sites or a specific site using --site-name",
            "cli.docs.clean.begin",
            "cli.docs.clean.end",
        ),
        (
            "--v3-api docs clean --site-name local_site --all",
            "Please specify either --all to clean all sites or a specific site using --site-name",
            "cli.docs.clean.begin",
            "cli.docs.clean.end",
        ),
        (
            "--v3-api docs clean --site-name fake_site",
            "The specified site name `fake_site` does not exist in this project.",
            "cli.docs.clean.begin",
            "cli.docs.clean.end",
        ),
        (
            "--v3-api docs build --site-name fake_site",
            "The specified site name `fake_site` does not exist in this project.",
            "cli.docs.build.begin",
            "cli.docs.build.end",
        ),
    ],
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_docs_clean_and_build_raises_helpful_errors(
    mock_emit,
    mock_webbrowser,
    invocation,
    expected_error,
    expected_usage_event_begin,
    expected_usage_event_end,
    caplog,
    monkeypatch,
    context_with_site_built,
):
    """
    Test helpful error messages for docs build and docs clean
        - invalid site names are specified via --site-name

    For docs clean:
        - invalid combinations of the --all and --site-name flags are used
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
    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": expected_usage_event_begin,
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": expected_usage_event_end,
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

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
def test_docs_clean_happy_paths_clean_expected_sites(
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
    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.docs.clean.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.docs.clean.end",
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
