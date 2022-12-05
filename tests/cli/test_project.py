import os
import shutil
from unittest import mock

import pytest
from click.testing import CliRunner

import great_expectations as gx
from great_expectations.cli import cli
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.util import file_relative_path
from tests.cli.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)


@pytest.fixture
def titanic_data_context_clean_usage_stats_enabled(
    tmp_path_factory, monkeypatch
) -> DataContext:
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    titanic_yml_path = file_relative_path(
        __file__, "../test_fixtures/great_expectations_v013clean_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return gx.data_context.DataContext(context_path)


@pytest.fixture
def titanic_data_context_v2_datasources_and_validation_operators_usage_stats_enabled(
    tmp_path_factory, monkeypatch
) -> DataContext:
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    os.makedirs(os.path.join(context_path, "checkpoints"), exist_ok=True)
    data_path = os.path.join(context_path, "..", "data")
    os.makedirs(os.path.join(data_path), exist_ok=True)

    titanic_yml_path = file_relative_path(
        __file__, "../test_fixtures/great_expectations_v013_titanic.yml"
    )
    shutil.copy(
        titanic_yml_path, str(os.path.join(context_path, "great_expectations.yml"))
    )
    titanic_csv_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(
        titanic_csv_path, str(os.path.join(context_path, "..", "data", "Titanic.csv"))
    )
    return gx.data_context.DataContext(context_path)


def test_project_check_on_missing_ge_dir_guides_user_to_fix(
    caplog, monkeypatch, tmp_path_factory
):
    project_dir = str(tmp_path_factory.mktemp("empty_dir"))
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(project_dir)
    result = runner.invoke(
        cli,
        ["project", "check-config"],
        catch_exceptions=False,
    )
    stdout = result.output
    assert "Checking your config files for validity" in stdout
    assert "Unfortunately, your config appears to be invalid" in stdout
    assert "Error: No great_expectations directory was found here!" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_project_check_on_valid_project_says_so(
    mock_emit, caplog, monkeypatch, titanic_data_context_clean_usage_stats_enabled
):

    context = titanic_data_context_clean_usage_stats_enabled
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["project", "check-config"],
        catch_exceptions=False,
    )
    assert "Checking your config files for validity" in result.output
    assert "Your config file appears valid" in result.output
    assert result.exit_code == 0

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.project.check_config",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_project_check_on_project_with_v2_datasources_and_validation_operators(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_data_context_v2_datasources_and_validation_operators_usage_stats_enabled,
):
    context = (
        titanic_data_context_v2_datasources_and_validation_operators_usage_stats_enabled
    )
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "project", "check-config"],
        catch_exceptions=False,
    )
    assert "Checking your config files for validity" in result.output
    assert (
        "Your project needs to be upgraded in order to be compatible with Great Expectations V3 API."
        in result.output
    )
    assert (
        "The following Data Sources must be upgraded manually, due to using the old Datasource format, which is being deprecated:"
        in result.output
    )
    assert "- Data Sources: mydatasource" in result.output
    assert (
        "Your configuration uses validation_operators, which are being deprecated.  Please, manually convert validation_operators to use the new Checkpoint validation unit, since validation_operators will be deleted."
        in result.output
    )
    assert "Unfortunately, your config appears to be invalid" in result.output
    assert (
        "The configuration of your great_expectations.yml is outdated.  Please consult the V3 API migration guide https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#migrating-to-the-batch-request-v3-api and upgrade your Great Expectations configuration in order to take advantage of the latest capabilities."
        in result.output
    )
    assert result.exit_code == 1

    assert mock_emit.call_count == 1
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )


def test_project_check_on_project_with_missing_config_file_guides_user(
    caplog, monkeypatch, titanic_data_context
):
    context = titanic_data_context
    # Remove the config file.
    os.remove(os.path.join(context.root_directory, "great_expectations.yml"))

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["project", "check-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Checking your config files for validity" in result.output
    assert "Unfortunately, your config appears to be invalid" in result.output
    assert_no_logging_messages_or_tracebacks(caplog, result)
