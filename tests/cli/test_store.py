import os

import mock
from click.testing import CliRunner

from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_store_list_stores(
    mock_emit, caplog, empty_data_context_stats_enabled, monkeypatch
):
    project_dir = empty_data_context_stats_enabled.root_directory
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(project_dir))
    result = runner.invoke(
        cli,
        f"--v3-api store list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    for expected_output in [
        "4 Stores found",
        "expectations_store",
        "validations_store",
        "evaluation_parameter_store",
        "checkpoint_store",
    ]:
        assert expected_output in result.output

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.store.list",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)
