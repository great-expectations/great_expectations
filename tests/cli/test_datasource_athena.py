import os
from unittest import mock

import nbformat
import pytest
from click.testing import CliRunner

from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


pytestmark = [pytest.mark.cli]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@pytest.mark.slow  # 1.85s
def test_cli_athena_datasource_new_connection_string(
    mock_subprocess,
    mock_emit,
    empty_data_context_stats_enabled,
    caplog,
    monkeypatch,
):
    root_dir = empty_data_context_stats_enabled.root_directory
    context = empty_data_context_stats_enabled
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "datasource new",
        input="Y\n2\n7\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout

    assert "Athena" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GX_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")

    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "Athena",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.new.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)

    # Mock the user adding a connection string into the notebook by overwriting the right cell
    credentials_cell = nb["cells"][5]["source"]

    credentials = (
        "connection_string",
        "schema_name",
        "table_name",
        "region",
        "s3_path",
    )
    for credential in credentials:
        assert credential in credentials_cell

    region_test = "us-east-1"
    schema_test = "test_data"
    table_name_test = "table_test"
    connection_string = f"awsathena+rest://@athena.{region_test}.amazonaws.com/{schema_test}?s3_staging_dir=s3://YOUR_S3_BUCKET/path/to/"

    nb["cells"][5]["source"] = credentials_cell.replace("YOUR_REGION", region_test)

    credentials_cell = nb["cells"][5]["source"]
    nb["cells"][5]["source"] = credentials_cell.replace("YOUR_SCHEMA", schema_test)

    credentials_cell = nb["cells"][5]["source"]
    nb["cells"][5]["source"] = credentials_cell.replace(
        "YOUR_TABLE_NAME", table_name_test
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)
