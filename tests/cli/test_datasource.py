import pytest
from click.testing import CliRunner

from great_expectations.cli import cli
from great_expectations.cli.cli_messages import DATASOURCE_NEW_WARNING

pytestmark = [pytest.mark.cli]


def test_cli_datasource_new_warning(empty_data_context, monkeypatch):
    context = empty_data_context
    monkeypatch.chdir(context.root_directory)

    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(cli, args="datasource new", input="n")
    assert result.exit_code == 0
    assert DATASOURCE_NEW_WARNING in result.stdout
