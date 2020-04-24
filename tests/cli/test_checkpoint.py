from click.testing import CliRunner

from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_checkpoint_list(caplog, empty_context_with_checkpoint):
    context = empty_context_with_checkpoint
    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        f"checkpoint list -d {root_dir}",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert result.exit_code == 0
    assert "Found 1 checkpoint." in stdout
    assert "my_checkpoint" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)
