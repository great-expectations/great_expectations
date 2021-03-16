from click.testing import CliRunner

from great_expectations.cli import cli


def test_validation_operator_with_new_api_raises_error():
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, "--v3-api validation-operator")
    assert result.exit_code == 2
    assert "Error: No such command" in result.stderr
    assert ("'validation-operator'" in result.stderr) or (
        '"validation-operator"' in result.stderr
    )
