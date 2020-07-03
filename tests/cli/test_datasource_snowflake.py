from unittest.mock import patch

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.cli.datasource import _collect_snowflake_credentials


def test_snowflake_user_password_credentials_exit(empty_data_context):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir],
        catch_exceptions=False,
        input="2\n4\nmy_snowflake_db\n1\nuser\nABCD.us-east-1\ndefault_db\ndefault_schema\nxsmall\npublic\npassword\nn\n",
    )

    stdout = result.output.strip()
    assert "Ok, exiting now" in stdout


@patch("click.prompt")
def test_snowflake_user_password_credentials(mock_prompt):
    mock_prompt.side_effect = [
        "1",
        "user",
        "ABCD.us-east-1",
        "default_db",
        "default_schema",
        "xsmall",
        "public",
        "password",
    ]

    credentials = _collect_snowflake_credentials(None)

    assert credentials == {
        "drivername": "snowflake",
        "database": "default_db",
        "host": "ABCD.us-east-1",
        "password": "password",
        "query": {"role": "public", "schema": "default_schema", "warehouse": "xsmall"},
        "username": "user",
    }


@patch("click.prompt")
def test_snowflake_sso_credentials(mock_prompt):
    mock_prompt.side_effect = [
        "2",
        "user",
        "ABCD.us-east-1",
        "default_db",
        "default_schema",
        "xsmall",
        "public",
        "externalbrowser",
    ]

    credentials = _collect_snowflake_credentials(None)

    assert credentials == {
        "drivername": "snowflake",
        "database": "default_db",
        "host": "ABCD.us-east-1",
        "connect_args": {"authenticator": "externalbrowser",},
        "query": {"role": "public", "schema": "default_schema", "warehouse": "xsmall"},
        "username": "user",
    }
