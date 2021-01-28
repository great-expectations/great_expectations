from copy import deepcopy
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.cli.datasource import _collect_snowflake_credentials
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import DatasourceKeyPairAuthBadPassphraseError


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
    assert "ok, exiting now" in stdout.lower()


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
        "connect_args": {
            "authenticator": "externalbrowser",
        },
        "query": {"role": "public", "schema": "default_schema", "warehouse": "xsmall"},
        "username": "user",
    }


@patch("click.prompt")
def test_snowflake_key_pair_credentials(mock_prompt, basic_sqlalchemy_datasource):
    database_key_path_pass = file_relative_path(
        __file__, "../test_fixtures/database_key_test.p8"
    )

    mock_prompt.side_effect = [
        "3",
        "user",
        "ABCD.us-east-1",
        "default_db",
        "default_schema",
        "xsmall",
        "public",
        database_key_path_pass,
        "test123",
    ]

    credentials = _collect_snowflake_credentials(None)

    assert credentials == {
        "drivername": "snowflake",
        "database": "default_db",
        "host": "ABCD.us-east-1",
        "private_key_path": database_key_path_pass,
        "private_key_passphrase": "test123",
        "query": {"role": "public", "schema": "default_schema", "warehouse": "xsmall"},
        "username": "user",
    }

    # making sure with the correct params the key is read correctly
    basic_sqlalchemy_datasource._get_sqlalchemy_key_pair_auth_url(
        "snowflake", deepcopy(credentials)
    )

    # check that with a bad pass phrase an informative message is returned to the user
    credentials["private_key_passphrase"] = "bad_pass"
    with pytest.raises(DatasourceKeyPairAuthBadPassphraseError) as e:
        basic_sqlalchemy_datasource._get_sqlalchemy_key_pair_auth_url(
            "snowflake", deepcopy(credentials)
        )

    assert "passphrase incorrect" in e.value.message

    # check that with no pass the key is read correctly
    database_key_path_no_pass = file_relative_path(
        __file__, "../test_fixtures/database_key_test_no_pass.p8"
    )
    credentials["private_key_path"] = database_key_path_no_pass
    credentials["private_key_passphrase"] = ""
    (
        sqlalchemy_uri,
        create_engine_kwargs,
    ) = basic_sqlalchemy_datasource._get_sqlalchemy_key_pair_auth_url(
        "snowflake", deepcopy(credentials)
    )

    assert (
        str(sqlalchemy_uri)
        == "snowflake://user@ABCD.us-east-1/default_db?role=public&schema=default_schema&warehouse=xsmall"
    )
    assert create_engine_kwargs.get("connect_args", {}).get(
        "private_key", ""
    )  # check that the private_key is not empty
