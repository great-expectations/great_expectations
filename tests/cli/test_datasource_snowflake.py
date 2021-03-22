import os
from copy import deepcopy
from unittest import mock
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.cli.datasource import (
    SnowflakeAuthMethod,
    _prompt_for_snowflake_auth_method,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import DatasourceKeyPairAuthBadPassphraseError
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_snowflake_user_password_credentials_generates_notebook(
    mock_subprocess, caplog, empty_data_context, monkeypatch
):
    root_dir = empty_data_context.root_directory
    context = DataContext(root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource new",
        catch_exceptions=False,
        input="2\n4\n1\n",
    )

    stdout = result.stdout.strip()

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "Which database backend are you using?" in stdout

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # We don't have a snowflake account to use for testing, therefore we do not
    # want to run the notebook, as it will hang as it tries to connect.
    assert_no_logging_messages_or_tracebacks(caplog, result)


@patch("click.prompt")
def test_snowflake_auth_method_prompt_user_and_password(mock_prompt):
    mock_prompt.side_effect = ["1"]
    assert _prompt_for_snowflake_auth_method() == SnowflakeAuthMethod.USER_AND_PASSWORD


@patch("click.prompt")
def test_snowflake_auth_method_prompt_SSO(mock_prompt):
    mock_prompt.side_effect = ["2"]
    assert _prompt_for_snowflake_auth_method() == SnowflakeAuthMethod.SSO


@patch("click.prompt")
def test_snowflake_auth_method_prompt_keys(mock_prompt):
    mock_prompt.side_effect = ["3"]
    assert _prompt_for_snowflake_auth_method() == SnowflakeAuthMethod.KEY_PAIR


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

    credentials = _prompt_for_snowflake_auth_method()

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
