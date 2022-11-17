import os
from unittest import mock
from unittest.mock import patch

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.cli.datasource import (
    SnowflakeAuthMethod,
    _prompt_for_snowflake_auth_method,
)
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

# TODO this test errors out in CI with a bash 137. It was deemed low enough risk
#  to comment out. A backlog task was created to address this after release.
# @mock.patch("subprocess.call", return_value=True, side_effect=None)
# def test_snowflake_user_password_credentials_generates_notebook(
#     mock_subprocess, caplog, empty_data_context, monkeypatch
# ):
#     root_dir = empty_data_context.root_directory
#     context = DataContext(root_dir)
#
#     runner = CliRunner(mix_stderr=False)
#     monkeypatch.chdir(os.path.dirname(context.root_directory))
#     result = runner.invoke(
#         cli,
#         "--v3-api datasource new",
#         catch_exceptions=False,
#         input="2\n4\n1\n",
#     )
#
#     stdout = result.stdout.strip()
#
#     assert "What data would you like Great Expectations to connect to?" in stdout
#     assert "Which database backend are you using?" in stdout
#
#     uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
#     expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
#     assert os.path.isfile(expected_notebook)
#     mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])
#
#     # We don't have a snowflake account to use for testing, therefore we do not
#     # want to run the notebook, as it will hang as it tries to connect.
#     assert_no_logging_messages_or_tracebacks(caplog, result)


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
