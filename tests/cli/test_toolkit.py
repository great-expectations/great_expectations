import os
from unittest import mock

import pytest

from great_expectations.cli import toolkit
from great_expectations.data_context import DataContext
from great_expectations.exceptions import UnsupportedConfigVersionError


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_launch_jupyter_notebook_env_none(mock_subprocess):
    try:
        if os.environ.get("GE_JUPYTER_CMD"):
            temp = os.environ.great_expectations("GE_JUPYTER_CMD")
            del os.environ["GE_JUPYTER_CMD"]
        toolkit.launch_jupyter_notebook("test_path")
        mock_subprocess.assert_called_once_with(["jupyter", "notebook", "test_path"])
    except Exception:
        if temp:
            os.environ["GE_JUPYTER_CMD"] = temp
        raise


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_launch_jupyter_notebook_env_set_in_env(mock_subprocess):
    with mock.patch.dict(
        os.environ, {"GE_JUPYTER_CMD": "jupyter notebook --test-args"}, clear=True
    ):
        toolkit.launch_jupyter_notebook("test_path")
        mock_subprocess.assert_called_once_with(
            "jupyter notebook --test-args test_path", shell=True
        )


def test_load_data_context_with_error_handling_v1_config(v10_project_directory):
    with pytest.raises(UnsupportedConfigVersionError):
        DataContext(context_root_dir=v10_project_directory)


def test_parse_cli_config_file_location():

    input_path = "just_a_file.yml"
    expected = {
        "directory": "",
        "filename": "just_a_file.yml",
    }
    assert toolkit.parse_cli_config_file_location(input_path) == expected

    input_path = "/path/to/file/filename.yml"
    expected = {
        "directory": "/path/to/file",
        "filename": "filename.yml",
    }
    assert toolkit.parse_cli_config_file_location(input_path) == expected

    input_path = "/just/a/directory/ending/slash/"
    expected = {
        "directory": "/just/a/directory/ending/slash/",
        "filename": None,
    }
    assert toolkit.parse_cli_config_file_location(input_path) == expected

    input_path = "/just/a/directory/no/slash"
    expected = {
        "directory": "/just/a/directory/no/slash/",
        "filename": None,
    }
    assert toolkit.parse_cli_config_file_location(input_path) == expected

    input_path = None
    expected = {
        "directory": None,
        "filename": None,
    }
    assert toolkit.parse_cli_config_file_location(input_path) == expected
