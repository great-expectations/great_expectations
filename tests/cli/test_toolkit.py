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


def test_parse_cli_config_file_location_posix_paths():

    filename_fixtures = [
        {
            "input_path": "just_a_file.yml",
            "windows": False,
            "expected": {
                "directory": ".",
                "filename": "just_a_file.yml",
            },
        },
    ]
    absolute_path_fixtures = [
        {
            "input_path": "/path/to/file/filename.yml",
            "windows": False,
            "expected": {
                "directory": "/path/to/file",
                "filename": "filename.yml",
            },
        },
        {
            "input_path": "/absolute/directory/ending/slash/",
            "windows": False,
            "expected": {
                "directory": "/absolute/directory/ending/slash/",
                "filename": None,
            },
        },
        {
            "input_path": "/absolute/directory/ending/no/slash",
            "windows": False,
            "expected": {
                "directory": "/absolute/directory/ending/no/slash",
                "filename": None,
            },
        },
    ]

    relative_path_fixtures = [
        {
            "input_path": "relative/path/to/file.yml",
            "windows": False,
            "expected": {
                "directory": "relative/path/to",
                "filename": "file.yml",
            },
        },
        {
            "input_path": "relative/path/to/directory/slash/",
            "windows": False,
            "expected": {
                "directory": "relative/path/to/directory/slash/",
                "filename": None,
            },
        },
        {
            "input_path": "relative/path/to/directory/no_slash",
            "windows": False,
            "expected": {
                "directory": "relative/path/to/directory/no_slash",
                "filename": None,
            },
        },
    ]

    fixtures = filename_fixtures + absolute_path_fixtures + relative_path_fixtures

    for fixture in fixtures:
        assert (
            toolkit.parse_cli_config_file_location(
                fixture["input_path"], windows=fixture.get("windows")
            )
            == fixture["expected"]
        )


def test_parse_cli_config_file_location_empty_paths():

    posix_fixtures = [
        {
            "input_path": None,
            "windows": False,
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
        {
            "input_path": "",
            "windows": False,
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
    ]
    windows_fixtures = [
        {
            "input_path": None,
            "windows": True,
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
        {
            "input_path": "",
            "windows": True,
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
    ]

    fixtures = posix_fixtures + windows_fixtures

    for fixture in fixtures:
        assert (
            toolkit.parse_cli_config_file_location(
                fixture["input_path"], windows=fixture.get("windows")
            )
            == fixture["expected"]
        )


def test_parse_cli_config_file_location_windows_paths():

    filename_fixtures = [
        {
            "input_path": "just_a_file.yml",
            "windows": True,
            "expected": {
                "directory": ".",
                "filename": "just_a_file.yml",
            },
        },
    ]
    absolute_path_fixtures = [
        {
            "input_path": r"C:\absolute\windows\path\to\file.yml",
            "windows": True,
            "expected": {
                "directory": r"C:\absolute\windows\path\to",
                "filename": "file.yml",
            },
        },
        {
            "input_path": r"C:\absolute\windows\directory\ending\slash\\",
            "windows": True,
            "expected": {
                "directory": r"C:\absolute\windows\directory\ending\slash\\",
                "filename": None,
            },
        },
        {
            "input_path": r"C:\absolute\windows\directory\ending\no_slash",
            "windows": True,
            "expected": {
                "directory": r"C:\absolute\windows\directory\ending\no_slash",
                "filename": None,
            },
        },
    ]
    relative_path_fixtures = [
        {
            "input_path": r"relative\windows\path\to\file.yml",
            "windows": True,
            "expected": {
                "directory": r"relative\windows\path\to",
                "filename": "file.yml",
            },
        },
        # Double slash at end of raw string to escape slash
        {
            "input_path": r"relative\windows\path\to\directory\slash\\",
            "windows": True,
            "expected": {
                "directory": r"relative\windows\path\to\directory\slash\\",
                "filename": None,
            },
        },
        {
            "input_path": r"relative\windows\path\to\directory\no_slash",
            "windows": True,
            "expected": {
                "directory": r"relative\windows\path\to\directory\no_slash",
                "filename": None,
            },
        },
    ]
    fixtures = filename_fixtures + absolute_path_fixtures + relative_path_fixtures

    for fixture in fixtures:
        assert (
            toolkit.parse_cli_config_file_location(
                fixture["input_path"], windows=fixture.get("windows")
            )
            == fixture["expected"]
        )
