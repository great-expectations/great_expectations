import os
import shutil
from unittest import mock

import pytest

from great_expectations import exceptions as ge_exceptions
from great_expectations.cli import toolkit
from great_expectations.cli.toolkit import (
    get_relative_path_from_config_file_to_base_path,
    is_cloud_file_url,
)
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


def test_parse_cli_config_file_location_posix_paths(tmp_path_factory):
    """
    What does this test and why?
    We want to parse posix paths into their directory and filename parts
    so that we can pass the directory to our data context constructor.
    We need to be able to do that with all versions of path that can be input.
    This tests for posix paths for files/dirs that don't exist and files/dirs that do.
    Other tests handle testing for windows support.
    """

    filename_fixtures = [
        {
            "input_path": "just_a_file.yml",
            "expected": {
                "directory": "",
                "filename": "just_a_file.yml",
            },
        },
    ]
    absolute_path_fixtures = [
        {
            "input_path": "/path/to/file/filename.yml",
            "expected": {
                "directory": "/path/to/file",
                "filename": "filename.yml",
            },
        },
        {
            "input_path": "/absolute/directory/ending/slash/",
            "expected": {
                "directory": "/absolute/directory/ending/slash/",
                "filename": None,
            },
        },
        {
            "input_path": "/absolute/directory/ending/no/slash",
            "expected": {
                "directory": "/absolute/directory/ending/no/slash",
                "filename": None,
            },
        },
    ]
    relative_path_fixtures = [
        {
            "input_path": "relative/path/to/file.yml",
            "expected": {
                "directory": "relative/path/to",
                "filename": "file.yml",
            },
        },
        {
            "input_path": "relative/path/to/directory/slash/",
            "expected": {
                "directory": "relative/path/to/directory/slash/",
                "filename": None,
            },
        },
        {
            "input_path": "relative/path/to/directory/no_slash",
            "expected": {
                "directory": "relative/path/to/directory/no_slash",
                "filename": None,
            },
        },
    ]

    fixtures = filename_fixtures + absolute_path_fixtures + relative_path_fixtures

    for fixture in fixtures:
        with pytest.raises(ge_exceptions.ConfigNotFoundError):
            toolkit.parse_cli_config_file_location(fixture["input_path"])

        # Create files and re-run assertions
    root_dir = tmp_path_factory.mktemp("posix")
    root_dir = str(root_dir)
    for fixture in fixtures:
        expected_dir = fixture.get("expected").get("directory")

        # Make non-absolute path
        if expected_dir is not None and expected_dir.startswith("/"):
            expected_dir = expected_dir[1:]

        expected_filename = fixture.get("expected").get("filename")
        if expected_dir:
            test_directory = os.path.join(root_dir, expected_dir)
            os.makedirs(test_directory, exist_ok=True)
            if expected_filename:
                expected_filepath = os.path.join(test_directory, expected_filename)
                with open(expected_filepath, "w") as fp:
                    pass

                output = toolkit.parse_cli_config_file_location(expected_filepath)

                assert output == {
                    "directory": os.path.join(root_dir, expected_dir),
                    "filename": expected_filename,
                }


def test_parse_cli_config_file_location_posix_paths_existing_files_with_no_extension(
    tmp_path_factory,
):

    filename_no_extension_fixtures = [
        {
            "input_path": "relative/path/to/file/no_extension",
            "expected": {
                "directory": "relative/path/to/file",
                "filename": "no_extension",
            },
        },
        {
            "input_path": "/absolute/path/to/file/no_extension",
            "expected": {
                "directory": "/absolute/path/to/file",
                "filename": "no_extension",
            },
        },
        {
            "input_path": "no_extension",
            "expected": {
                "directory": None,
                "filename": "no_extension",
            },
        },
    ]

    for fixture in filename_no_extension_fixtures:
        with pytest.raises(ge_exceptions.ConfigNotFoundError):
            toolkit.parse_cli_config_file_location(fixture["input_path"])

    # Create files and re-run assertions

    root_dir = tmp_path_factory.mktemp("posix")
    root_dir = str(root_dir)
    for fixture in filename_no_extension_fixtures:
        expected_dir = fixture.get("expected").get("directory")

        # Make non-absolute path
        if expected_dir is not None and expected_dir.startswith("/"):
            expected_dir = expected_dir[1:]

        expected_filename = fixture.get("expected").get("filename")
        if expected_dir:
            test_directory = os.path.join(root_dir, expected_dir)
            os.makedirs(test_directory, exist_ok=True)
            if expected_filename:
                expected_filepath = os.path.join(test_directory, expected_filename)
                with open(expected_filepath, "w") as fp:
                    pass

                output = toolkit.parse_cli_config_file_location(expected_filepath)

                assert output == {
                    "directory": os.path.join(root_dir, expected_dir),
                    "filename": expected_filename,
                }


def test_parse_cli_config_file_location_empty_paths():

    posix_fixtures = [
        {
            "input_path": None,
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
        {
            "input_path": "",
            "expected": {
                "directory": None,
                "filename": None,
            },
        },
    ]

    fixtures = posix_fixtures

    for fixture in fixtures:
        assert (
            toolkit.parse_cli_config_file_location(fixture["input_path"])
            == fixture["expected"]
        )


def test_parse_cli_config_file_location_windows_paths(tmp_path_factory):
    """
    What does this test and why?
    Since we are unable to test windows paths on our unix CI, this just
    tests that if a file doesn't exist we raise an error.
    Args:
        tmp_path_factory:
    Returns:
    """

    filename_fixtures = [
        {
            "input_path": "just_a_file.yml",
            "expected": {
                "directory": "",
                "filename": "just_a_file.yml",
            },
        },
    ]
    absolute_path_fixtures = [
        {
            "input_path": r"C:\absolute\windows\path\to\file.yml",
            "expected": {
                "directory": r"C:\absolute\windows\path\to",
                "filename": "file.yml",
            },
        },
        {
            "input_path": r"C:\absolute\windows\directory\ending\slash\\",
            "expected": {
                "directory": r"C:\absolute\windows\directory\ending\slash\\",
                "filename": None,
            },
        },
        {
            "input_path": r"C:\absolute\windows\directory\ending\no_slash",
            "expected": {
                "directory": r"C:\absolute\windows\directory\ending\no_slash",
                "filename": None,
            },
        },
    ]
    relative_path_fixtures = [
        {
            "input_path": r"relative\windows\path\to\file.yml",
            "expected": {
                "directory": r"relative\windows\path\to",
                "filename": "file.yml",
            },
        },
        # Double slash at end of raw string to escape slash
        {
            "input_path": r"relative\windows\path\to\directory\slash\\",
            "expected": {
                "directory": r"relative\windows\path\to\directory\slash\\",
                "filename": None,
            },
        },
        {
            "input_path": r"relative\windows\path\to\directory\no_slash",
            "expected": {
                "directory": r"relative\windows\path\to\directory\no_slash",
                "filename": None,
            },
        },
    ]
    fixtures = filename_fixtures + absolute_path_fixtures + relative_path_fixtures

    for fixture in fixtures:
        with pytest.raises(ge_exceptions.ConfigNotFoundError):
            toolkit.parse_cli_config_file_location(fixture["input_path"])

    # Create files and re-run assertions

    # We are unable to create files with windows paths on our unix test CI


def test_is_cloud_file_path_local_posix():
    assert not is_cloud_file_url("bucket/files/ ")
    assert not is_cloud_file_url("./bucket/files/ ")
    assert not is_cloud_file_url("/full/path/files/ ")


def test_is_cloud_file_path_file_url():
    assert not is_cloud_file_url("file://bucket/files/ ")
    assert not is_cloud_file_url("file://./bucket/files/ ")
    assert not is_cloud_file_url("file:///full/path/files/ ")


def test_is_cloud_file_path_ftp_url():
    assert is_cloud_file_url("ftp://bucket/files/ ")
    assert is_cloud_file_url("ftp://./bucket/files/ ")
    assert is_cloud_file_url("ftp:///full/path/files/ ")


def test_is_cloud_file_path_s3():
    assert is_cloud_file_url("s3://bucket/files/")
    assert is_cloud_file_url(" s3://bucket/files/ ")


def test_is_cloud_file_path_google_storage():
    assert is_cloud_file_url("gs://bucket/files/")
    assert is_cloud_file_url(" gs://bucket/files/ ")


def test_is_cloud_file_path_azure_storage():
    assert is_cloud_file_url("wasb://bucket/files/")
    assert is_cloud_file_url(" wasb://bucket/files/ ")


def test_is_cloud_file_path_http_url():
    assert is_cloud_file_url("http://bucket/files/")
    assert is_cloud_file_url(" http://bucket/files/ ")
    assert is_cloud_file_url("https://bucket/files/")
    assert is_cloud_file_url(" https://bucket/files/ ")


@pytest.fixture
def simulated_project_directories(tmp_path_factory):
    """
    Using a wacky simulated directory structure allows testing of permutations
    of relative, absolute, and current working directories.

    /random/pytest/dir/projects/pipeline1/great_expectations
    /random/pytest/dir/projects/data/pipeline1
    """
    test_dir = tmp_path_factory.mktemp("projects", numbered=False)
    assert os.path.isabs(test_dir)

    ge_dir = os.path.join(test_dir, "pipeline1", "great_expectations")
    os.makedirs(ge_dir)
    assert os.path.isdir(ge_dir)

    data_dir = os.path.join(test_dir, "data", "pipeline1")
    os.makedirs(data_dir)
    assert os.path.isdir(data_dir)

    yield ge_dir, data_dir
    shutil.rmtree(test_dir)


def test_get_relative_path_from_config_file_to_data_base_file_path_from_within_ge_directory_and_relative_data_path(
    monkeypatch, simulated_project_directories
):
    """
    This test simulates using the CLI from within the great_expectations
    directory.

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1

    cwd: /projects/pipeline1/great_expectations
    data: ../../data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    monkeypatch.chdir(ge_dir)
    assert str(os.path.abspath(os.path.curdir)) == str(ge_dir)

    obs = get_relative_path_from_config_file_to_base_path(
        ge_dir, os.path.join("..", "..", "data", "pipeline1")
    )
    assert obs == os.path.join("..", "..", "data", "pipeline1")


def test_get_relative_path_from_config_file_to_data_base_file_path_from_within_ge_directory_and_absolute_data_path(
    monkeypatch, simulated_project_directories
):
    """
    This test simulates using the CLI from within the great_expectations
    directory and using an absolute path.

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1

    cwd: /projects/pipeline1/great_expectations
    data: /projects/data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    monkeypatch.chdir(ge_dir)
    assert str(os.path.abspath(os.path.curdir)) == str(ge_dir)

    absolute_path = os.path.abspath(os.path.join("..", "..", "data", "pipeline1"))
    obs = get_relative_path_from_config_file_to_base_path(ge_dir, absolute_path)
    assert obs == os.path.join("..", "..", "data", "pipeline1")


def test_get_relative_path_from_config_file_to_data_base_file_path_from_adjacent_directory_and_relative_data_path(
    monkeypatch, simulated_project_directories
):
    """
    This test simulates using the CLI from a directory containing the
    great_expectations directory.

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1

    cwd: /projects/pipeline1
    data: ../data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    adjacent_dir = os.path.dirname(ge_dir)
    monkeypatch.chdir(adjacent_dir)
    assert str(os.path.abspath(os.path.curdir)) == str(adjacent_dir)

    obs = get_relative_path_from_config_file_to_base_path(
        ge_dir, os.path.join("..", "data", "pipeline1")
    )
    assert obs == os.path.join("..", "..", "data", "pipeline1")


def test_get_relative_path_from_config_file_to_data_base_file_path_from_adjacent_directory_and_absolute_data_path(
    monkeypatch, simulated_project_directories
):
    """
    This test simulates using the CLI from a directory containing the
    great_expectations directory and using an absolute path.

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1

    cwd: /projects/pipeline1
    data: /projects/data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    adjacent_dir = os.path.dirname(ge_dir)
    monkeypatch.chdir(adjacent_dir)
    assert str(os.path.abspath(os.path.curdir)) == str(adjacent_dir)

    absolute_path = os.path.abspath(os.path.join("..", "data", "pipeline1"))
    obs = get_relative_path_from_config_file_to_base_path(ge_dir, absolute_path)
    assert obs == os.path.join("..", "..", "data", "pipeline1")


def test_get_relative_path_from_config_file_to_data_base_file_path_from_misc_directory_and_relative_data_path(
    monkeypatch, misc_directory, simulated_project_directories
):
    """
    This test simulates using the CLI with the --config flag operating from a
    random directory

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1
    /tmp_path/misc

    cwd: /tmp_path/random
    data: ../../projects/data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    monkeypatch.chdir(misc_directory)
    assert str(os.path.abspath(os.path.curdir)) == str(misc_directory)

    obs = get_relative_path_from_config_file_to_base_path(
        ge_dir, os.path.join("..", "..", "projects", "data", "pipeline1")
    )
    assert obs == os.path.join("..", "..", "data", "pipeline1")


def test_get_relative_path_from_config_file_to_data_base_file_path_from_misc_directory_and_absolute_data_path(
    monkeypatch, misc_directory, simulated_project_directories
):
    """
    This test simulates using the CLI with the --config flag operating from a
    random directory and using an absolute path.

    /projects/pipeline1/great_expectations
    /projects/data/pipeline1
    /tmp_path/misc

    cwd: /tmp_path/misc
    data: /projects/data/pipeline1
    expected results in yaml: ../../data/pipeline1
    """
    ge_dir, data_dir = simulated_project_directories
    monkeypatch.chdir(misc_directory)
    assert str(os.path.abspath(os.path.curdir)) == str(misc_directory)

    absolute_path = os.path.abspath(
        os.path.join("..", "..", "projects", "data", "pipeline1")
    )
    obs = get_relative_path_from_config_file_to_base_path(ge_dir, absolute_path)
    assert obs == os.path.join("..", "..", "data", "pipeline1")
