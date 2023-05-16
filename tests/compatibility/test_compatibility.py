from __future__ import annotations

import pathlib

import pytest

from great_expectations.compatibility.aws import boto3, botocore
from great_expectations.compatibility.azure import storage as azure_storage
from great_expectations.compatibility.docstring_parser import docstring_parser
from great_expectations.compatibility.google import storage as google_storage
from great_expectations.compatibility.not_imported import NotImported
from great_expectations.compatibility.pyarrow import pyarrow
from great_expectations.compatibility.pyspark import pyspark
from great_expectations.compatibility.sqlalchemy import sqlalchemy


@pytest.fixture
def expected_files() -> set[str, ...]:
    return {
        "pyarrow",
        "azure",
        "sqlalchemy_compatibility_wrappers",
        "google",
        "numpy",
        "pandas_compatibility",
        "aws",
        "sqlalchemy",
        "sqlalchemy_and_pandas",
        "not_imported",
        "pyspark",
        "docstring_parser",
    }


@pytest.mark.unit
@pytest.mark.compatibility
def test_optional_import_fixture_completeness(expected_files: set[str, ...]):
    """What does this test and why?

    Make sure we don't have optional dependencies that are not in tested in
    test_error_raised_when_optional_import_not_installed
    If a new file is added to the `compatibility` folder that has optional
    imports, please add a representative import to _OPTIONAL_IMPORTS so that
    we can test that an appropriate error is raised when it is used in an
    environment where the optional dependency is not installed. You'll also
    need to add the filename stem to expected_files.
    """
    repo_root = pathlib.Path(__file__).parents[2]
    compatibility_dir = repo_root / "great_expectations" / "compatibility"
    py_files_no_dunders = {
        filename.stem
        for filename in compatibility_dir.iterdir()
        if not filename.stem.startswith("__") and filename.suffix == ".py"
    }
    assert (
        py_files_no_dunders == expected_files
    ), "Please add representative import to _OPTIONAL_IMPORTS and the filename to expected_files"


_OPTIONAL_IMPORTS = (
    pyspark,
    boto3,
    botocore,
    azure_storage,
    docstring_parser,
    google_storage,
    pyarrow,
    sqlalchemy,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ["package"],
    [
        pytest.param(
            library,
        )
        for library in _OPTIONAL_IMPORTS
    ],
)
@pytest.mark.compatibility
def test_error_raised_when_optional_import_not_installed(package: NotImported):
    """What does this test and why?

    This test makes sure that an error is raised when an optionally installed
    dependency is not installed and then attempted to be used.

    The _OPTIONAL_IMPORTS tuple should include representative imports for each
    package in great_expectations/compatibility.
    """
    assert isinstance(package, NotImported)
    assert not package
    with pytest.raises(ModuleNotFoundError):
        _ = package.some_param
