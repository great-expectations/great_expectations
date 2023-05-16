import pathlib

import pytest

from great_expectations.compatibility.pyarrow import pyarrow
from great_expectations.compatibility.google import google
from great_expectations.compatibility.docstring_parser import docstring_parser
from great_expectations.compatibility.azure import azure
from great_expectations.compatibility.not_imported import NotImported
from great_expectations.compatibility.aws import boto3
from great_expectations.compatibility.aws import botocore
from great_expectations.compatibility.pyspark import pyspark
from great_expectations.compatibility.sqlalchemy import sqlalchemy


libraries = [
    pyspark,
    boto3,
    botocore,
    azure,
    docstring_parser,
    google,
    pyarrow,
    sqlalchemy,
]

expected_files = [
    "pyarrow",
    "azure",
    "sqlalchemy_compatibility_wrappers",
    "__init__",
    "google",
    "numpy",
    "__pycache__",
    "pandas_compatibility",
    "aws",
    "README",
    "sqlalchemy",
    "sqlalchemy_and_pandas",
    "not_imported",
    "pyspark",
    "docstring_parser",
]


@pytest.mark.compatibility
def test_optional_import_fixture_completeness():
    """What does this test and why?

    Make sure we don't have optional dependencies that are not in our set of tests in test_error_raised_when_optional_import_not_installed
    """
    repo_root = pathlib.Path(__file__).parents[2]
    compatibility_dir = repo_root / "great_expectations" / "compatibility"
    for filename in compatibility_dir.iterdir():
        assert (
            filename.stem in expected_files
        ), "Please add representative import to libraries and the filename to expected_files"


@pytest.mark.unit
@pytest.mark.parametrize(
    ["package"],
    [
        pytest.param(
            library,
        )
        for library in libraries
    ],
)
@pytest.mark.compatibility
def test_error_raised_when_optional_import_not_installed(package):

    assert isinstance(package, NotImported)
    assert not package
    with pytest.raises(ModuleNotFoundError):
        _ = package.some_param
