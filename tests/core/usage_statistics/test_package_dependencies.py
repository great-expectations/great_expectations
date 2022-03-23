from importlib import metadata
from typing import List
from unittest import mock

import pytest

from great_expectations.core.usage_statistics.package_dependencies import (
    GEDependencies,
    GEExecutionEnvironment,
    PackageInfo,
)


@pytest.fixture
def ge_required_dependency_names():
    """This should be kept in sync with our requirements.txt"""
    ge_required_dependencies: List[str] = sorted(
        [
            "altair",
            "Click",
            "colorama",
            "cryptography",
            "importlib-metadata",
            "ipywidgets",
            "jinja2",
            "jsonpatch",
            "jsonschema",
            "mistune",
            "nbformat",
            "numpy",
            "packaging",
            "pandas",
            "pyparsing",
            "python-dateutil",
            "pytz",
            "requests",
            "ruamel.yaml",
            "scipy",
            "termcolor",
            "tqdm",
            "typing-extensions",
            "urllib3",
            "tzlocal",
        ]
    )
    return ge_required_dependencies


@pytest.fixture
def ge_dev_dependency_names():
    """This should be kept in sync with our requirements-dev*.txt dependencies"""
    ge_dev_dependencies: List[str] = sorted(
        [
            "PyMySQL",
            "azure-identity",
            "azure-keyvault-secrets",
            "azure-storage-blob",
            "black",
            "boto3",
            "feather-format",
            "flake8",
            "flask",
            "freezegun",
            "gcsfs",
            "google-cloud-secret-manager",
            "google-cloud-storage",
            "isort",
            "moto",
            "nbconvert",
            "openpyxl",
            "pre-commit",
            "psycopg2-binary",
            "pyarrow",
            "pyathena",
            "pyfakefs",
            "pyodbc",
            "pypd",
            "pyspark",
            "pytest",
            "pytest-benchmark",
            "pytest-cov",
            "pytest-order",
            "pyupgrade",
            "requirements-parser",
            "s3fs",
            "snapshottest",
            "snowflake-connector-python",
            "snowflake-sqlalchemy",
            "sqlalchemy",
            "sqlalchemy-bigquery",
            "sqlalchemy-dremio",
            "sqlalchemy-redshift",
            "teradatasqlalchemy",
            "xlrd",
        ]
    )
    return ge_dev_dependencies


# mymodule = mock.MagicMock()


class MockWithVersion(mock.MagicMock):
    __version__ = "9.9.9"


mymodule = MockWithVersion()


# @mock.patch.dict(sys.modules, some_module=MockWithVersion())
@mock.patch("importlib.metadata.version", return_value=True, side_effect=None)
def test_get_imported_packages(mock_version):
    # import sys
    # module = sys.modules["scipy"]
    # print(module)
    # print(module.__version__)
    # print(type(sys.modules))
    # print(mock_sys.__version__)
    mock_version.return_value = "8.8.8"

    # mymodule2 = MockWithVersion()
    # print(mymodule2.__version__)
    #
    # v = version.parse(metadata.version("scipy"))
    # print(v)
    # print(type(v))

    try:
        print("Yay")
        # print(sys.modules["some_module"])
        # print(sys.modules["some_module"].__version__)
        module_version = metadata.version("some_module")
        print(module_version)
        print("yay2")
        # v = version.parse(metadata.version("some_module"))
        # print(v)
        # print(type(v))
    except metadata.PackageNotFoundError:
        pass


# Mock metadata.version() call with output


def test_get_required_dependency_names(ge_required_dependency_names):
    ge_dependencies: GEDependencies = GEDependencies()
    assert (
        ge_dependencies.get_required_dependency_names() == ge_required_dependency_names
    )


def test_get_required_dependency_names_fail(ge_required_dependency_names):
    ge_dependencies: GEDependencies = GEDependencies()
    with pytest.raises(AssertionError):
        assert (
            ge_dependencies.get_required_dependency_names()
            == ge_required_dependency_names
        )


def test_get_dev_dependency_names(ge_dev_dependency_names):
    ge_dependencies: GEDependencies = GEDependencies()
    assert ge_dependencies.get_dev_dependency_names() == ge_dev_dependency_names


def test__get_dependency_names_from_requirements_file():
    raise NotImplementedError


def test_sandbox():

    # ge_dependencies: GEDependencies = GEDependencies()
    ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()
    installed_required_dependencies: List[
        PackageInfo
    ] = ge_execution_environment.installed_required_dependencies
    print(installed_required_dependencies)

    installed_dev_dependencies: List[
        PackageInfo
    ] = ge_execution_environment.installed_dev_dependencies
    print(installed_dev_dependencies)

    # print(ge_dependencies.get_dev_requirements_paths())
    # print(ge_dependencies.get_dev_dependency_names())
