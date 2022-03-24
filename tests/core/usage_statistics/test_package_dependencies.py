from importlib import metadata
from typing import List
from unittest import mock

import pytest
from packaging import version

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


@mock.patch("importlib.metadata.version", return_value=True, side_effect=None)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_dev_dependency_names",
    return_value=True,
    side_effect=None,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_required_dependency_names",
    return_value=True,
    side_effect=None,
)
def test_get_installed_packages(
    get_required_dependency_names, get_dev_dependency_names, mock_version
):
    get_required_dependency_names.return_value = ["req-package-1", "req-package-2"]
    get_dev_dependency_names.return_value = ["dev-package-1", "dev-package-2"]
    mock_version.return_value = "8.8.8"

    ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()

    assert ge_execution_environment.installed_required_dependencies == [
        PackageInfo(
            package_name="req-package-1",
            installed=True,
            version=version.Version("8.8.8"),
        ),
        PackageInfo(
            package_name="req-package-2",
            installed=True,
            version=version.Version("8.8.8"),
        ),
    ]

    assert ge_execution_environment.installed_dev_dependencies == [
        PackageInfo(
            package_name="dev-package-1",
            installed=True,
            version=version.Version("8.8.8"),
        ),
        PackageInfo(
            package_name="dev-package-2",
            installed=True,
            version=version.Version("8.8.8"),
        ),
    ]


@mock.patch(
    "importlib.metadata.version",
    return_value=False,
    side_effect=metadata.PackageNotFoundError,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_dev_dependency_names",
    return_value=True,
    side_effect=None,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_required_dependency_names",
    return_value=True,
    side_effect=None,
)
def test_get_not_installed_packages(
    get_required_dependency_names, get_dev_dependency_names, mock_version
):
    """
    This test raises metadata.PackageNotFoundError when calling importlib.metadata.version
    which is raised when a package is not installed

    """
    get_required_dependency_names.return_value = [
        "not-installed-req-package-1",
        "not-installed-req-package-2",
    ]
    get_dev_dependency_names.return_value = [
        "not-installed-dev-package-1",
        "not-installed-dev-package-2",
    ]

    ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()

    assert ge_execution_environment.not_installed_required_dependencies == [
        PackageInfo(
            package_name="not-installed-req-package-1", installed=False, version=None
        ),
        PackageInfo(
            package_name="not-installed-req-package-2", installed=False, version=None
        ),
    ]

    assert ge_execution_environment.not_installed_dev_dependencies == [
        PackageInfo(
            package_name="not-installed-dev-package-1", installed=False, version=None
        ),
        PackageInfo(
            package_name="not-installed-dev-package-2", installed=False, version=None
        ),
    ]


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
