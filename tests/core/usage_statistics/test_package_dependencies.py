from typing import List
from unittest import mock

import pytest
from packaging import version

from great_expectations.core.usage_statistics.package_dependencies import (
    GEDependencies,
    GEExecutionEnvironment,
    PackageInfo,
)


@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEExecutionEnvironment.get_all_installed_packages",
    return_value=True,
)
@mock.patch("importlib.metadata.version", return_value=True)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_dev_dependency_names",
    return_value=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_required_dependency_names",
    return_value=True,
)
def test_get_installed_packages(
    get_required_dependency_names,
    get_dev_dependency_names,
    mock_version,
    get_all_installed_packages,
):
    get_required_dependency_names.return_value = ["req-package-1", "req-package-2"]
    get_dev_dependency_names.return_value = ["dev-package-1", "dev-package-2"]
    mock_version.return_value = "8.8.8"
    get_all_installed_packages.return_value = [
        "req-package-1",
        "req-package-2",
        "dev-package-1",
        "dev-package-2",
    ]

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
    "great_expectations.core.usage_statistics.package_dependencies.GEExecutionEnvironment.get_all_installed_packages",
    return_value=True,
)
@mock.patch(
    "importlib.metadata.version",
    return_value=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_dev_dependency_names",
    return_value=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_required_dependency_names",
    return_value=True,
)
def test_get_not_installed_packages(
    get_required_dependency_names,
    get_dev_dependency_names,
    mock_version,
    get_all_installed_packages,
):
    """
    This test raises metadata.PackageNotFoundError when calling importlib.metadata.version
    which is raised when a package is not installed

    """
    get_required_dependency_names.return_value = [
        "req-package-1",
        "req-package-2",
        "not-installed-req-package-1",
        "not-installed-req-package-2",
    ]
    get_dev_dependency_names.return_value = [
        "dev-package-1",
        "dev-package-2",
        "not-installed-dev-package-1",
        "not-installed-dev-package-2",
    ]
    mock_version.return_value = "8.8.8"
    get_all_installed_packages.return_value = [
        "req-package-1",
        "req-package-2",
        "dev-package-1",
        "dev-package-2",
    ]

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


@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEExecutionEnvironment.get_all_installed_packages",
    return_value=True,
)
@mock.patch(
    "importlib.metadata.version",
    return_value=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_dev_dependency_names",
    return_value=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.package_dependencies.GEDependencies.get_required_dependency_names",
    return_value=True,
)
def test_get_installed_and_not_installed_packages(
    get_required_dependency_names,
    get_dev_dependency_names,
    mock_version,
    get_all_installed_packages,
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
    mock_version.return_value = "8.8.8"
    get_all_installed_packages.return_value = [
        "req-package-1",
        "req-package-2",
        "dev-package-1",
        "dev-package-2",
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


def test_get_required_dependency_names_fail(ge_required_dependency_names):
    # TODO: How to make this a valuable test?
    ge_dependencies: GEDependencies = GEDependencies()
    with pytest.raises(AssertionError):
        assert (
            ge_dependencies.get_required_dependency_names()
            == ge_required_dependency_names.extend(["bad_dependency"])
        )


def test__get_dependency_names():
    """
    Test that the regex parses a requirements file correctly
    Returns:

    """
    mock_dependencies: List[str] = [
        "# This is a comment",
        "#This is another comment",
        "--requirement requirements.txt",
        "latest-package",
        "duplicate-package",
        "duplicate-package",  # Should not be filtered in this method
        "pinned-package1==0.2.4",
        "pinned-package2==0.2.4 # With comment after",
        "lower-bound-package1>=5.2.4",
        "lower-bound-package2>=5.2.4 # With comment after",
        "lower-bound-package3>5.2.4",
        "upper-bound-package1<=2.3.8",
        "upper-bound-package2<=2.3.8 # With comment after",
        "upper-bound-package3<2.3.8",
        "two-bounds-package1>=0.8.4,<2.0.0",
        "two-bounds-package2>=0.8.4,<2.0.0 # With comment after",
        "package_with_underscores",
        "1",
        "-",
        "",
    ]
    expected_dependendencies: List[str] = [
        "latest-package",
        "duplicate-package",
        "duplicate-package",
        "pinned-package1",
        "pinned-package2",
        "lower-bound-package1",
        "lower-bound-package2",
        "lower-bound-package3",
        "upper-bound-package1",
        "upper-bound-package2",
        "upper-bound-package3",
        "two-bounds-package1",
        "two-bounds-package2",
        "package_with_underscores",
        "1",
        "-",
    ]
    ge_dependencies: GEDependencies = GEDependencies()
    observed_dependencies = ge_dependencies._get_dependency_names(mock_dependencies)
    assert observed_dependencies == expected_dependendencies


@pytest.mark.integration
def test_required_dependency_names_match_requirements_file():
    ge_dependencies: GEDependencies = GEDependencies()
    assert (
        ge_dependencies.get_required_dependency_names()
        == ge_dependencies.get_required_dependency_names_from_requirements_file()
    )


@pytest.mark.integration
def test_dev_dependency_names_match_requirements_file():
    ge_dependencies: GEDependencies = GEDependencies()
    assert (
        ge_dependencies.get_dev_dependency_names()
        == ge_dependencies.get_dev_dependency_names_from_requirements_file()
    )


@pytest.mark.integration
def test__get_dependency_names_from_requirements_file():
    raise NotImplementedError
