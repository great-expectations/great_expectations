from typing import List
from unittest import mock

import pytest
from packaging import version

from great_expectations.core.usage_statistics.execution_environment import (
    GEExecutionEnvironment,
    InstallEnvironment,
    PackageInfo,
)


@pytest.mark.parametrize(
    "input_version", ["8.8.8", "0.14.12+14.g8f54aa902.dirty", "0.1.0.post0"]
)
@mock.patch(
    "great_expectations.core.usage_statistics.execution_environment.GEExecutionEnvironment._get_all_installed_packages",
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
    input_version,
):
    """Test that we are able to retrieve installed and not installed packages in the GE execution environment."""

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
    mock_version.return_value = input_version
    get_all_installed_packages.return_value = [
        "req-package-1",
        "req-package-2",
        "dev-package-1",
        "dev-package-2",
    ]

    ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()
    expected_dependencies: List[PackageInfo] = [
        PackageInfo(
            package_name="req-package-1",
            installed=True,
            install_environment=InstallEnvironment.REQUIRED,
            version=version.Version(input_version),
        ),
        PackageInfo(
            package_name="req-package-2",
            installed=True,
            install_environment=InstallEnvironment.REQUIRED,
            version=version.Version(input_version),
        ),
        PackageInfo(
            package_name="not-installed-req-package-1",
            installed=False,
            install_environment=InstallEnvironment.REQUIRED,
            version=None,
        ),
        PackageInfo(
            package_name="not-installed-req-package-2",
            installed=False,
            install_environment=InstallEnvironment.REQUIRED,
            version=None,
        ),
        PackageInfo(
            package_name="dev-package-1",
            installed=True,
            install_environment=InstallEnvironment.DEV,
            version=version.Version(input_version),
        ),
        PackageInfo(
            package_name="dev-package-2",
            installed=True,
            install_environment=InstallEnvironment.DEV,
            version=version.Version(input_version),
        ),
        PackageInfo(
            package_name="not-installed-dev-package-1",
            installed=False,
            install_environment=InstallEnvironment.DEV,
            version=None,
        ),
        PackageInfo(
            package_name="not-installed-dev-package-2",
            installed=False,
            install_environment=InstallEnvironment.DEV,
            version=None,
        ),
    ]
    assert ge_execution_environment.dependencies == expected_dependencies
