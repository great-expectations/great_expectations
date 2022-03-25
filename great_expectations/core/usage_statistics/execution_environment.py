"""Identify installed package dependencies.

This module contains utilities for identifying installed package dependencies
to help enable the core team to safely upgrade package dependencies to gain
access to features of new package versions.

    Typical usage example:
        ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()
        dependencies: List[PackageInfo] = ge_execution_environment.dependencies

"""

import enum
from dataclasses import dataclass
from importlib import metadata
from typing import List, Optional

from packaging import version

from great_expectations.core.usage_statistics.package_dependencies import GEDependencies


class InstallEnvironment(enum.Enum):
    DEV = "dev"
    REQUIRED = "required"


@dataclass
class PackageInfo:
    package_name: str
    installed: bool
    install_environment: InstallEnvironment
    version: Optional[version.Version]


class GEExecutionEnvironment:
    """The list of GE dependencies with version and install information.

    This does not return any dependencies that are not specified directly in
    either requirements.txt or any requirements-dev*.txt files.

    Attributes: None
    """

    def __init__(self):
        self._ge_dependencies: GEDependencies = GEDependencies()
        self._all_installed_packages = None
        self._get_all_installed_packages()

        self._dependencies = []
        self._build_required_dependencies()
        self._build_dev_dependencies()

    @property
    def dependencies(self) -> List[PackageInfo]:
        """The list of GE dependencies with version and install information.

        This does not return any dependencies that are not specified directly in
        either requirements.txt or any requirements-dev*.txt files.

        Attributes: None
        """
        return self._dependencies

    def _build_required_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependencies_info(
            self._ge_dependencies.get_required_dependency_names(),
            install_environment=InstallEnvironment.REQUIRED,
        )
        self._dependencies.extend(dependency_list)

    def _build_dev_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependencies_info(
            self._ge_dependencies.get_dev_dependency_names(),
            install_environment=InstallEnvironment.DEV,
        )
        self._dependencies.extend(dependency_list)

    def _get_all_installed_packages(self) -> List[str]:
        """Get all installed packages. Used to check if dependency is installed or not.

        Returns:
            list of string names of packages that are installed
        """
        if self._all_installed_packages is None:
            # Only retrieve once
            self._all_installed_packages = [
                item.metadata.get("Name")
                for item in metadata.distributions()
                if item.metadata.get("Name") is not None
            ]
        return self._all_installed_packages

    def _build_dependencies_info(
        self, dependency_names: List[str], install_environment: InstallEnvironment
    ) -> List[PackageInfo]:
        """Build list of info about dependencies including version and install status.

        Args:
            dependency_names: Retrieve info on these dependencies.
            install_environment: Environment these dependencies are installed in.

        Returns:
            List of PackageInfo objects for each dependency including version,
            install status, install environment.
        """
        dependencies: List[PackageInfo] = []
        for dependency_name in dependency_names:

            package_version: Optional[version.Version]
            installed: bool
            if dependency_name in self._get_all_installed_packages():
                installed = True
                package_version = version.parse(metadata.version(dependency_name))
            else:
                installed = False
                package_version = None

            dependencies.append(
                PackageInfo(
                    package_name=dependency_name,
                    installed=installed,
                    install_environment=install_environment,
                    version=package_version,
                )
            )
        return dependencies
