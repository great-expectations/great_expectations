"""Identify installed package dependencies.

This module contains utilities for identifying installed package dependencies
to help enable the core team to safely upgrade package dependencies to gain
access to features of new package versions.

    Typical usage example:
        ge_execution_environment = GXExecutionEnvironment()
        dependencies: List[PackageInfo] = ge_execution_environment.dependencies

"""
from __future__ import annotations

import enum
import sys
from dataclasses import dataclass
from typing import Iterable, List, Optional, cast

from marshmallow import Schema, fields
from packaging import version

from great_expectations.core.usage_statistics.package_dependencies import GXDependencies

if sys.version_info < (3, 8):
    # Note: importlib_metadata is included in the python standard library as importlib
    # starting with v3.8. At the time we remove support for python v3.7
    # this conditional can be removed.
    import importlib_metadata as metadata
else:
    from importlib import metadata


class InstallEnvironment(enum.Enum):
    DEV = "dev"
    REQUIRED = "required"


@dataclass
class PackageInfo:
    package_name: str
    installed: bool
    install_environment: InstallEnvironment
    version: Optional[version.Version]


class PackageInfoSchema(Schema):
    package_name = fields.Str()
    installed = fields.Boolean()
    install_environment = fields.Function(lambda obj: obj.install_environment.value)
    version = fields.Str(required=False, allow_none=True)


class GXExecutionEnvironment:
    """The list of GX dependencies with version and install information.

    This does not return any dependencies that are not specified directly in
    either requirements.txt or any requirements-dev*.txt files.

    Attributes: None
    """

    def __init__(self) -> None:
        self._ge_dependencies = GXDependencies()
        self._all_installed_packages: List[str] = []
        self._get_all_installed_packages()

        self._dependencies: List[PackageInfo] = []
        self._build_required_dependencies()
        self._build_dev_dependencies()

    @property
    def dependencies(self) -> List[PackageInfo]:
        """The list of GX dependencies with version and install information.

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
        if not self._all_installed_packages:
            # Only retrieve once
            package_names: list[str] = []
            for package in metadata.distributions():
                package_name = package.metadata.get("Name")
                if package_name:
                    package_names.append(package_name)
            package_names = [pn.lower() for pn in package_names]
            self._all_installed_packages = package_names

        return self._all_installed_packages

    def _build_dependencies_info(
        self,
        dependency_names: Iterable[str],
        install_environment: InstallEnvironment,
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
                # Chetan - 20221213 - Due to mypy inconsistencies, we need to follow this pattern to ensure
                # we end up with a version.Version obj. Casting the parse result sometimes leads to redundant
                # cast errors
                parsed_version = version.parse(metadata.version(dependency_name))
                if not isinstance(parsed_version, version.Version):
                    parsed_version = cast(version.Version, parsed_version)
                package_version = parsed_version
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
