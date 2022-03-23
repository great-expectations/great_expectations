"""Track installed package dependencies.

This module contains utilities for tracking installed package dependencies to help enable the core team
to safely upgrade package dependencies to gain access to features of new package versions.

    Typical usage example:
    # TODO AJB 20220322: Fill me in
"""
import enum
import os
import re
from dataclasses import dataclass
from importlib import metadata
from typing import List, Optional, Set, Tuple

from packaging import version


class GEDependencies:
    def __init__(self):
        self._requirements_relative_base_dir = "../../../"
        self._dev_requirements_prefix: str = "requirements-dev"

    def get_required_dependency_names(self):
        return sorted(
            self._get_dependency_names_from_requirements_file(
                self.required_requirements_path
            )
        )

    def get_dev_dependency_names(self) -> List[str]:
        """
        Get unique names of dependencies
        Returns:

        """
        dev_dependency_names: Set[str] = set()
        for dev_dependency_filename in self.dev_requirements_paths:
            dependency_names: List[
                str
            ] = self._get_dependency_names_from_requirements_file(
                os.path.join(
                    self._requirements_relative_base_dir, dev_dependency_filename
                )
            )
            dev_dependency_names.update(dependency_names)
        return sorted(list(dev_dependency_names))

    @property
    def required_requirements_path(self) -> str:
        return os.path.join(self._requirements_relative_base_dir, "requirements.txt")

    @property
    def dev_requirements_paths(self) -> List[str]:
        """
        Get all paths for requirements-dev files with dependencies in them
        Returns:

        """
        return [
            filename
            for filename in os.listdir(self._requirements_relative_base_dir)
            if filename.startswith(self._dev_requirements_prefix)
        ]

    @staticmethod
    def _get_dependency_names_from_requirements_file(filepath: str):
        with open(filepath) as f:
            dependencies_with_versions = f.read().splitlines()
        dependency_matches = [
            re.search(r"^(?!--requirement)([\w\-.]+)", s)
            for s in dependencies_with_versions
        ]
        dependency_names: List[str] = []
        for match in dependency_matches:
            if match is not None:
                dependency_names.append(match.group(0))
        return dependency_names


@dataclass
class PackageInfo:
    package_name: str
    installed: bool
    version: Optional[version.Version]


class ExecutionEnvironmentType(enum.Enum):
    ORCHESTRATION = "orchestration"  # Airflow, prefect, dagster, kedro, etc.
    INTERACTIVE = "interactive"  # Jupyter / Databricks / EMR notebooks
    HOSTED_ORCHESTRATION = "hosted_orchestration"  # Google Cloud Composer


@dataclass
class ExecutionEnvironment:
    environment_name: str
    version: Optional[version.Version]
    environment_type: ExecutionEnvironmentType


class GEExecutionEnvironment:
    """The list of installed GE dependencies with name/version along with the likely execution environment.

    Note we may not be able to uniquely determine the execution environment so we track all possibilities in a list.

    Attributes: None
    """

    def __init__(self):
        self._ge_dependencies: GEDependencies = GEDependencies()

        self._installed_required_dependencies = None
        self._not_installed_required_dependencies = None
        self.build_required_dependencies()

        self._installed_dev_dependencies = None
        self._not_installed_dev_dependencies = None
        self.build_dev_dependencies()

        # if execution_environments is None:
        #     self._execution_environments = self.build_execution_environments()
        # else:
        #     self._execution_environments = execution_environments

    def build_required_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependency_list(
            self._ge_dependencies.get_required_dependency_names()
        )
        (
            installed_dependencies,
            not_installed_dependencies,
        ) = self._split_dependencies_installed_vs_not(dependency_list)
        self._installed_required_dependencies = installed_dependencies
        self._not_installed_required_dependencies = not_installed_dependencies

    def build_dev_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependency_list(
            self._ge_dependencies.get_dev_dependency_names()
        )
        (
            installed_dependencies,
            not_installed_dependencies,
        ) = self._split_dependencies_installed_vs_not(dependency_list)
        self._installed_dev_dependencies = installed_dependencies
        self._not_installed_dev_dependencies = not_installed_dependencies

    @staticmethod
    def _split_dependencies_installed_vs_not(
        dependency_list: List[PackageInfo],
    ) -> Tuple[List[PackageInfo], List[PackageInfo]]:
        installed_dependencies: List[PackageInfo] = [
            d for d in dependency_list if d.installed
        ]
        not_installed_dependencies: List[PackageInfo] = [
            d for d in dependency_list if not d.installed
        ]
        return installed_dependencies, not_installed_dependencies

    def _build_dependency_list(self, dependency_names: List[str]) -> List[PackageInfo]:
        dependencies: List[PackageInfo] = []
        for dependency_name in dependency_names:
            try:
                package_version: version.Version = self._get_version_from_package_name(
                    dependency_name
                )
                dependencies.append(
                    PackageInfo(
                        package_name=dependency_name,
                        version=package_version,
                        installed=True,
                    )
                )
            except metadata.PackageNotFoundError:
                dependencies.append(
                    PackageInfo(
                        package_name=dependency_name, version=None, installed=False
                    )
                )
        return dependencies

    @staticmethod
    def _get_version_from_package_name(package_name: str) -> version.Version:
        """Get version information from package name.

        Args:
            package_name: str

        Returns:
            packaging.version.Version for the package

        Raises:
            importlib.metadata.PackageNotFoundError
        """
        package_version: version.Version = version.parse(metadata.version(package_name))
        return package_version

    # def build_execution_environments(self):
    #     raise NotImplementedError

    @property
    def installed_required_dependencies(self):
        return self._installed_required_dependencies

    @property
    def installed_dev_dependencies(self):
        return self._installed_dev_dependencies

    @property
    def not_installed_required_dependencies(self):
        return self._not_installed_required_dependencies

    @property
    def not_installed_dev_dependencies(self):
        return self._not_installed_dev_dependencies

    # @property
    # def execution_environments(self):
    #     return self._execution_environments
