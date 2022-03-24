"""Track installed package dependencies.

This module contains utilities for tracking installed package dependencies to help enable the core team
to safely upgrade package dependencies to gain access to features of new package versions.

    Typical usage example:
        ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()
        ge_execution_environment.installed_required_dependencies
        # dev and not-installed dependencies also available
    # TODO AJB 20220322: Fill me in,  incl running this file to update static lists
"""
import os
import re
from dataclasses import dataclass
from importlib import metadata
from typing import List, Optional, Set, Tuple

from packaging import version


class GEDependencies:

    """This list should be kept in sync with our requirements.txt"""

    GE_REQUIRED_DEPENDENCIES: List[str] = sorted(
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
    GE_DEV_DEPENDENCIES: List[str] = sorted(
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

    def __init__(self):
        self._requirements_relative_base_dir = "../../../"
        self._dev_requirements_prefix: str = "requirements-dev"

    def get_required_dependency_names(self) -> List[str]:
        return self.GE_REQUIRED_DEPENDENCIES

    def get_dev_dependency_names(self) -> List[str]:
        return self.GE_DEV_DEPENDENCIES

    def get_required_dependency_names_from_requirements_file(self):
        """Get unique names of required dependencies

        Returns:

        """
        return sorted(
            list(
                set(
                    self._get_dependency_names_from_requirements_file(
                        self.required_requirements_path
                    )
                )
            )
        )

    def get_dev_dependency_names_from_requirements_file(self) -> List[str]:
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

    def _get_dependency_names_from_requirements_file(self, filepath: str):
        with open(filepath) as f:
            dependencies_with_versions = f.read().splitlines()
            return self._get_dependency_names(dependencies_with_versions)

    def _get_dependency_names(self, dependencies: List[str]) -> List[str]:
        dependency_matches = [
            re.search(r"^(?!--requirement)([\w\-.]+)", s) for s in dependencies
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


class GEExecutionEnvironment:
    """The list of installed GE dependencies with name/version along with the likely execution environment.

    Note we may not be able to uniquely determine the execution environment so we track all possibilities in a list.

    Attributes: None
    """

    def __init__(self):
        self._ge_dependencies: GEDependencies = GEDependencies()
        self._all_installed_packages = None
        self.get_all_installed_packages()

        self._installed_required_dependencies = None
        self._not_installed_required_dependencies = None
        self.build_required_dependencies()

        self._installed_dev_dependencies = None
        self._not_installed_dev_dependencies = None
        self.build_dev_dependencies()

    def get_all_installed_packages(self) -> List[str]:
        if self._all_installed_packages is None:
            # Only retrieve once
            self._all_installed_packages = [
                item.metadata.get("Name") for item in metadata.distributions()
            ]
        return self._all_installed_packages

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

            if dependency_name in self.get_all_installed_packages():
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
            else:
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


if __name__ == "__main__":
    """Run this module to generate a list of packages from requirements files to update our static lists"""
    ge_dependencies: GEDependencies = GEDependencies()
    print("\n\nRequired Dependencies:\n\n")
    print(ge_dependencies.get_required_dependency_names_from_requirements_file())
    print("\n\nDev Dependencies:\n\n")
    print(ge_dependencies.get_dev_dependency_names_from_requirements_file())
    assert (
        ge_dependencies.get_required_dependency_names()
        == ge_dependencies.get_required_dependency_names_from_requirements_file()
    ), "Mismatch between required dependencies in requirements files and in GEDependencies"
    assert (
        ge_dependencies.get_dev_dependency_names()
        == ge_dependencies.get_dev_dependency_names_from_requirements_file()
    ), "Mismatch between dev dependencies in requirements files and in GEDependencies"
    print(
        "Required and Dev dependencies in requirements files match those in GEDependencies"
    )
