"""Track installed package dependencies.

This module contains utilities for tracking installed package dependencies to help enable the core team
to safely upgrade package dependencies to gain access to features of new package versions.
It contains a

    Typical usage example:
        ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()
        ge_execution_environment.installed_required_dependencies
        # dev and not-installed dependencies also available
    # TODO AJB 20220322: Fill me in,  incl running this file to update static lists
"""
import os
import re
from typing import List, Set


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
