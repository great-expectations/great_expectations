"""Track installed package dependencies.

This module contains utilities for tracking installed package dependencies to help enable the core team
to safely upgrade package dependencies to gain access to features of new package versions.

    Typical usage example:
    # TODO AJB 20220322: Fill me in
"""
import abc
import enum
import importlib
import os
import re
from dataclasses import dataclass
from types import ModuleType
from typing import List, Optional

from packaging import version
from packaging.version import Version


class GEDependencies:
    @staticmethod
    def get_required_dependency_names():
        # Parse requirements.txt
        requirements_txt_path: str = os.path.join("../../../", "requirements.txt")
        with open(requirements_txt_path) as f:
            required_with_versions = f.read().splitlines()
        required_dependency_names = [
            re.search(r"([\w\-.]*)", s).group(0) for s in required_with_versions
        ]
        return required_dependency_names


class PackageVersion(abc.ABC):

    package_name: str
    version: Version

    @abc.abstractmethod
    def get_version(self) -> Version:
        raise NotImplementedError


class PandasPackageVersion(PackageVersion):

    package_name = "pandas"

    def get_version(self) -> Version:
        try:
            import pandas

            return version.parse(pandas.__version__)
        except Exception:
            pass


def get_version_from_package_name(package_name: str) -> PackageVersion:
    package: ModuleType = importlib.import_module(package_name)
    print(package)


class ExecutionEnvironmentType(enum.Enum):
    ORCHESTRATION = "orchestration"  # Airflow, prefect, dagster, kedro, etc.
    INTERACTIVE = "interactive"  # Jupyter / Databricks / EMR notebooks
    HOSTED_ORCHESTRATION = "hosted_orchestration"  # Google Cloud Composer


@dataclass
class ExecutionEnvironment:
    environment_name: str
    version: Optional[Version]
    environment_type: ExecutionEnvironmentType


@dataclass
class GEExecutionEnvironment:
    """The list of installed GE dependencies with name/version along with the likely execution environment.

    Note we may not be able to uniquely determine the execution environment so we track all possibilities in a list.

    Attributes:
         installed_dependencies: The installed package dependencies in the environment where GE is executing.
         execution_environments: The likely execution environments GE is executing in.
    """

    installed_dependencies: List[PackageVersion]
    execution_environments: List[ExecutionEnvironment]


class GEExecutionEnvironmentBuilder:
    def build_environement(self):
        pass
