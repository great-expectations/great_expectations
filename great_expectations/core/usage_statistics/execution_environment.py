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
    """The list of installed GE dependencies with name/version along with the likely execution environment.

    Note we may not be able to uniquely determine the execution environment so we track all possibilities in a list.

    Attributes: None
    """

    def __init__(self):
        self._ge_dependencies: GEDependencies = GEDependencies()
        self._all_installed_packages = None
        self.get_all_installed_packages()

        self._dependencies = []
        self._build_required_dependencies()
        self._build_dev_dependencies()

    def get_all_installed_packages(self) -> List[str]:
        if self._all_installed_packages is None:
            # Only retrieve once
            self._all_installed_packages = [
                item.metadata.get("Name") for item in metadata.distributions()
            ]
        return self._all_installed_packages

    def _build_required_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependency_list(
            self._ge_dependencies.get_required_dependency_names(),
            install_environment=InstallEnvironment.REQUIRED,
        )
        self._dependencies.extend(dependency_list)

    def _build_dev_dependencies(self) -> None:
        dependency_list: List[PackageInfo] = self._build_dependency_list(
            self._ge_dependencies.get_dev_dependency_names(),
            install_environment=InstallEnvironment.DEV,
        )
        self._dependencies.extend(dependency_list)

    @property
    def dependencies(self) -> List[PackageInfo]:
        return self._dependencies

    def _build_dependency_list(
        self, dependency_names: List[str], install_environment: InstallEnvironment
    ) -> List[PackageInfo]:
        dependencies: List[PackageInfo] = []
        for dependency_name in dependency_names:

            package_version: Optional[version.Version]
            installed: bool
            if dependency_name in self.get_all_installed_packages():
                installed = True
                package_version = self._get_version_from_package_name(dependency_name)
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
