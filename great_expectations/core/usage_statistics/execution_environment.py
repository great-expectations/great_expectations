"Identify installed package dependencies.\n\nThis module contains utilities for identifying installed package dependencies\nto help enable the core team to safely upgrade package dependencies to gain\naccess to features of new package versions.\n\n    Typical usage example:\n        ge_execution_environment: GEExecutionEnvironment = GEExecutionEnvironment()\n        dependencies: List[PackageInfo] = ge_execution_environment.dependencies\n\n"
import enum
import sys
from dataclasses import dataclass
from typing import List, Optional

from packaging import version

from great_expectations.core.usage_statistics.package_dependencies import GEDependencies
from great_expectations.marshmallow__shade import Schema, fields

if sys.version_info < (3, 8):
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


class GEExecutionEnvironment:
    "The list of GE dependencies with version and install information.\n\n    This does not return any dependencies that are not specified directly in\n    either requirements.txt or any requirements-dev*.txt files.\n\n    Attributes: None\n"

    def __init__(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self._ge_dependencies: GEDependencies = GEDependencies()
        self._all_installed_packages = None
        self._get_all_installed_packages()
        self._dependencies = []
        self._build_required_dependencies()
        self._build_dev_dependencies()

    @property
    def dependencies(self) -> List[PackageInfo]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "The list of GE dependencies with version and install information.\n\n        This does not return any dependencies that are not specified directly in\n        either requirements.txt or any requirements-dev*.txt files.\n\n        Attributes: None\n        "
        return self._dependencies

    def _build_required_dependencies(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        dependency_list: List[PackageInfo] = self._build_dependencies_info(
            self._ge_dependencies.get_required_dependency_names(),
            install_environment=InstallEnvironment.REQUIRED,
        )
        self._dependencies.extend(dependency_list)

    def _build_dev_dependencies(self) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        dependency_list: List[PackageInfo] = self._build_dependencies_info(
            self._ge_dependencies.get_dev_dependency_names(),
            install_environment=InstallEnvironment.DEV,
        )
        self._dependencies.extend(dependency_list)

    def _get_all_installed_packages(self) -> List[str]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Get all installed packages. Used to check if dependency is installed or not.\n\n        Returns:\n            list of string names of packages that are installed\n        "
        if self._all_installed_packages is None:
            self._all_installed_packages = [
                item.metadata.get("Name")
                for item in metadata.distributions()
                if (item.metadata.get("Name") is not None)
            ]
        return self._all_installed_packages

    def _build_dependencies_info(
        self, dependency_names: List[str], install_environment: InstallEnvironment
    ) -> List[PackageInfo]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "Build list of info about dependencies including version and install status.\n\n        Args:\n            dependency_names: Retrieve info on these dependencies.\n            install_environment: Environment these dependencies are installed in.\n\n        Returns:\n            List of PackageInfo objects for each dependency including version,\n            install status, install environment.\n        "
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
