"""Utilities to handle optional imports and related warnings e.g. sqlalchemy.

Great Expectations contains support for datasources and data stores that are
not included in the core package by default. Support requires install of
additional packages. To ensure these code paths are not executed when supporting
libraries are not installed, we check for existence of the associated library.

We also consolidate logic for warning based on version number in this module.
"""

from __future__ import annotations

from typing import Any, Literal, NoReturn

from packaging.version import Version

from great_expectations.compatibility.typing_extensions import override


class NotImported:
    def __init__(self, message: str) -> None:
        self.__dict__["gx_error_message"] = message

    def __getattr__(self, attr: str) -> NoReturn:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    @override
    def __setattr__(self, key: str, value: Any) -> NoReturn:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    def __call__(self, *args, **kwargs) -> NoReturn:
        raise ModuleNotFoundError(self.__dict__["gx_error_message"])

    @override
    def __str__(self) -> str:
        return self.__dict__["gx_error_message"]

    def __bool__(self) -> Literal[False]:
        return False


def is_version_greater_or_equal(version: str | Version, compare_version: str | Version) -> bool:
    """Check if the version is greater or equal to the compare_version.

    Args:
        version: Current version.
        compare_version: Version to compare to.

    Returns:
        Boolean indicating if the version is greater or equal to the compare version.
    """
    if isinstance(version, str):
        version = Version(version)
    if isinstance(compare_version, str):
        compare_version = Version(compare_version)

    return version >= compare_version


def is_version_less_than(version: str | Version, compare_version: str | Version) -> bool:
    """Check if the version is less than the compare_version.

    Args:
        version: Current version.
        compare_version: Version to compare to.

    Returns:
        Boolean indicating if the version is less than the compare version.
    """
    if isinstance(version, str):
        version = Version(version)
    if isinstance(compare_version, str):
        compare_version = Version(compare_version)

    return version < compare_version
