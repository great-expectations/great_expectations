"""Utilities to handle optional imports and related warnings e.g. sqlalchemy.

Great Expectations contains support for datasources and data stores that are
not included in the core package by default. Support requires install of
additional packages. To ensure these code paths are not executed when supporting
libraries are not installed, we check for existence of the associated library.

We also consolidate logic for warning based on version number in this module.
"""
from __future__ import annotations

import warnings

from packaging.version import Version

try:
    import sqlalchemy as sa
except ImportError:
    sa = None


def sqlalchemy_version_check(version: str | Version) -> None:
    """Check if the sqlalchemy version is supported or warn if not.

    Args:
        version: sqlalchemy version as a string or Version.
    """
    if isinstance(version, str):
        version = Version(version)

    if version >= Version("2.0.0"):
        warnings.warn(
            "SQLAlchemy v2.0.0 or later is not yet supported by Great Expectations.",
            UserWarning,
        )
