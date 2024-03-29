from __future__ import annotations

"""Warnings (including deprecation).

This module consolidates warnings throughout our codebase into a single place
so that changes e.g. to verbiage are restricted to this file.
"""
import warnings


def warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0() -> None:
    """Warning to emit when using pandas less than v1.4.0 with sqlalchemy greater than or equal to 2.0.0."""  # noqa: E501
    warnings.warn(
        """Please be aware that pandas versions below 2.0.0 may have issues when paired with SQLAlchemy 2.0.0 and above when using pandas + sql functionality (like the pandas read_sql reader method).""",  # noqa: E501
        UserWarning,
    )
