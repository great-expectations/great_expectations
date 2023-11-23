"""Warnings (including deprecation).

This module consolidates warnings throughout our codebase into a single place
so that changes e.g. to verbiage are restricted to this file.
"""
import warnings


def warn_deprecated_parse_strings_as_datetimes() -> None:
    """Warn when using parse_strings_as_datetimes in expectations."""
    # deprecated-v0.13.41
    warnings.warn(
        """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why-we-dont-do-transformations-for-expectations-and-when-we-do
""",
        DeprecationWarning,
    )


def warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0() -> None:
    """Warning to emit when using pandas less than v1.4.0 with sqlalchemy greater than or equal to 2.0.0."""
    warnings.warn(
        """Please be aware that pandas versions below 2.0.0 may have issues when paired with SQLAlchemy 2.0.0 and above when using pandas + sql functionality (like the pandas read_sql reader method).""",
        UserWarning,
    )
