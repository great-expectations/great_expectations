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
