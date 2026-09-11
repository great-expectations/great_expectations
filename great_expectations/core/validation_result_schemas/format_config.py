"""ResultFormatConfig TypedDict describing the output of ``parse_result_format``.

``parse_result_format`` normalises the several shapes a ``result_format`` argument
may take into one dict.  This TypedDict is the shape of that dict, and the
dispatcher reads it when it has to recover a ResultFormat from a configured value.

These types are not part of the public API and must not be exported via
great_expectations/__init__.py or decorated with @public_api.

This module is deliberately stdlib/typing-only.  It carries no pydantic, no
schema, and no dependency back onto the rest of the package, so annotating a
function elsewhere in great_expectations with these types can never introduce an
import cycle.
"""

from __future__ import annotations

from typing import List, TypedDict


class ResultFormatConfigRequired(TypedDict):
    """Keys ``parse_result_format`` always injects, whatever it is handed."""

    partial_unexpected_count: int
    include_unexpected_rows: bool
    map_expectation_unexpected_rows_as_dict: bool


class ResultFormatConfig(ResultFormatConfigRequired, total=False):
    """Full result-format config dict including optional keys.

    ``result_format`` is optional, not required.  ``parse_result_format`` injects
    the three keys above into any dict it is given, but it only sets
    ``result_format`` itself when the caller passed a string; a caller that passes
    a dict without that key (``parse_result_format({})`` is a live call site) gets
    a config back that never names a format.  Declaring it required would let a
    reader assume ``config["result_format"]`` is always safe.

    The two-class overlay pattern (required base + total=False subclass) lets us
    express "required + optional" without NotRequired[...], which requires
    Python 3.11+.  This keeps the code parseable on Python 3.10.
    """

    result_format: str  # one of the 4 ResultFormat enum values
    exclude_unexpected_values: bool
    return_unexpected_index_query: bool
    unexpected_index_column_names: List[str]
