"""Per-expectation schema overrides.

Some expectations emit result dicts that do not fit the generic map or
aggregate families.  Each override here is a standalone Pydantic model with
``extra = Extra.forbid`` so unexpected fields surface as validation errors.

Import rules (enforced by ruff banned-api):
- Pydantic symbols come exclusively from ``great_expectations.compatibility.pydantic``.
- No PEP 604 unions (``X | Y``); use ``Optional[X]`` or ``Union[X, Y]``.
- No direct ``import pydantic``.
"""

from __future__ import annotations

from typing import Any

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import BaseModel


class TypeExpectationObservedValueResult(BaseModel):
    """Result shape for expectations that report a bare observed type value.

    ``expect_column_values_to_be_of_type`` and
    ``expect_column_values_to_be_in_type_list`` both bypass ``_format_map_output``
    on every engine -- pandas, SQL, and Spark alike -- whenever they take their
    non-map validation path (pandas only takes the map path on an object-dtype
    column being compared against a non-object expected type; every other case,
    on every engine, takes this one).  For BASIC / SUMMARY / COMPLETE formats the
    result dict contains only ``{observed_value: <type-name>}``.  For BOOLEAN_ONLY
    format the result dict is empty ``{}``, so ``observed_value`` defaults to None
    to allow both cases through the same override schema.  The dispatcher selects
    this schema by the shape of the result dict, not by which engine or
    expectation produced it, and the name says so.

    ``observed_value`` is typed ``Any``, not ``str``: on case-insensitive SQL
    dialects ``compare_column_type`` hands back the column type object itself
    rather than its name, and this layer reports values without altering them —
    a ``str`` annotation would silently stringify whatever it was given.  The
    shape is still pinned, because ``extra = Extra.forbid`` rejects any other key.
    """

    class Config:
        extra = pydantic.Extra.forbid

    observed_value: Any = None
