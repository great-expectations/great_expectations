"""Map-style validation result schema family.

Covers ColumnMapExpectation, ColumnPairMapExpectation, and
MulticolumnMapExpectation.  Membership is derived from the expectation class
hierarchy at dispatch time (see ``dispatcher.family_for``) rather than listed
here, so it cannot drift as expectations are added or re-parented.

Four format-discriminated classes share a common base:

    MapResultBase
    ├── MapBooleanOnlyResult  (BOOLEAN_ONLY)
    └── MapBasicResult        (BASIC)
        └── MapSummaryResult  (SUMMARY)
            └── MapCompleteResult  (COMPLETE)

This layer types the *shape* of a result dict and reports on it; it must never
change a value it is handed.  pydantic v1 coerces freely by default — ``int``
truncates 2.7 to 2, ``float`` widens 5 to 5.0, ``str`` turns 5 into "5" — so every
scalar field below is either ``Any`` (for genuinely heterogeneous values) or a
Strict* type that accepts the value unchanged or not at all.  A rejection surfaces
as a finding; a silent conversion would make the typed view disagree with
``ExpectationValidationResult.result`` with nothing to show for it.  Container
fields keep ``Any`` element types so their contents pass through untouched.

Import rules (enforced by ruff banned-api):
- Pydantic symbols come exclusively from ``great_expectations.compatibility.pydantic``.
- No PEP 604 unions (``X | Y``); use ``Optional[X]`` or ``Union[X, Y]``.
- No direct ``import pydantic``.
"""

from __future__ import annotations

from typing import Any, List, Optional

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import BaseModel
from great_expectations.core.validation_result_schemas.field_validators import (
    validate_partial_unexpected_counts_fallback,
    validate_unexpected_rows_passthrough,
)
from great_expectations.core.validation_result_schemas.types import StrictNumber


class MapResultBase(BaseModel):
    """Base for all map-style result models.

    Fields here are the always-allowed superset shared by every format variant.
    ``extra = Extra.forbid`` is intentional: the matrix runner *wants* unexpected
    fields to fail validation so they surface in findings as cleanup queue entries.
    """

    class Config:
        extra = pydantic.Extra.forbid
        arbitrary_types_allowed = True

    # Internal engine hint — declared as a normal field so it stays visible on
    # the model (and to any future validator bound here) even though
    # ``Field(..., exclude=True)`` drops it from ``.dict()``/``.json()`` output.
    # It is not something the engine emitted and must never appear in exported
    # output as though it were. It is only ever set from an explicit hint
    # supplied by the caller; None means "engine unknown".
    engine_hint: Optional[pydantic.StrictStr] = pydantic.Field(default=None, exclude=True)

    # Present on every engine, not just SQL: pandas emits unexpected_index_query
    # as a ``df.filter(items=[...], axis=0)`` expression. This layer types and
    # reports the field; it does not enforce that it be present under any
    # particular engine or result-format combination.
    unexpected_index_query: Optional[pydantic.StrictStr] = None
    unexpected_index_column_names: Optional[List[pydantic.StrictStr]] = None


class MapBooleanOnlyResult(MapResultBase):
    """ResultFormat.BOOLEAN_ONLY — empty result dict for map expectations.

    The parent EVR carries ``success``.  The result dict may carry only the
    index-query overflow fields when ``return_unexpected_index_query=True``.
    """

    pass  # No additional fields beyond the index-query fields in base


class MapBasicResult(MapResultBase):
    """ResultFormat.BASIC — counts, percents, and the partial unexpected list.

    Note: ``observed_value`` is included here because a small set of map
    expectations (e.g. ``expect_column_values_to_be_of_type``,
    ``expect_column_values_to_be_in_type_list``) emit it alongside the
    standard map fields on the pandas engine path.  It is Optional so that
    the majority of map expectations — which do *not* emit it — continue to
    validate cleanly.
    """

    element_count: Optional[pydantic.StrictInt] = None
    # The two counts are numbers, not integers, because that is what ships: MySQL returns
    # COUNT(*) results through the SQL execution engine as floats (2.0, not 2), while every other
    # engine returns ints. Strict typing keeps the value exactly as emitted either way, so the
    # findings' recorded runtime types are what expose the divergence; narrowing to int here
    # would make the schema describe what should be uniform rather than what is.
    unexpected_count: Optional[StrictNumber] = None
    unexpected_percent: Optional[StrictNumber] = None
    missing_count: Optional[StrictNumber] = None
    missing_percent: Optional[StrictNumber] = None
    unexpected_percent_total: Optional[StrictNumber] = None
    unexpected_percent_nonmissing: Optional[StrictNumber] = None
    partial_unexpected_list: Optional[List[Any]] = None
    # Some map expectations (e.g. expect_column_values_to_be_of_type on pandas)
    # emit observed_value alongside the standard map fields.  Typed Any for the
    # same reason as the aggregate family: the value is reported, never validated.
    observed_value: Any = None
    # engine-typed; classified at runtime, not validated by type
    unexpected_rows: Any = None

    _validate_rows = pydantic.validator("unexpected_rows", pre=True, allow_reuse=True)(
        validate_unexpected_rows_passthrough
    )


class MapSummaryResult(MapBasicResult):
    """ResultFormat.SUMMARY — adds counts and index list for partial unexpected."""

    partial_unexpected_counts: Optional[List[Any]] = None
    partial_unexpected_index_list: Optional[List[Any]] = None

    _validate_counts = pydantic.validator("partial_unexpected_counts", pre=True, allow_reuse=True)(
        validate_partial_unexpected_counts_fallback
    )


class MapCompleteResult(MapSummaryResult):
    """ResultFormat.COMPLETE — adds the full unexpected list and index list.

    This layer types and reports the shape of a result dict; it does not
    enforce cross-field invariants such as "an index query was requested, so
    one must be present". ``unexpected_index_query`` stays Optional here for
    the same reason every other field does: a caller that requested it and
    didn't get one is a finding to surface, not a construction error to raise.
    """

    unexpected_list: Optional[List[Any]] = None
    unexpected_index_list: Optional[List[Any]] = None
