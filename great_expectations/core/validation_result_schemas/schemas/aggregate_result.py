"""Aggregate-style validation result schema family.

Covers AggregateExpectation types (column-level aggregate expectations such as
expect_column_mean_to_be_between, expect_column_min_to_be_between, etc.).

Four format-discriminated classes share a common base:

    AggregateResultBase
    ├── AggregateBooleanOnlyResult  (BOOLEAN_ONLY)
    └── AggregateBasicResult        (BASIC)
        └── AggregateSummaryResult  (SUMMARY)
            └── AggregateCompleteResult  (COMPLETE)

This layer types the *shape* of a result dict and reports on it; it must never
change a value it is handed.  pydantic v1 coerces freely by default — ``int``
truncates 2.7 to 2, ``float`` turns 5 into 5.0, ``str`` turns 5 into "5", and a
multi-member Union converts to whichever member matches first — so every scalar
field below is either ``Any`` (for genuinely heterogeneous values) or a Strict*
type that accepts the value unchanged or not at all.  A rejection surfaces as a
finding; a silent conversion would make the typed view disagree with
``ExpectationValidationResult.result`` with nothing to show for it.

Container fields keep ``Any`` element types, so their contents pass through
untouched.  ``details`` is keyed ``str`` because result-dict keys are strings by
construction.

Import rules (enforced by ruff banned-api):
- Pydantic symbols come exclusively from ``great_expectations.compatibility.pydantic``.
- No PEP 604 unions (``X | Y``); use ``Optional[X]`` or ``Union[X, Y]``.
- No direct ``import pydantic``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import BaseModel
from great_expectations.core.validation_result_schemas.types import StrictNumber

# observed_value carries whatever the expectation computed: a Python scalar, a
# numpy scalar (expect_column_mean_to_be_between returns numpy.float64), a list of
# distinct values, a dict of quantiles.  It is typed Any deliberately — the value
# is reported, never validated.  A Union of concrete types cannot express that:
# under pydantic v1 the members absorb each other left to right, so 1 arrives as
# True under a leading ``bool`` and 2 arrives as 2.0 under a leading ``float``.
ObservedValue = Any


class AggregateResultBase(BaseModel):
    """Base for all aggregate-style result models.

    Fields here are the always-allowed superset shared by every format variant.
    ``extra = Extra.forbid`` is intentional: the matrix runner *wants* unexpected
    fields to fail validation so they surface in findings as cleanup queue entries.
    """

    class Config:
        extra = pydantic.Extra.forbid
        arbitrary_types_allowed = True

    observed_value: ObservedValue = None
    details: Optional[Dict[str, Any]] = None


class AggregateBooleanOnlyResult(AggregateResultBase):
    """ResultFormat.BOOLEAN_ONLY — typically empty result dict for aggregate expectations.

    The parent EVR carries ``success``.  The result dict for BOOLEAN_ONLY
    aggregate expectations typically has no additional fields.
    """

    pass  # BOOLEAN_ONLY: typically empty


class AggregateBasicResult(AggregateResultBase):
    """ResultFormat.BASIC — counts, percents, and partial lists.

    Note: ``unexpected_count`` and ``unexpected_percent`` are included here
    because a subset of aggregate expectations emit them alongside the
    standard aggregate fields (``expect_column_distinct_values_to_equal_set``
    emits the count; ``expect_query_results_to_match_comparison`` emits both).
    They are Optional so that the majority of aggregate expectations — which
    do *not* emit them — continue to validate cleanly.
    """

    element_count: Optional[pydantic.StrictInt] = None
    missing_count: Optional[StrictNumber] = None
    missing_percent: Optional[StrictNumber] = None
    unexpected_count: Optional[StrictNumber] = None
    unexpected_percent: Optional[StrictNumber] = None
    partial_unexpected_list: Optional[List[Any]] = None
    partial_missing_list: Optional[List[Any]] = None


class AggregateSummaryResult(AggregateBasicResult):
    """ResultFormat.SUMMARY — aggregate expectations rarely diverge from BASIC.

    Kept explicit so the dispatcher can name it distinctly.
    """

    pass  # Aggregate expectations rarely diverge between BASIC and SUMMARY


class AggregateCompleteResult(AggregateSummaryResult):
    """ResultFormat.COMPLETE — adds the full unexpected list and index list."""

    unexpected_list: Optional[List[Any]] = None
    unexpected_index_list: Optional[List[Any]] = None
