"""Unit tests for the AggregateResult schema family.

Covers:
- Each format variant (AggregateBooleanOnlyResult, AggregateBasicResult,
  AggregateSummaryResult, AggregateCompleteResult) parses a valid result dict correctly.
- All expected fields match the input.
- Unknown extra fields raise pydantic.ValidationError (extra=forbid).
- observed_value round-trips every shape with its type and value unchanged.
- Scalar fields are strict: they accept a value as-is or reject it, never coerce.
- Details field is optional and accepts None or dict.
- Every format variant can be constructed with minimal (empty) args.

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_schemas_aggregate.py -m unit
"""

from __future__ import annotations

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core.validation_result_schemas.schemas.aggregate_result import (
    AggregateBasicResult,
    AggregateBooleanOnlyResult,
    AggregateCompleteResult,
    AggregateResultBase,
    AggregateSummaryResult,
)

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_BASIC_RESULT_DATA = {
    "element_count": 200,
    "missing_count": 10,
    "missing_percent": 5.0,
    "partial_unexpected_list": ["a", "b"],
    "partial_missing_list": [None],
}

_COMPLETE_EXTRA_DATA = {
    "unexpected_list": ["a", "b", "c"],
    "unexpected_index_list": [0, 1, 2],
}


# ---------------------------------------------------------------------------
# observed_value shapes
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_observed_value_int() -> None:
    """observed_value accepts int scalar."""
    m = AggregateResultBase(observed_value=42)
    assert m.observed_value == 42


@pytest.mark.unit
def test_observed_value_float() -> None:
    """observed_value accepts float scalar."""
    m = AggregateResultBase(observed_value=3.14)
    assert m.observed_value == 3.14


@pytest.mark.unit
def test_observed_value_str() -> None:
    """observed_value accepts string scalar."""
    m = AggregateResultBase(observed_value="mean=3.14")
    assert m.observed_value == "mean=3.14"


@pytest.mark.unit
def test_observed_value_bool() -> None:
    """observed_value accepts bool scalar."""
    m = AggregateResultBase(observed_value=True)
    assert m.observed_value is True


@pytest.mark.unit
def test_observed_value_list() -> None:
    """observed_value accepts list."""
    m = AggregateResultBase(observed_value=[1, 2, 3])
    assert m.observed_value == [1, 2, 3]


@pytest.mark.unit
def test_observed_value_dict() -> None:
    """observed_value accepts dict."""
    m = AggregateResultBase(observed_value={"min": 0, "max": 10})
    assert m.observed_value == {"min": 0, "max": 10}


@pytest.mark.unit
def test_observed_value_none() -> None:
    """observed_value defaults to None."""
    m = AggregateResultBase()
    assert m.observed_value is None


@pytest.mark.unit
def test_observed_value_explicit_none() -> None:
    """observed_value accepts explicit None."""
    m = AggregateResultBase(observed_value=None)
    assert m.observed_value is None


# ---------------------------------------------------------------------------
# observed_value is reported, never converted
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [
        1,
        0,
        2,
        2.5,
        True,
        False,
        "x",
        [1, 2, 3],
        {"min": 0, "max": 10},
        None,
    ],
    ids=[
        "int-1",
        "int-0",
        "int-2",
        "float-2.5",
        "bool-True",
        "bool-False",
        "str",
        "list",
        "dict",
        "none",
    ],
)
def test_observed_value_round_trips_with_identical_type_and_value(value: object) -> None:
    """The typed view must report exactly what the result dict held.

    An equality-only assertion cannot catch this: under a Union of concrete types
    pydantic v1 converts to whichever member matches first, so 1 comes back as
    True and 2 as 2.0 — both still ``==`` the input.  The type is the assertion.
    """
    parsed = AggregateResultBase.parse_obj({"observed_value": value}).observed_value
    assert type(parsed) is type(value)
    assert parsed == value or (parsed is None and value is None)


@pytest.mark.unit
def test_observed_value_preserves_a_numpy_scalar() -> None:
    """expect_column_mean_to_be_between observes a numpy float, not a Python one."""
    numpy = pytest.importorskip("numpy")
    value = numpy.float64(3.0)
    parsed = AggregateResultBase.parse_obj({"observed_value": value}).observed_value
    assert type(parsed) is numpy.float64
    assert parsed == value


# ---------------------------------------------------------------------------
# Scalar fields accept a value unchanged or reject it — they never convert
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_counts_reject_a_float_rather_than_truncating() -> None:
    """A non-integral count is a finding, not something to silently floor."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBasicResult.parse_obj({"element_count": 2.7})


@pytest.mark.unit
def test_counts_reject_a_bool() -> None:
    """bool is a subclass of int; accepting it would report True as 1."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBasicResult.parse_obj({"unexpected_count": True})


@pytest.mark.unit
def test_counts_reject_a_numeric_string() -> None:
    with pytest.raises(pydantic.ValidationError):
        AggregateBasicResult.parse_obj({"missing_count": "3"})


@pytest.mark.unit
@pytest.mark.parametrize(
    "value", [0, 5, 5.0, 2.5], ids=["int-0", "int-5", "float-5.0", "float-2.5"]
)
def test_percent_accepts_int_and_float_without_widening(value: object) -> None:
    parsed = AggregateBasicResult.parse_obj({"missing_percent": value}).missing_percent
    assert type(parsed) is type(value)
    assert parsed == value


# ---------------------------------------------------------------------------
# Details field
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_details_absent_defaults_to_none() -> None:
    """details field defaults to None when not provided."""
    m = AggregateResultBase()
    assert m.details is None


@pytest.mark.unit
def test_details_present_with_dict() -> None:
    """details field accepts a dict."""
    m = AggregateResultBase(details={"percentile": 0.95, "min": 0, "max": 100})
    assert m.details == {"percentile": 0.95, "min": 0, "max": 100}


@pytest.mark.unit
def test_details_present_empty_dict() -> None:
    """details field accepts an empty dict."""
    m = AggregateResultBase(details={})
    assert m.details == {}


@pytest.mark.unit
def test_details_explicit_none() -> None:
    """details field accepts explicit None."""
    m = AggregateResultBase(details=None)
    assert m.details is None


# ---------------------------------------------------------------------------
# AggregateResultBase extra=forbid
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_result_base_extra_forbid() -> None:
    """AggregateResultBase enforces extra=forbid."""
    with pytest.raises(pydantic.ValidationError):
        AggregateResultBase.parse_obj({"completely_unknown": "value"})


# ---------------------------------------------------------------------------
# AggregateBooleanOnlyResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_boolean_only_empty() -> None:
    """BOOLEAN_ONLY result is typically empty."""
    m = AggregateBooleanOnlyResult()
    assert m.observed_value is None
    assert m.details is None


@pytest.mark.unit
def test_aggregate_boolean_only_with_observed_value() -> None:
    """AggregateBooleanOnlyResult inherits observed_value from base."""
    m = AggregateBooleanOnlyResult(observed_value=42)
    assert m.observed_value == 42


@pytest.mark.unit
def test_aggregate_boolean_only_with_details() -> None:
    """AggregateBooleanOnlyResult inherits details from base."""
    m = AggregateBooleanOnlyResult(details={"info": "extra"})
    assert m.details == {"info": "extra"}


@pytest.mark.unit
def test_aggregate_boolean_only_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in AggregateBooleanOnlyResult."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBooleanOnlyResult.parse_obj({"unknown_field": "should_fail"})


@pytest.mark.unit
def test_aggregate_boolean_only_basic_fields_rejected() -> None:
    """AggregateBooleanOnlyResult does not accept AggregateBasicResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBooleanOnlyResult.parse_obj({"element_count": 100})


# ---------------------------------------------------------------------------
# AggregateBasicResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_basic_parses_valid_result() -> None:
    """AggregateBasicResult parses a typical BASIC result dict correctly."""
    m = AggregateBasicResult.parse_obj(_BASIC_RESULT_DATA)
    assert m.element_count == 200
    assert m.missing_count == 10
    assert m.missing_percent == 5.0
    assert m.partial_unexpected_list == ["a", "b"]
    assert m.partial_missing_list == [None]


@pytest.mark.unit
def test_aggregate_basic_all_fields_none() -> None:
    """All fields are Optional so AggregateBasicResult can be constructed with no args."""
    m = AggregateBasicResult()
    assert m.element_count is None
    assert m.missing_count is None
    assert m.missing_percent is None
    assert m.partial_unexpected_list is None
    assert m.partial_missing_list is None


@pytest.mark.unit
def test_aggregate_basic_with_observed_value() -> None:
    """AggregateBasicResult inherits observed_value from base."""
    m = AggregateBasicResult(observed_value=3.14, element_count=100)
    assert m.observed_value == 3.14
    assert m.element_count == 100


@pytest.mark.unit
def test_aggregate_basic_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in AggregateBasicResult."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBasicResult.parse_obj({**_BASIC_RESULT_DATA, "unknown_field": "bad"})


@pytest.mark.unit
def test_aggregate_basic_complete_only_field_raises() -> None:
    """AggregateBasicResult does not accept AggregateCompleteResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        AggregateBasicResult.parse_obj({**_BASIC_RESULT_DATA, "unexpected_list": [1, 2, 3]})


# ---------------------------------------------------------------------------
# AggregateSummaryResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_summary_parses_valid_result() -> None:
    """AggregateSummaryResult parses a typical SUMMARY result dict correctly."""
    m = AggregateSummaryResult.parse_obj(_BASIC_RESULT_DATA)
    assert m.element_count == 200
    assert m.missing_count == 10
    assert m.partial_unexpected_list == ["a", "b"]


@pytest.mark.unit
def test_aggregate_summary_all_optional() -> None:
    """All fields in AggregateSummaryResult are Optional."""
    m = AggregateSummaryResult()
    assert m.element_count is None
    assert m.missing_count is None
    assert m.partial_unexpected_list is None


@pytest.mark.unit
def test_aggregate_summary_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in AggregateSummaryResult."""
    with pytest.raises(pydantic.ValidationError):
        AggregateSummaryResult.parse_obj({**_BASIC_RESULT_DATA, "unknown_field": "bad"})


@pytest.mark.unit
def test_aggregate_summary_with_observed_value_and_details() -> None:
    """AggregateSummaryResult inherits base fields."""
    m = AggregateSummaryResult(
        observed_value={"mean": 42.0},
        details={"row_count": 1000},
        element_count=1000,
    )
    assert m.observed_value == {"mean": 42.0}
    assert m.details == {"row_count": 1000}
    assert m.element_count == 1000


@pytest.mark.unit
def test_aggregate_summary_complete_only_field_raises() -> None:
    """AggregateSummaryResult does not accept AggregateCompleteResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        AggregateSummaryResult.parse_obj({**_BASIC_RESULT_DATA, "unexpected_list": [1, 2, 3]})


# ---------------------------------------------------------------------------
# AggregateCompleteResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_complete_parses_valid_result() -> None:
    """AggregateCompleteResult parses a typical COMPLETE result dict correctly."""
    data = {**_BASIC_RESULT_DATA, **_COMPLETE_EXTRA_DATA}
    m = AggregateCompleteResult.parse_obj(data)
    assert m.element_count == 200
    assert m.missing_count == 10
    assert m.unexpected_list == ["a", "b", "c"]
    assert m.unexpected_index_list == [0, 1, 2]


@pytest.mark.unit
def test_aggregate_complete_all_optional() -> None:
    """All fields in AggregateCompleteResult are Optional."""
    m = AggregateCompleteResult()
    assert m.unexpected_list is None
    assert m.unexpected_index_list is None
    assert m.element_count is None


@pytest.mark.unit
def test_aggregate_complete_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in AggregateCompleteResult."""
    data = {**_BASIC_RESULT_DATA, **_COMPLETE_EXTRA_DATA}
    with pytest.raises(pydantic.ValidationError):
        AggregateCompleteResult.parse_obj({**data, "not_a_real_field": "value"})


@pytest.mark.unit
def test_aggregate_complete_inherits_all_ancestor_fields() -> None:
    """AggregateCompleteResult inherits fields from all ancestor classes."""
    data = {
        **_BASIC_RESULT_DATA,
        **_COMPLETE_EXTRA_DATA,
        "observed_value": 3.14,
        "details": {"info": "complete"},
    }
    m = AggregateCompleteResult.parse_obj(data)
    # From AggregateResultBase
    assert m.observed_value == 3.14
    assert m.details == {"info": "complete"}
    # From AggregateBasicResult
    assert m.element_count == 200
    assert m.partial_unexpected_list == ["a", "b"]
    assert m.partial_missing_list == [None]
    # From AggregateCompleteResult
    assert m.unexpected_list == ["a", "b", "c"]
    assert m.unexpected_index_list == [0, 1, 2]


@pytest.mark.unit
def test_aggregate_complete_with_list_observed_value() -> None:
    """AggregateCompleteResult accepts list observed_value."""
    m = AggregateCompleteResult(observed_value=["a", "b", "c"])
    assert m.observed_value == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Inheritance chain sanity
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_inheritance_chain() -> None:
    """Inheritance chain: Complete → Summary → Basic → ResultBase."""
    assert issubclass(AggregateCompleteResult, AggregateSummaryResult)
    assert issubclass(AggregateSummaryResult, AggregateBasicResult)
    assert issubclass(AggregateBasicResult, AggregateResultBase)
    assert issubclass(AggregateBooleanOnlyResult, AggregateResultBase)


@pytest.mark.unit
def test_aggregate_complete_is_not_aggregate_boolean_only() -> None:
    """AggregateCompleteResult and AggregateBooleanOnlyResult are separate leaf classes."""
    assert not issubclass(AggregateCompleteResult, AggregateBooleanOnlyResult)
    assert not issubclass(AggregateBooleanOnlyResult, AggregateCompleteResult)


# ---------------------------------------------------------------------------
# extra=forbid on AggregateResultBase
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_aggregate_summary_is_subclass_of_basic() -> None:
    """AggregateSummaryResult is a subclass of AggregateBasicResult (no new fields)."""
    assert issubclass(AggregateSummaryResult, AggregateBasicResult)
    # verify they have the same fields (summary adds no new fields)
    assert set(AggregateSummaryResult.__fields__.keys()) == set(
        AggregateBasicResult.__fields__.keys()
    )
