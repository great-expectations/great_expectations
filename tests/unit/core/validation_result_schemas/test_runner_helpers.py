"""Unit coverage for the matrix runner's helpers.

The interesting one is ``assert_field_set_covered``. It used to compare key *sets* between the raw
result dict and the parsed model -- which asserted nothing at all, because every schema in this
package sets ``extra = Extra.forbid``: a raw key the model does not declare has already failed
parsing before the comparison runs, and a key it does declare is in ``.dict()`` whether or not the
value survived. The tests below therefore include mutation checks: a model that silently coerces a
value passes a key-set comparison and must fail this one.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.compatibility import pydantic
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.dispatcher import ParseError, as_typed
from great_expectations.core.validation_result_schemas.types import RuntimeTypeName
from tests.integration.data_sources_and_expectations.expectations import (
    _validation_result_schemas_helpers as _helpers,
)
from tests.integration.data_sources_and_expectations.expectations import (
    test_validation_result_schemas_matrix as _matrix,
)
from tests.integration.data_sources_and_expectations.expectations._validation_result_schemas_cases import (  # noqa: E501
    SELF_DATA_SOURCE_SENTINEL,
    SELF_TABLE_SENTINEL,
)

assert_field_set_covered = _helpers.assert_field_set_covered
resolve_self_references = _helpers.resolve_self_references
summarize_raised_exception = _helpers.summarize_raised_exception
summarize_raw_dict = _helpers.summarize_raw_dict
extra_fields_rejected = _matrix._extra_fields_rejected

# ---------------------------------------------------------------------------
# Models for exercising assert_field_set_covered
# ---------------------------------------------------------------------------


class _PassthroughModel(pydantic.BaseModel):
    """A model that reports values without altering them, plus an extra field the raw dict
    never carries. Every scalar is Strict* or Any, mirroring the real schemas."""

    success: Optional[pydantic.StrictBool] = None
    element_count: Optional[pydantic.StrictInt] = None
    unexpected_percent: Optional[pydantic.StrictFloat] = None
    observed_value: Any = None
    partial_unexpected_list: Optional[List[Any]] = None
    result: Optional[Dict[str, Any]] = None
    unexpected_rows: Any = None
    engine_hint: Optional[str] = None


class _CoercingModel(pydantic.BaseModel):
    """The mutation: the same field declared non-strict, so pydantic widens an int to a float.

    A key-set comparison cannot see this happen. That is the whole reason
    ``assert_field_set_covered`` compares values and types.
    """

    unexpected_percent: Optional[float] = None


class _StringifyingModel(pydantic.BaseModel):
    """A second mutation: a field declared ``str``, which stringifies whatever it is given."""

    observed_value: Optional[str] = None


# ---------------------------------------------------------------------------
# assert_field_set_covered — passing cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_field_set_covered_when_every_value_survives_unchanged() -> None:
    # Annotated rather than inferred: mypy narrows a heterogeneous dict literal to its join
    # (here `object`), which then refuses to splat into any typed field.
    raw: Dict[str, Any] = {
        "success": True,
        "element_count": 6,
        "unexpected_percent": 33.5,
        "observed_value": 6,
        "partial_unexpected_list": ["a", "b"],
        "result": {"nested": 1},
    }
    assert_field_set_covered(raw, _PassthroughModel(**raw))


@pytest.mark.unit
def test_field_set_covered_ignores_model_fields_the_raw_dict_lacks() -> None:
    """The dispatcher injects ``engine_hint`` into map models, and a wider format variant declares
    fields a narrower result never carries. Neither is information loss."""
    raw = {"success": True}
    model = _PassthroughModel(success=True, engine_hint="pandas")
    assert_field_set_covered(raw, model)


@pytest.mark.unit
def test_field_set_covered_accepts_nan_reproduced_as_nan() -> None:
    """``NaN != NaN`` by IEEE rule, so a NaN that survived untouched must not read as changed."""
    raw: Dict[str, Any] = {"unexpected_percent": float("nan")}
    assert_field_set_covered(raw, _PassthroughModel(**raw))


@pytest.mark.unit
def test_field_set_covered_passes_engine_native_value_through_by_identity() -> None:
    """``unexpected_rows`` holds an engine-native frame. Two frames with identical data do not
    compare equal, so the exemption asserts the model handed back the very object it was given."""
    frame = pd.DataFrame({"a": [1, 2]})
    raw = {"unexpected_rows": frame}
    model = _PassthroughModel(unexpected_rows=frame)
    assert_field_set_covered(raw, model)
    assert model.unexpected_rows is frame


# ---------------------------------------------------------------------------
# assert_field_set_covered — mutations that must fail
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_field_set_covered_rejects_a_widened_int() -> None:
    """The planted coercion: a non-strict ``float`` field turns the integer 5 into 5.0. The key
    sets are identical and the values compare equal, so only the type check catches it."""
    raw: Dict[str, Any] = {"unexpected_percent": 5}
    model = _CoercingModel(**raw)
    assert model.unexpected_percent == 5  # equal, and still wrong
    with pytest.raises(AssertionError, match=r"unexpected_percent: type changed from int to float"):
        assert_field_set_covered(raw, model)


@pytest.mark.unit
def test_field_set_covered_rejects_a_stringified_value() -> None:
    raw: Dict[str, Any] = {"observed_value": 6}
    with pytest.raises(AssertionError, match=r"observed_value: type changed from int to str"):
        assert_field_set_covered(raw, _StringifyingModel(**raw))


@pytest.mark.unit
def test_field_set_covered_rejects_a_changed_value() -> None:
    """Same type, different value: caught by the equality half of the check."""

    class _RewritingModel(pydantic.BaseModel):
        element_count: Optional[pydantic.StrictInt] = None

    raw = {"element_count": 6}
    model = _RewritingModel(element_count=7)
    with pytest.raises(AssertionError, match=r"element_count: value changed from 6 to 7"):
        assert_field_set_covered(raw, model)


@pytest.mark.unit
def test_field_set_covered_rejects_a_replaced_engine_native_value() -> None:
    """The exemption is not a free pass: a frame the model replaced with a different object still
    fails, because the check is identity rather than "anything goes"."""
    raw = {"unexpected_rows": pd.DataFrame({"a": [1, 2]})}
    model = _PassthroughModel(unexpected_rows=pd.DataFrame({"a": [1, 2]}))
    with pytest.raises(AssertionError, match="engine-native value was replaced"):
        assert_field_set_covered(raw, model)


@pytest.mark.unit
def test_field_set_covered_reports_every_offending_key_together() -> None:
    class _DoublyWrongModel(pydantic.BaseModel):
        element_count: Optional[float] = None
        unexpected_percent: Optional[float] = None

    raw: Dict[str, Any] = {"element_count": 6, "unexpected_percent": 5}
    with pytest.raises(AssertionError) as exc_info:
        assert_field_set_covered(raw, _DoublyWrongModel(**raw))
    message = str(exc_info.value)
    assert "element_count" in message
    assert "unexpected_percent" in message


@pytest.mark.unit
def test_field_set_covered_rejects_a_raw_key_the_model_never_declared() -> None:
    """The original contract, kept: a raw key absent from the model is information loss. It is
    unreachable through the shipped schemas (``Extra.forbid`` rejects such a dict before this
    function sees it), which is exactly why it needs a direct test."""

    class _NarrowModel(pydantic.BaseModel):
        success: Optional[pydantic.StrictBool] = None

    raw = {"success": True, "missing_field": "some_value"}
    with pytest.raises(AssertionError, match="missing_field"):
        assert_field_set_covered(raw, _NarrowModel(success=True))


# ---------------------------------------------------------------------------
# summarize_raw_dict
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_summarize_raw_dict_empty() -> None:
    """Empty dict returns empty raw_field_set and raw_field_types."""
    result = summarize_raw_dict({})
    assert result == {"raw_field_set": [], "raw_field_types": {}}


@pytest.mark.unit
def test_summarize_raw_dict_scalar_values() -> None:
    """Scalar values are classified to the correct RuntimeTypeName."""
    raw = {
        "an_int": 42,
        "a_float": 3.14,
        "a_str": "hello",
        "a_bool": True,
    }
    result = summarize_raw_dict(raw)
    assert result["raw_field_set"] == sorted(raw.keys())
    assert result["raw_field_types"]["an_int"] == RuntimeTypeName.INT.value
    assert result["raw_field_types"]["a_float"] == RuntimeTypeName.FLOAT.value
    assert result["raw_field_types"]["a_str"] == RuntimeTypeName.STR.value
    assert result["raw_field_types"]["a_bool"] == RuntimeTypeName.BOOL.value


@pytest.mark.unit
def test_summarize_raw_dict_collection_values() -> None:
    """list and dict values are classified correctly."""
    raw = {
        "a_list": [1, 2, 3],
        "a_dict": {"nested": True},
    }
    result = summarize_raw_dict(raw)
    assert result["raw_field_types"]["a_list"] == RuntimeTypeName.LIST.value
    assert result["raw_field_types"]["a_dict"] == RuntimeTypeName.DICT.value


@pytest.mark.unit
def test_summarize_raw_dict_none_values() -> None:
    """None values are classified as RuntimeTypeName.NONE."""
    raw = {"nullable_field": None}
    result = summarize_raw_dict(raw)
    assert result["raw_field_types"]["nullable_field"] == RuntimeTypeName.NONE.value


@pytest.mark.unit
def test_summarize_raw_dict_field_set_is_sorted() -> None:
    """raw_field_set must be in sorted order regardless of insertion order."""
    raw = {"z_last": 1, "a_first": 2, "m_middle": 3}
    result = summarize_raw_dict(raw)
    assert result["raw_field_set"] == ["a_first", "m_middle", "z_last"]


@pytest.mark.unit
def test_summarize_raw_dict_never_includes_values() -> None:
    """The result dict must not contain raw field values — only structure."""
    raw = {"secret_value": "do_not_leak_this"}
    result = summarize_raw_dict(raw)
    # Values should not appear anywhere in the output
    assert "do_not_leak_this" not in str(result)
    # But the key (structure) should be present
    assert "secret_value" in result["raw_field_set"]


# ---------------------------------------------------------------------------
# summarize_raised_exception
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_summarize_raised_exception_flat_record() -> None:
    """The shape ``batch.validate`` returns when the expectation itself recorded the failure."""
    info = {
        "raised_exception": True,
        "exception_message": "Cannot compare Timestamp with datetime.date",
        "exception_traceback": "…",
    }
    assert summarize_raised_exception(info) == "Cannot compare Timestamp with datetime.date"


@pytest.mark.unit
def test_summarize_raised_exception_keyed_by_metric() -> None:
    """The other shape: one record per metric identifier. A runner that understood only the flat
    shape would read this as 'nothing was raised' and file the empty result dict as coverage."""
    info = {
        "metric-id-1": {"raised_exception": False, "exception_message": None},
        "metric-id-2": {"raised_exception": True, "exception_message": "no such column"},
    }
    assert summarize_raised_exception(info) == "no such column"


@pytest.mark.unit
def test_summarize_raised_exception_returns_none_when_nothing_raised() -> None:
    assert (
        summarize_raised_exception(
            {"raised_exception": False, "exception_message": None, "exception_traceback": None}
        )
        is None
    )
    assert summarize_raised_exception({}) is None
    assert summarize_raised_exception(None) is None


@pytest.mark.unit
def test_summarize_raised_exception_falls_back_when_no_message_recorded() -> None:
    assert summarize_raised_exception({"raised_exception": True}) == (
        "exception recorded with no message"
    )


# ---------------------------------------------------------------------------
# resolve_self_references
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_resolve_self_references_substitutes_the_table_sentinel() -> None:
    expectation = gxe.ExpectTableRowCountToEqualOtherTable(other_table_name=SELF_TABLE_SENTINEL)
    resolved = resolve_self_references(
        expectation, table_name="tbl_abc123", data_source_name="ds_abc123"
    )
    assert resolved.other_table_name == "tbl_abc123"
    # The shared, module-level case instance is reused by every other cell, so it must not be
    # mutated in place.
    assert expectation.other_table_name == SELF_TABLE_SENTINEL


@pytest.mark.unit
def test_resolve_self_references_substitutes_the_data_source_sentinel() -> None:
    expectation = gxe.ExpectQueryResultsToMatchComparison(
        base_query="SELECT 1 AS v FROM {batch}",
        comparison_data_source_name=SELF_DATA_SOURCE_SENTINEL,
        comparison_query="SELECT 1 AS v",
    )
    resolved = resolve_self_references(
        expectation, table_name="tbl_abc123", data_source_name="ds_abc123"
    )
    assert resolved.comparison_data_source_name == "ds_abc123"
    assert resolved.base_query == "SELECT 1 AS v FROM {batch}"


@pytest.mark.unit
def test_resolve_self_references_returns_the_same_instance_when_no_sentinel_is_present() -> None:
    expectation = gxe.ExpectColumnValuesToNotBeNull(column="increasing_key")
    assert (
        resolve_self_references(expectation, table_name="tbl", data_source_name="ds") is expectation
    )


@pytest.mark.unit
def test_resolve_self_references_rejects_an_unavailable_name() -> None:
    """A batch setup with no table would otherwise pass the literal sentinel through to the engine
    as a table name, where it surfaces as a confusing 'no such table __matrix_self_table__'."""
    expectation = gxe.ExpectTableRowCountToEqualOtherTable(other_table_name=SELF_TABLE_SENTINEL)
    with pytest.raises(ValueError, match="no such name"):
        resolve_self_references(expectation, table_name=None, data_source_name="ds_abc123")


# ---------------------------------------------------------------------------
# _extra_fields_rejected — the runner's schema_extras_rejected extraction
# ---------------------------------------------------------------------------
#
# schema_extras_rejected is populated only when a result dict was rejected for carrying a
# field its schema does not declare. That is observable in exactly one place: a ParseError
# whose wrapped pydantic errors include an "extra fields not permitted" entry naming the
# offending key. Every other failure path (a metric that raised, an assertion that the parsed
# model changed a value) has nothing to report here, and a clean parse never reaches this
# function at all -- the runner never calls it on the PARSED path.


@pytest.mark.unit
def test_extra_fields_rejected_populated_on_a_synthetic_extra_field_failure() -> None:
    """A ParseError carrying pydantic's own 'extra fields not permitted' errors names the
    offending keys, without parsing the exception's message text."""
    exc = ParseError(
        "synthetic failure for this test",
        pydantic_errors=[
            {
                "loc": ("unexpected_extra_field",),
                "msg": "extra fields not permitted",
                "type": "value_error.extra",
            },
        ],
    )
    assert extra_fields_rejected(exc) == ["unexpected_extra_field"]


@pytest.mark.unit
def test_extra_fields_rejected_populated_from_a_real_dispatcher_parse_error() -> None:
    """The same extraction against a ParseError the dispatcher actually raised, not a fabricated
    one -- proving the ``pydantic_errors`` attribute and the extraction agree on the real shape
    pydantic v1 produces for Extra.forbid."""
    bad_dict = {"observed_value": 6, "unexpected_extra_field": "boom"}
    with pytest.raises(ParseError) as exc_info:
        as_typed(
            bad_dict,
            expectation_type="expect_column_mean_to_be_between",
            result_format=ResultFormat.BOOLEAN_ONLY,
        )
    assert extra_fields_rejected(exc_info.value) == ["unexpected_extra_field"]


@pytest.mark.unit
def test_extra_fields_rejected_empty_for_a_parse_error_with_no_extra_field_errors() -> None:
    """A ParseError raised for a reason other than an unpermitted extra field -- an unregistered
    expectation type, say -- carries no pydantic_errors at all, and reports no rejected keys."""
    exc = ParseError("no such expectation is registered")
    assert extra_fields_rejected(exc) == []


@pytest.mark.unit
def test_extra_fields_rejected_empty_for_a_non_parse_error() -> None:
    """Any other exception type (an AssertionError from assert_field_set_covered, say) is not a
    schema rejection and reports no rejected keys."""
    assert extra_fields_rejected(AssertionError("some other failure")) == []
