"""Unit tests for the MapResult schema family.

Covers:
- Each format variant (MapBooleanOnlyResult, MapBasicResult, MapSummaryResult,
  MapCompleteResult) parses a valid result dict correctly.
- All expected fields match the input.
- Unknown extra fields raise pydantic.ValidationError (extra=forbid).
- Validator functions (validate_unexpected_rows_passthrough,
  validate_partial_unexpected_counts_fallback) work as expected.
- This layer types and reports shape; it does not enforce cross-field
  invariants such as "an index query was requested, so one must be present".
- Scalar fields are strict: they accept a value as-is or reject it, never coerce.

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_schemas_map.py -m unit
"""

from __future__ import annotations

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core.validation_result_schemas.schemas.map_result import (
    MapBasicResult,
    MapBooleanOnlyResult,
    MapCompleteResult,
    MapResultBase,
    MapSummaryResult,
)

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

_BASIC_RESULT_DATA = {
    "element_count": 100,
    "unexpected_count": 5,
    "unexpected_percent": 5.0,
    "missing_count": 2,
    "missing_percent": 2.0,
    "unexpected_percent_total": 5.0,
    "unexpected_percent_nonmissing": 5.0,
    "partial_unexpected_list": [1, 2, 3],
    "unexpected_rows": None,
}

_SUMMARY_EXTRA_DATA = {
    "partial_unexpected_counts": [{"value": 1, "count": 3}],
    "partial_unexpected_index_list": [],
}

_COMPLETE_EXTRA_DATA = {
    "unexpected_list": [1, 2, 3],
    "unexpected_index_list": [10, 11, 12],
}


# ---------------------------------------------------------------------------
# MapBooleanOnlyResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_boolean_only_empty_dict() -> None:
    """BOOLEAN_ONLY result dict is typically empty."""
    m = MapBooleanOnlyResult()
    assert m.unexpected_index_query is None
    assert m.unexpected_index_column_names is None
    assert m.engine_hint is None


@pytest.mark.unit
def test_map_boolean_only_with_sql_fields() -> None:
    """SQL engine can set unexpected_index_query and unexpected_index_column_names."""
    m = MapBooleanOnlyResult(
        unexpected_index_query="SELECT * FROM foo WHERE ...",
        unexpected_index_column_names=["id"],
    )
    assert m.unexpected_index_query == "SELECT * FROM foo WHERE ..."
    assert m.unexpected_index_column_names == ["id"]


@pytest.mark.unit
def test_map_boolean_only_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError."""
    with pytest.raises(pydantic.ValidationError):
        MapBooleanOnlyResult.parse_obj({"unknown_field": "should_fail"})


@pytest.mark.unit
def test_map_boolean_only_basic_result_fields_are_rejected() -> None:
    """MapBooleanOnlyResult does not accept MapBasicResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        MapBooleanOnlyResult.parse_obj({"element_count": 100})


# ---------------------------------------------------------------------------
# MapBasicResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_basic_parses_valid_result() -> None:
    """MapBasicResult parses a typical pandas BASIC result dict correctly."""
    m = MapBasicResult.parse_obj(_BASIC_RESULT_DATA)
    assert m.element_count == 100
    assert m.unexpected_count == 5
    assert m.unexpected_percent == 5.0
    assert m.missing_count == 2
    assert m.missing_percent == 2.0
    assert m.unexpected_percent_total == 5.0
    assert m.unexpected_percent_nonmissing == 5.0
    assert m.partial_unexpected_list == [1, 2, 3]
    assert m.unexpected_rows is None


@pytest.mark.unit
def test_map_basic_all_fields_none() -> None:
    """All fields are Optional so MapBasicResult can be constructed with no args."""
    m = MapBasicResult()
    assert m.element_count is None
    assert m.unexpected_count is None
    assert m.partial_unexpected_list is None
    assert m.unexpected_rows is None


@pytest.mark.unit
def test_map_basic_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in MapBasicResult."""
    with pytest.raises(pydantic.ValidationError):
        MapBasicResult.parse_obj({**_BASIC_RESULT_DATA, "unknown_field": "bad"})


@pytest.mark.unit
def test_map_basic_unexpected_rows_accepts_none() -> None:
    m = MapBasicResult(unexpected_rows=None)
    assert m.unexpected_rows is None


@pytest.mark.unit
def test_map_basic_unexpected_rows_accepts_list() -> None:
    rows = [{"col_a": 1, "col_b": "x"}, {"col_a": 2, "col_b": "y"}]
    m = MapBasicResult(unexpected_rows=rows)
    assert m.unexpected_rows == rows


@pytest.mark.unit
def test_map_basic_unexpected_rows_accepts_string() -> None:
    """unexpected_rows: Any accepts string (e.g., a serialized representation)."""
    m = MapBasicResult(unexpected_rows="some-string-representation")
    assert m.unexpected_rows == "some-string-representation"


@pytest.mark.unit
def test_map_basic_inherits_sql_fields() -> None:
    """MapBasicResult inherits SQL-only fields from MapResultBase."""
    m = MapBasicResult.parse_obj(
        {
            **_BASIC_RESULT_DATA,
            "unexpected_index_query": "SELECT ...",
            "unexpected_index_column_names": ["pk"],
        }
    )
    assert m.unexpected_index_query == "SELECT ..."
    assert m.unexpected_index_column_names == ["pk"]


@pytest.mark.unit
def test_map_basic_summary_only_field_raises() -> None:
    """MapBasicResult does not accept MapSummaryResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        MapBasicResult.parse_obj({**_BASIC_RESULT_DATA, "partial_unexpected_counts": []})


# ---------------------------------------------------------------------------
# MapSummaryResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_summary_parses_valid_result() -> None:
    """MapSummaryResult parses a typical SUMMARY result dict correctly."""
    data = {**_BASIC_RESULT_DATA, **_SUMMARY_EXTRA_DATA}
    m = MapSummaryResult.parse_obj(data)
    assert m.element_count == 100
    assert m.partial_unexpected_counts == [{"value": 1, "count": 3}]
    assert m.partial_unexpected_index_list == []


@pytest.mark.unit
def test_map_summary_all_optional() -> None:
    """All fields in MapSummaryResult are Optional."""
    m = MapSummaryResult()
    assert m.partial_unexpected_counts is None
    assert m.partial_unexpected_index_list is None


@pytest.mark.unit
def test_map_summary_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in MapSummaryResult."""
    data = {**_BASIC_RESULT_DATA, **_SUMMARY_EXTRA_DATA}
    with pytest.raises(pydantic.ValidationError):
        MapSummaryResult.parse_obj({**data, "unknown_field": "bad"})


@pytest.mark.unit
def test_map_summary_partial_counts_accepts_canonical_shape() -> None:
    """partial_unexpected_counts: [{value: x, count: n}, ...] is canonical."""
    counts = [{"value": "foo", "count": 3}, {"value": "bar", "count": 1}]
    m = MapSummaryResult(partial_unexpected_counts=counts)
    assert m.partial_unexpected_counts == counts


@pytest.mark.unit
def test_map_summary_partial_counts_accepts_error_fallback() -> None:
    """partial_unexpected_counts: [{"error": "..."}] fallback shape is accepted."""
    fallback = [{"error": "partial_exception_counts requires a hashable type"}]
    m = MapSummaryResult(partial_unexpected_counts=fallback)
    assert m.partial_unexpected_counts == fallback


@pytest.mark.unit
def test_map_summary_partial_counts_accepts_none() -> None:
    m = MapSummaryResult(partial_unexpected_counts=None)
    assert m.partial_unexpected_counts is None


@pytest.mark.unit
def test_map_summary_complete_only_field_raises() -> None:
    """MapSummaryResult does not accept MapCompleteResult-only fields."""
    with pytest.raises(pydantic.ValidationError):
        MapSummaryResult.parse_obj({**_BASIC_RESULT_DATA, "unexpected_list": [1, 2, 3]})


# ---------------------------------------------------------------------------
# MapCompleteResult
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_complete_parses_valid_result() -> None:
    """MapCompleteResult parses a typical COMPLETE result dict correctly."""
    data = {**_BASIC_RESULT_DATA, **_SUMMARY_EXTRA_DATA, **_COMPLETE_EXTRA_DATA}
    m = MapCompleteResult.parse_obj(data)
    assert m.element_count == 100
    assert m.partial_unexpected_counts == [{"value": 1, "count": 3}]
    assert m.unexpected_list == [1, 2, 3]
    assert m.unexpected_index_list == [10, 11, 12]


@pytest.mark.unit
def test_map_complete_all_optional() -> None:
    """All fields in MapCompleteResult are Optional."""
    m = MapCompleteResult()
    assert m.unexpected_list is None
    assert m.unexpected_index_list is None


@pytest.mark.unit
def test_map_complete_extra_field_raises() -> None:
    """extra=forbid: unknown fields raise ValidationError in MapCompleteResult."""
    data = {**_BASIC_RESULT_DATA, **_SUMMARY_EXTRA_DATA, **_COMPLETE_EXTRA_DATA}
    with pytest.raises(pydantic.ValidationError):
        MapCompleteResult.parse_obj({**data, "not_a_real_field": "value"})


@pytest.mark.unit
def test_map_complete_inherits_all_ancestor_fields() -> None:
    """MapCompleteResult inherits fields from all ancestor classes."""
    data = {
        **_BASIC_RESULT_DATA,
        **_SUMMARY_EXTRA_DATA,
        **_COMPLETE_EXTRA_DATA,
        "unexpected_index_query": "SELECT ...",
        "unexpected_index_column_names": ["id"],
    }
    m = MapCompleteResult.parse_obj(data)
    # From MapResultBase
    assert m.unexpected_index_query == "SELECT ..."
    assert m.unexpected_index_column_names == ["id"]
    # From MapBasicResult
    assert m.element_count == 100
    assert m.partial_unexpected_list == [1, 2, 3]
    # From MapSummaryResult
    assert m.partial_unexpected_counts == [{"value": 1, "count": 3}]
    # From MapCompleteResult
    assert m.unexpected_list == [1, 2, 3]
    assert m.unexpected_index_list == [10, 11, 12]


# ---------------------------------------------------------------------------
# This layer types and reports; it does not enforce cross-field invariants
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_complete_sql_hint_without_index_query_parses() -> None:
    """A SQL-hinted COMPLETE result missing ``unexpected_index_query`` still parses.

    Every field on the map family is Optional and this layer's job is to type
    and report the shape it is handed, not to assert that an engine-conditional
    field must be present. Guard against this schema growing an enforcement
    rule it does not actually carry.
    """
    m = MapCompleteResult(engine_hint="sql", unexpected_index_query=None)
    assert m.engine_hint == "sql"
    assert m.unexpected_index_query is None


# ---------------------------------------------------------------------------
# engine_hint field inheritance
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_engine_hint_present_in_all_variants() -> None:
    """engine_hint is declared on MapResultBase and inherited by all variants."""
    assert "engine_hint" in MapResultBase.__fields__
    assert "engine_hint" in MapBooleanOnlyResult.__fields__
    assert "engine_hint" in MapBasicResult.__fields__
    assert "engine_hint" in MapSummaryResult.__fields__
    assert "engine_hint" in MapCompleteResult.__fields__


@pytest.mark.unit
def test_engine_hint_defaults_to_none() -> None:
    """engine_hint defaults to None on all variants."""
    assert MapBooleanOnlyResult().engine_hint is None
    assert MapBasicResult().engine_hint is None
    assert MapSummaryResult().engine_hint is None
    assert MapCompleteResult().engine_hint is None


# ---------------------------------------------------------------------------
# engine_hint is excluded from exported output, but not from the model itself
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "model_class",
    [MapResultBase, MapBooleanOnlyResult, MapBasicResult, MapSummaryResult, MapCompleteResult],
    ids=["base", "boolean_only", "basic", "summary", "complete"],
)
def test_engine_hint_excluded_from_dict_and_json_on_every_variant(model_class) -> None:
    """engine_hint is internal plumbing; it must never appear in exported output.

    ``Field(exclude=True)`` in pydantic v1 drops the field from ``.dict()`` and
    ``.json()`` while leaving it fully readable as a plain attribute -- export and
    attribute access are governed independently.
    """
    m = model_class(engine_hint="sql")
    assert "engine_hint" not in m.dict()
    assert m.engine_hint == "sql"
    assert '"engine_hint"' not in m.json()


@pytest.mark.unit
def test_engine_hint_omitted_from_dict_when_unset() -> None:
    """The exclusion holds even when engine_hint is left at its default of None."""
    m = MapCompleteResult()
    assert "engine_hint" not in m.dict()
    assert m.engine_hint is None


@pytest.mark.unit
def test_engine_hint_still_reaches_root_validators_despite_export_exclusion() -> None:
    """``exclude=True`` governs serialization only -- pydantic v1 still populates the
    field in the ``values`` dict a root validator receives, since validation runs
    before export. Any root validator bound on the map family that reads
    ``engine_hint`` would see it, so the export exclusion cannot silently blind one.
    """
    seen_values: dict = {}

    def _capture_values(cls, values):
        seen_values.update(values)
        return values

    class _EngineHintVisibilityProbe(MapCompleteResult):
        _capture = pydantic.root_validator(allow_reuse=True)(_capture_values)

    m = _EngineHintVisibilityProbe(engine_hint="sql")
    assert seen_values.get("engine_hint") == "sql"
    # And the exclusion still holds on the subclass's own export.
    assert "engine_hint" not in m.dict()


# ---------------------------------------------------------------------------
# extra=forbid on MapResultBase
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_map_result_base_extra_forbid() -> None:
    """MapResultBase itself also enforces extra=forbid."""
    with pytest.raises(pydantic.ValidationError):
        MapResultBase.parse_obj({"completely_unknown": "value"})


# ---------------------------------------------------------------------------
# Scalar fields accept a value unchanged or reject it — they never convert
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_counts_reject_a_float_rather_than_truncating() -> None:
    """A non-integral count is a finding, not something to silently floor."""
    with pytest.raises(pydantic.ValidationError):
        MapBasicResult.parse_obj({"element_count": 2.7})


@pytest.mark.unit
def test_counts_reject_a_bool() -> None:
    """bool is a subclass of int; accepting it would report True as 1."""
    with pytest.raises(pydantic.ValidationError):
        MapBasicResult.parse_obj({"unexpected_count": True})


@pytest.mark.unit
@pytest.mark.parametrize(
    "value", [0, 5, 5.0, 2.5], ids=["int-0", "int-5", "float-5.0", "float-2.5"]
)
def test_percent_accepts_int_and_float_without_widening(value: object) -> None:
    parsed = MapBasicResult.parse_obj({"unexpected_percent": value}).unexpected_percent
    assert type(parsed) is type(value)
    assert parsed == value


@pytest.mark.unit
def test_index_query_rejects_a_non_string_rather_than_stringifying() -> None:
    with pytest.raises(pydantic.ValidationError):
        MapResultBase.parse_obj({"unexpected_index_query": 42})


@pytest.mark.unit
def test_index_column_names_reject_non_strings() -> None:
    with pytest.raises(pydantic.ValidationError):
        MapResultBase.parse_obj({"unexpected_index_column_names": [1, 2]})


@pytest.mark.unit
@pytest.mark.parametrize(
    "value",
    [1, 0, 2, 2.5, True, "int64", [1], {"a": 1}],
    ids=["int-1", "int-0", "int-2", "float", "bool", "str", "list", "dict"],
)
def test_observed_value_round_trips_with_identical_type_and_value(value: object) -> None:
    """Map results carry observed_value too, and it is reported, never converted."""
    parsed = MapBasicResult.parse_obj({"observed_value": value}).observed_value
    assert type(parsed) is type(value)
    assert parsed == value


@pytest.mark.unit
def test_partial_unexpected_list_elements_are_untouched() -> None:
    """Container element types stay Any so their contents pass through as-is."""
    values = [1, True, 2.5, "x", None]
    parsed = MapBasicResult.parse_obj({"partial_unexpected_list": values}).partial_unexpected_list
    assert parsed is not None
    assert [type(v) for v in parsed] == [type(v) for v in values]
    assert parsed == values


# ---------------------------------------------------------------------------
# Inheritance chain sanity
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_inheritance_chain() -> None:
    """MapCompleteResult → MapSummaryResult → MapBasicResult → MapResultBase."""
    assert issubclass(MapCompleteResult, MapSummaryResult)
    assert issubclass(MapSummaryResult, MapBasicResult)
    assert issubclass(MapBasicResult, MapResultBase)
    assert issubclass(MapBooleanOnlyResult, MapResultBase)


@pytest.mark.unit
def test_map_complete_is_not_map_boolean_only() -> None:
    """MapCompleteResult and MapBooleanOnlyResult are separate leaf classes."""
    assert not issubclass(MapCompleteResult, MapBooleanOnlyResult)
    assert not issubclass(MapBooleanOnlyResult, MapCompleteResult)


@pytest.mark.unit
@pytest.mark.parametrize("raw_count", [2, 2.0])
def test_basic_counts_keep_the_numeric_type_the_engine_emitted(raw_count) -> None:
    """MySQL returns COUNT results as floats; every other engine returns ints.

    The schema accepts both and preserves whichever arrived, so the findings' recorded runtime
    type is what exposes the divergence rather than a coercion hiding it or a rejection
    misreporting it as a schema failure.
    """
    model = MapBasicResult.parse_obj(
        {"element_count": 4, "unexpected_count": raw_count, "missing_count": raw_count}
    )
    assert model.unexpected_count == raw_count
    assert type(model.unexpected_count) is type(raw_count)
    assert type(model.missing_count) is type(raw_count)


@pytest.mark.unit
def test_basic_counts_reject_strings() -> None:
    with pytest.raises(pydantic.ValidationError):
        MapBasicResult.parse_obj({"unexpected_count": "2"})
