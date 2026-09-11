"""Unit tests for the dispatcher module.

Covers:
- Synthetic input per (family, format) cell — all 8 combinations.
- family_for derives the family from the expectation class hierarchy, and raises
  for an unregistered type.
- Format inference from the result dict's key set.
- engine_hint is never guessed from the result dict.
- Per-expectation override route (expect_column_values_to_be_of_type on sql/spark).
- ParseError raised with a diagnostic message on bad input.
- test_every_core_expectation_resolves_to_a_family: every expectation registered
  from great_expectations.expectations.core resolves without raising.

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_dispatcher.py -m unit -v
"""

from __future__ import annotations

from itertools import pairwise

import pytest

from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.dispatcher import (
    _AMBIGUOUS_SHAPE_FORMAT,
    _FAMILY_CACHE,
    _FORMAT_MAP,
    _FORMAT_ORDER,
    _SCHEMA_FIELDS,
    FAMILY_AGGREGATE,
    FAMILY_MAP,
    ParseError,
    UnknownExpectationTypeError,
    as_typed,
    family_for,
    infer_result_format,
)
from great_expectations.core.validation_result_schemas.schemas.aggregate_result import (
    AggregateBasicResult,
    AggregateBooleanOnlyResult,
    AggregateCompleteResult,
    AggregateSummaryResult,
)
from great_expectations.core.validation_result_schemas.schemas.map_result import (
    MapBasicResult,
    MapBooleanOnlyResult,
    MapCompleteResult,
    MapSummaryResult,
)
from great_expectations.core.validation_result_schemas.schemas.per_expectation_overrides import (
    TypeExpectationObservedValueResult,
)
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)

# ---------------------------------------------------------------------------
# A canonical map expectation and aggregate expectation used across tests
# ---------------------------------------------------------------------------

MAP_EXPECTATION = "expect_column_values_to_be_between"
AGG_EXPECTATION = "expect_column_mean_to_be_between"
UNREGISTERED_EXPECTATION = "expect_some_custom_unknown_expectation"

# ---------------------------------------------------------------------------
# Minimal valid result dicts per family x format
# ---------------------------------------------------------------------------

MAP_BOOLEAN_ONLY_DICT: dict = {}
MAP_BASIC_DICT: dict = {
    "element_count": 100,
    "unexpected_count": 5,
    "unexpected_percent": 5.0,
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 5.0,
    "unexpected_percent_nonmissing": 5.0,
    "partial_unexpected_list": [1, 2, 3],
}
MAP_SUMMARY_DICT: dict = {
    **MAP_BASIC_DICT,
    "partial_unexpected_counts": [{"value": 1, "count": 2}],
    "partial_unexpected_index_list": [0, 1],
}
MAP_COMPLETE_DICT: dict = {
    **MAP_SUMMARY_DICT,
    "unexpected_list": [1, 2, 3, 4, 5],
    "unexpected_index_list": [0, 1, 2, 3, 4],
}

AGG_BOOLEAN_ONLY_DICT: dict = {}
AGG_BASIC_DICT: dict = {"observed_value": 42.0}
AGG_SUMMARY_DICT: dict = {"observed_value": 42.0}
AGG_COMPLETE_DICT: dict = {
    "observed_value": 42.0,
    "unexpected_list": None,
    "unexpected_index_list": None,
}


# ---------------------------------------------------------------------------
# (family, format) matrix — 8 cells
# ---------------------------------------------------------------------------


class TestFamilyFormatMatrix:
    """as_typed returns the correct model class for every (family, format) cell."""

    @pytest.mark.unit
    def test_map_boolean_only(self):
        result = as_typed(
            MAP_BOOLEAN_ONLY_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.BOOLEAN_ONLY,
        )
        assert isinstance(result, MapBooleanOnlyResult)

    @pytest.mark.unit
    def test_map_basic(self):
        result = as_typed(
            MAP_BASIC_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.BASIC,
        )
        assert isinstance(result, MapBasicResult)
        assert result.element_count == 100
        assert result.unexpected_count == 5

    @pytest.mark.unit
    def test_map_summary(self):
        result = as_typed(
            MAP_SUMMARY_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.SUMMARY,
        )
        assert isinstance(result, MapSummaryResult)
        assert result.partial_unexpected_index_list == [0, 1]

    @pytest.mark.unit
    def test_map_complete(self):
        result = as_typed(
            MAP_COMPLETE_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
        )
        assert isinstance(result, MapCompleteResult)
        assert result.unexpected_list == [1, 2, 3, 4, 5]

    @pytest.mark.unit
    def test_aggregate_boolean_only(self):
        result = as_typed(
            AGG_BOOLEAN_ONLY_DICT,
            expectation_type=AGG_EXPECTATION,
            result_format=ResultFormat.BOOLEAN_ONLY,
        )
        assert isinstance(result, AggregateBooleanOnlyResult)

    @pytest.mark.unit
    def test_aggregate_basic(self):
        result = as_typed(
            AGG_BASIC_DICT,
            expectation_type=AGG_EXPECTATION,
            result_format=ResultFormat.BASIC,
        )
        assert isinstance(result, AggregateBasicResult)
        assert result.observed_value == 42.0

    @pytest.mark.unit
    def test_aggregate_summary(self):
        result = as_typed(
            AGG_SUMMARY_DICT,
            expectation_type=AGG_EXPECTATION,
            result_format=ResultFormat.SUMMARY,
        )
        assert isinstance(result, AggregateSummaryResult)

    @pytest.mark.unit
    def test_aggregate_complete(self):
        result = as_typed(
            AGG_COMPLETE_DICT,
            expectation_type=AGG_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
        )
        assert isinstance(result, AggregateCompleteResult)


# ---------------------------------------------------------------------------
# family_for — derived from the expectation class hierarchy
# ---------------------------------------------------------------------------


class TestFamilyFor:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_values_to_be_between",
            "expect_column_values_to_not_be_outliers",
            "expect_column_pair_values_to_be_equal",
            "expect_multicolumn_values_to_be_equal",
            "expect_compound_columns_to_be_unique",
        ],
    )
    def test_map_expectations(self, expectation_type: str):
        assert family_for(expectation_type) == FAMILY_MAP

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_mean_to_be_between",
            "expect_column_distinct_values_to_equal_set",
            "expect_table_row_count_to_equal",
            "expect_table_columns_to_match_set",
            "unexpected_rows_expectation",
        ],
    )
    def test_aggregate_expectations(self, expectation_type: str):
        assert family_for(expectation_type) == FAMILY_AGGREGATE

    @pytest.mark.unit
    def test_unregistered_type_raises(self):
        """An unregistered type has no derivable family, so family_for says so."""
        with pytest.raises(UnknownExpectationTypeError) as exc_info:
            family_for(UNREGISTERED_EXPECTATION)
        assert UNREGISTERED_EXPECTATION in str(exc_info.value)

    @pytest.mark.unit
    def test_unregistered_type_is_a_parse_error_from_as_typed(self):
        """as_typed converts the family lookup failure into a ParseError naming the type."""
        with pytest.raises(ParseError) as exc_info:
            as_typed(
                AGG_BASIC_DICT,
                expectation_type=UNREGISTERED_EXPECTATION,
                result_format=ResultFormat.BASIC,
            )
        assert UNREGISTERED_EXPECTATION in str(exc_info.value)

    @pytest.mark.unit
    def test_result_is_memoized(self):
        """Repeat lookups are served from the cache rather than the registry."""
        _FAMILY_CACHE.pop(MAP_EXPECTATION, None)
        assert family_for(MAP_EXPECTATION) == FAMILY_MAP
        assert _FAMILY_CACHE[MAP_EXPECTATION] == FAMILY_MAP
        # A cached answer is returned even if the registry no longer agrees.
        _FAMILY_CACHE[MAP_EXPECTATION] = FAMILY_AGGREGATE
        try:
            assert family_for(MAP_EXPECTATION) == FAMILY_AGGREGATE
        finally:
            _FAMILY_CACHE[MAP_EXPECTATION] = FAMILY_MAP

    @pytest.mark.unit
    def test_unregistered_type_is_not_cached(self):
        """A failed lookup must not poison the cache."""
        with pytest.raises(UnknownExpectationTypeError):
            family_for(UNREGISTERED_EXPECTATION)
        assert UNREGISTERED_EXPECTATION not in _FAMILY_CACHE


@pytest.mark.unit
def test_every_core_expectation_resolves_to_a_family():
    """Every expectation registered from expectations/core resolves to a family.

    The registry is the source of truth, not a directory glob: an expectation
    registered under a name that does not match its module's filename (e.g.
    unexpected_rows_expectation) is invisible to a glob but reaches the dispatcher
    like any other.
    """
    core_types = [
        name
        for name in list_registered_expectation_implementations()
        if get_expectation_impl(name).__module__.startswith("great_expectations.expectations.core")
    ]
    assert core_types, "No core expectations registered — the registry was not populated"

    unresolved = {}
    for name in core_types:
        try:
            family = family_for(name)
        except UnknownExpectationTypeError as exc:  # pragma: no cover - defensive
            unresolved[name] = str(exc)
            continue
        if family not in (FAMILY_MAP, FAMILY_AGGREGATE):
            unresolved[name] = f"unexpected family {family!r}"

    assert not unresolved, f"Unresolved expectation families: {unresolved}"


# ---------------------------------------------------------------------------
# Format inference from the result dict's key set
# ---------------------------------------------------------------------------


class TestInferResultFormat:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("result_dict", "expected"),
        [
            (MAP_BASIC_DICT, ResultFormat.BASIC),
            (MAP_SUMMARY_DICT, ResultFormat.SUMMARY),
            (MAP_COMPLETE_DICT, ResultFormat.COMPLETE),
        ],
    )
    def test_map_shapes(self, result_dict: dict, expected: ResultFormat):
        assert infer_result_format(result_dict, family=FAMILY_MAP) == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("family", [FAMILY_MAP, FAMILY_AGGREGATE])
    def test_empty_dict_is_undetermined(self, family: str):
        """An empty result dict does not, by itself, name a format.

        BOOLEAN_ONLY renders as an empty dict, but it is not the only format
        that can: some expectations (expect_column_to_exist, for one) emit an
        empty result dict at every format.  Emptiness is consistent with any
        of them, so it cannot discriminate -- the caller's fallback chain
        (explicit -> shape -> configured -> most permissive) decides instead.
        """
        assert infer_result_format({}, family=family) is None

    @pytest.mark.unit
    def test_aggregate_complete_shape(self):
        assert (
            infer_result_format(AGG_COMPLETE_DICT, family=FAMILY_AGGREGATE) == ResultFormat.COMPLETE
        )

    @pytest.mark.unit
    def test_aggregate_observed_value_only_is_undetermined(self):
        """Aggregate BASIC, SUMMARY, and COMPLETE payloads can be identical."""
        assert infer_result_format(AGG_BASIC_DICT, family=FAMILY_AGGREGATE) is None

    @pytest.mark.unit
    def test_index_query_alone_is_undetermined(self):
        """unexpected_index_query is on the base, so it names no particular format."""
        assert (
            infer_result_format(
                {"unexpected_index_query": "df.filter(items=[3], axis=0)"}, family=FAMILY_MAP
            )
            is None
        )


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expectation_type", "family"),
    [
        (MAP_EXPECTATION, FAMILY_MAP),
        (AGG_EXPECTATION, FAMILY_AGGREGATE),
    ],
)
@pytest.mark.parametrize("result_format", list(ResultFormat))
def test_explicit_result_format_wins_for_empty_dict(
    expectation_type: str, family: str, result_format: ResultFormat
):
    """An empty dict is undetermined on its own, but an explicit result_format is
    still authoritative -- every field of every variant is Optional, so {} parses
    cleanly under any of them, at any format the caller asks for.
    """
    result = as_typed(
        {},
        expectation_type=expectation_type,
        result_format=result_format,
    )
    assert type(result) is _FORMAT_MAP[family][result_format]


@pytest.mark.unit
def test_format_order_is_a_widening_chain():
    """Each format's field set contains the one before it, in both families.

    The inference reads the discriminating fields as set differences along this
    chain, and the ambiguous-shape fallback assumes the last format accepts every
    well-formed result. Both are wrong if the chain ever stops widening.
    """
    for family, by_format in _SCHEMA_FIELDS.items():
        for narrower, wider in pairwise(_FORMAT_ORDER):
            assert by_format[narrower] <= by_format[wider], (
                f"{family}: {wider.value} does not accept every {narrower.value} field"
            )


@pytest.mark.unit
def test_ambiguous_shape_falls_back_to_the_most_permissive_format():
    assert _FORMAT_ORDER[-1] == _AMBIGUOUS_SHAPE_FORMAT


class TestResultFormatResolution:
    """as_typed resolves the format from the argument, the shape, then configuration."""

    @pytest.mark.unit
    def test_declared_format_wins_over_the_shape(self):
        """An explicit request is authoritative, so a shape mismatch surfaces."""
        with pytest.raises(ParseError):
            as_typed(
                MAP_COMPLETE_DICT,
                expectation_type=MAP_EXPECTATION,
                result_format=ResultFormat.BASIC,
            )

    @pytest.mark.unit
    def test_shape_is_used_when_no_format_is_declared(self):
        result = as_typed(MAP_COMPLETE_DICT, expectation_type=MAP_EXPECTATION)
        assert isinstance(result, MapCompleteResult)

    @pytest.mark.unit
    def test_shape_wins_over_configuration(self):
        """Configuration can disagree with what the engine rendered; the dict cannot."""
        result = as_typed(
            MAP_COMPLETE_DICT,
            expectation_type=MAP_EXPECTATION,
            configured_result_format="BASIC",
        )
        assert isinstance(result, MapCompleteResult)

    @pytest.mark.unit
    def test_configuration_is_used_when_the_shape_is_undetermined(self):
        result = as_typed(
            AGG_BASIC_DICT,
            expectation_type=AGG_EXPECTATION,
            configured_result_format="BASIC",
        )
        assert isinstance(result, AggregateBasicResult)
        assert not isinstance(result, AggregateSummaryResult)

    @pytest.mark.unit
    def test_configuration_accepts_a_parsed_config_dict(self):
        result = as_typed(
            AGG_BASIC_DICT,
            expectation_type=AGG_EXPECTATION,
            configured_result_format={"result_format": "BASIC", "partial_unexpected_count": 20},
        )
        assert isinstance(result, AggregateBasicResult)
        assert not isinstance(result, AggregateSummaryResult)

    @pytest.mark.unit
    def test_config_dict_without_a_format_is_ignored(self):
        """parse_result_format({}) returns a config that names no format."""
        result = as_typed(
            AGG_BASIC_DICT,
            expectation_type=AGG_EXPECTATION,
            configured_result_format={"partial_unexpected_count": 20},
        )
        assert isinstance(result, AggregateCompleteResult)

    @pytest.mark.unit
    def test_undetermined_shape_with_no_configuration_uses_the_fallback(self):
        result = as_typed(AGG_BASIC_DICT, expectation_type=AGG_EXPECTATION)
        assert isinstance(result, AggregateCompleteResult)


# ---------------------------------------------------------------------------
# engine_hint — never guessed from the result dict
# ---------------------------------------------------------------------------


class TestEngineHint:
    @pytest.mark.unit
    def test_index_query_does_not_imply_sql(self):
        """pandas emits unexpected_index_query too, so its presence proves nothing.

        A pandas COMPLETE result carries e.g. ``df.filter(items=[3], axis=0)``.
        Treating that as a SQL engine would arm SQL-only validation against it.
        """
        result_dict = {
            **MAP_COMPLETE_DICT,
            "unexpected_index_query": "df.filter(items=[3], axis=0)",
        }
        result = as_typed(
            result_dict,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
            engine_hint=None,
        )
        assert isinstance(result, MapCompleteResult)
        assert result.unexpected_index_query == "df.filter(items=[3], axis=0)"
        assert result.engine_hint is None

    @pytest.mark.unit
    def test_explicit_engine_hint_is_propagated(self):
        result_dict = {
            **MAP_COMPLETE_DICT,
            "unexpected_index_query": "SELECT * FROM table WHERE x < 0",
        }
        result = as_typed(
            result_dict,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
            engine_hint="sql",
        )
        assert isinstance(result, MapCompleteResult)
        assert result.engine_hint == "sql"

    @pytest.mark.unit
    def test_pandas_engine_hint_is_propagated(self):
        result = as_typed(
            MAP_COMPLETE_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
            engine_hint="pandas",
        )
        assert isinstance(result, MapCompleteResult)
        assert result.engine_hint == "pandas"

    @pytest.mark.unit
    def test_no_hint_leaves_the_field_unset(self):
        result = as_typed(
            MAP_COMPLETE_DICT,
            expectation_type=MAP_EXPECTATION,
            result_format=ResultFormat.COMPLETE,
            engine_hint=None,
        )
        assert isinstance(result, MapCompleteResult)
        assert result.engine_hint is None

    @pytest.mark.unit
    def test_hint_is_not_injected_into_aggregate_schemas(self):
        """Aggregate schemas do not declare engine_hint and forbid extras."""
        result = as_typed(
            AGG_BASIC_DICT,
            expectation_type=AGG_EXPECTATION,
            result_format=ResultFormat.BASIC,
            engine_hint="pandas",
        )
        assert isinstance(result, AggregateBasicResult)


# ---------------------------------------------------------------------------
# Per-expectation override route
# ---------------------------------------------------------------------------


class TestPerExpectationOverride:
    """The override is selected by the shape of ``result_dict``, not ``engine_hint``.

    ``expect_column_values_to_be_of_type`` and ``expect_column_values_to_be_in_type_list``
    both emit a bare ``{"observed_value": ...}`` payload from pandas, SQL, and Spark
    alike whenever they take their non-map validation path -- only pandas' object-dtype
    map path produces the full map field set instead.  ``engine_hint`` plays no part in
    the decision; these tests pass it in each of its possible values to prove that.
    """

    @pytest.mark.unit
    def test_override_with_sql_engine_hint(self):
        """A bare observed_value payload types to the override under a sql hint."""
        result_dict = {"observed_value": "int64"}
        result = as_typed(
            result_dict,
            expectation_type="expect_column_values_to_be_of_type",
            result_format=ResultFormat.SUMMARY,
            engine_hint="sql",
        )
        assert isinstance(result, TypeExpectationObservedValueResult)
        assert result.observed_value == "int64"

    @pytest.mark.unit
    def test_override_with_spark_engine_hint(self):
        """A bare observed_value payload types to the same override under a spark hint."""
        result_dict = {"observed_value": "LongType"}
        result = as_typed(
            result_dict,
            expectation_type="expect_column_values_to_be_of_type",
            result_format=ResultFormat.COMPLETE,
            engine_hint="spark",
        )
        assert isinstance(result, TypeExpectationObservedValueResult)
        assert result.observed_value == "LongType"

    @pytest.mark.unit
    def test_override_sql_engine_hint_direct(self):
        """The override applies at any requested format once the shape matches."""
        result_dict = {"observed_value": "int64"}
        result = as_typed(
            result_dict,
            expectation_type="expect_column_values_to_be_of_type",
            result_format=ResultFormat.COMPLETE,
            engine_hint="sql",
        )
        assert isinstance(result, TypeExpectationObservedValueResult)
        assert result.observed_value == "int64"

    @pytest.mark.unit
    def test_no_override_when_the_shape_is_the_full_map_field_set(self):
        """A map-shaped payload falls through to family dispatch, hint or no hint.

        This is pandas' object-dtype map path: the result dict carries the full
        map field set (element_count, unexpected_count, ...), not a bare
        observed_value, so the shape predicate must not match it even though the
        expectation is one of the two the override table names.
        """
        result_dict = MAP_BASIC_DICT
        result = as_typed(
            result_dict,
            expectation_type="expect_column_values_to_be_of_type",
            result_format=ResultFormat.BASIC,
            engine_hint=None,
        )
        assert isinstance(result, MapBasicResult)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_values_to_be_of_type",
            "expect_column_values_to_be_in_type_list",
        ],
    )
    @pytest.mark.parametrize("engine_hint", ["pandas", "spark", "sql", None])
    def test_bare_observed_value_types_the_same_under_every_engine_hint(
        self, expectation_type: str, engine_hint
    ):
        """The same bare payload must not type differently depending on the engine.

        Before this predicate was shape-based, a pandas or unhinted call fell
        through to family dispatch (MapBasicResult) while a sql/spark call hit
        the override -- two different classes for the identical dict.
        """
        result = as_typed(
            {"observed_value": "INTEGER"},
            expectation_type=expectation_type,
            result_format=ResultFormat.BASIC,
            engine_hint=engine_hint,
        )
        assert isinstance(result, TypeExpectationObservedValueResult)
        assert result.observed_value == "INTEGER"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_values_to_be_of_type",
            "expect_column_values_to_be_in_type_list",
        ],
    )
    @pytest.mark.parametrize("engine_hint", ["pandas", "spark", "sql", None])
    def test_map_shaped_payload_types_to_the_map_family_under_every_engine_hint(
        self, expectation_type: str, engine_hint
    ):
        """A full map-shaped dict for either expectation is never routed to the override."""
        result = as_typed(
            MAP_BASIC_DICT,
            expectation_type=expectation_type,
            result_format=ResultFormat.BASIC,
            engine_hint=engine_hint,
        )
        assert isinstance(result, MapBasicResult)


# ---------------------------------------------------------------------------
# ParseError — raised with diagnostic message
# ---------------------------------------------------------------------------


class TestParseError:
    @pytest.mark.unit
    def test_parse_error_raised_on_bad_dict(self):
        """A result_dict with extra fields not accepted by the schema → ParseError."""
        bad_dict = {"totally_unknown_field": "bad_value", "another_bad": 999}
        with pytest.raises(ParseError) as exc_info:
            as_typed(
                bad_dict,
                expectation_type=MAP_EXPECTATION,
                result_format=ResultFormat.BOOLEAN_ONLY,
            )
        msg = str(exc_info.value)
        assert "MapBooleanOnlyResult" in msg or "expect_column_values_to_be_between" in msg

    @pytest.mark.unit
    def test_parse_error_raised_for_override_on_bad_dict(self):
        """Override path raises ParseError when schema rejects extra/missing fields."""
        # TypeExpectationObservedValueResult has extra=forbid.
        # An extra field not on the model will trigger validation error.
        bad_dict = {"observed_value": "int64", "unexpected_extra_field": "boom"}
        with pytest.raises(ParseError) as exc_info:
            as_typed(
                bad_dict,
                expectation_type="expect_column_values_to_be_of_type",
                result_format=ResultFormat.SUMMARY,
                engine_hint="sql",
            )
        msg = str(exc_info.value)
        assert "expect_column_values_to_be_of_type" in msg

    @pytest.mark.unit
    def test_parse_error_wraps_validation_error(self):
        """ParseError.__cause__ is a pydantic.ValidationError."""
        from great_expectations.compatibility import pydantic

        bad_dict = {"bad_field": "unexpected"}
        with pytest.raises(ParseError) as exc_info:
            as_typed(
                bad_dict,
                expectation_type=AGG_EXPECTATION,
                result_format=ResultFormat.BOOLEAN_ONLY,
            )
        assert isinstance(exc_info.value.__cause__, pydantic.ValidationError)
