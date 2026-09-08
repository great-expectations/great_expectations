"""Unit tests for ExpectationValidationResult.as_typed().

Covers:
- Returns the correct typed model for map/aggregate expectations, including for
  EVRs produced by a real ``Batch.validate(...)`` at each ResultFormat.
- Does not mutate the EVR in any way.
- EVR equality is preserved before and after calling as_typed().
- No new attributes appear in vars(evr) after the call.
- Missing expectation_config surfaces as a ParseError rather than a wrong guess.
- result_format can be given explicitly, or recovered from the result dict, or
  taken from the configured value in expectation kwargs.

All tests are marked @pytest.mark.unit and run via:
    pytest tests/unit/core/validation_result_schemas/test_as_typed.py -m unit -v
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Dict, Optional

import pandas as pd
import pytest

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.result_format import ResultFormat
from great_expectations.core.validation_result_schemas.dispatcher import ParseError
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
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

MAP_BASIC_RESULT = {
    "element_count": 100,
    "unexpected_count": 0,
    "unexpected_percent": 0.0,
    "missing_count": 0,
    "missing_percent": 0.0,
    "unexpected_percent_total": 0.0,
    "unexpected_percent_nonmissing": 0.0,
    "partial_unexpected_list": [],
}

MAP_SUMMARY_RESULT = {
    **MAP_BASIC_RESULT,
    "partial_unexpected_counts": [],
    "partial_unexpected_index_list": [],
}

MAP_COMPLETE_RESULT = {
    **MAP_SUMMARY_RESULT,
    "unexpected_list": [],
    "unexpected_index_list": [],
}

AGG_BASIC_RESULT = {
    "observed_value": 42.0,
}

AGG_SUMMARY_RESULT = {
    "observed_value": 42.0,
}

AGG_COMPLETE_RESULT = {
    "observed_value": 42.0,
    "unexpected_list": None,
    "unexpected_index_list": None,
}


def build_map_evr(
    result_format: str = "BASIC", result: Optional[dict] = None
) -> ExpectationValidationResult:
    """Build a map-family EVR (expect_column_values_to_not_be_null)."""
    config = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "col_a", "result_format": result_format},
    )
    return ExpectationValidationResult(
        success=True,
        expectation_config=config,
        result=result if result is not None else dict(MAP_BASIC_RESULT),
    )


def build_agg_evr(result_format: str = "BASIC") -> ExpectationValidationResult:
    """Build an aggregate-family EVR (expect_column_mean_to_be_between)."""
    config = ExpectationConfiguration(
        type="expect_column_mean_to_be_between",
        kwargs={"column": "col_a", "min_value": 0, "result_format": result_format},
    )
    return ExpectationValidationResult(
        success=True,
        expectation_config=config,
        result=dict(AGG_BASIC_RESULT),
    )


# ---------------------------------------------------------------------------
# Real EVRs, produced by an actual validate() at every ResultFormat
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pandas_batch() -> Batch:
    """A pandas batch whose validate() output exercises every result shape."""
    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_pandas("as_typed_pandas")
        .add_dataframe_asset("as_typed_asset")
        .add_batch_definition_whole_dataframe("as_typed_batch_definition")
    )
    dataframe = pd.DataFrame({"col_a": [1, 2, None, 4]})
    return batch_definition.get_batch(batch_parameters={"dataframe": dataframe})


@pytest.fixture(scope="module")
def map_evrs(pandas_batch: Batch) -> Dict[ResultFormat, ExpectationValidationResult]:
    """Real map-family EVRs, one per ResultFormat.

    ``result_format`` passed to validate() is not written back into
    ``expectation_config.kwargs``, so these EVRs are exactly the case that a
    kwargs-only lookup gets wrong.
    """
    return {
        result_format: pandas_batch.validate(
            gxe.ExpectColumnValuesToNotBeNull(column="col_a"),
            result_format=result_format,
        )
        for result_format in ResultFormat
    }


@pytest.fixture(scope="module")
def agg_evrs(pandas_batch: Batch) -> Dict[ResultFormat, ExpectationValidationResult]:
    """Real aggregate-family EVRs, one per ResultFormat."""
    return {
        result_format: pandas_batch.validate(
            gxe.ExpectColumnMeanToBeBetween(column="col_a", min_value=0, max_value=100),
            result_format=result_format,
        )
        for result_format in ResultFormat
    }


@pytest.fixture(scope="module")
def column_exist_complete_evr(pandas_batch: Batch) -> ExpectationValidationResult:
    """A real aggregate-family EVR whose result dict is empty at every ResultFormat.

    expect_column_to_exist reports only success/failure -- no result payload is
    ever rendered, at BOOLEAN_ONLY or otherwise. Validated at COMPLETE here so an
    empty-dict-implies-BOOLEAN_ONLY inference would misreport the format this EVR
    was actually rendered at.
    """
    return pandas_batch.validate(
        gxe.ExpectColumnToExist(column="col_a"),
        result_format=ResultFormat.COMPLETE,
    )


class TestRealValidateResults:
    """as_typed() with no arguments types results produced by an actual validate()."""

    @pytest.mark.unit
    def test_result_format_is_absent_from_kwargs(
        self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """The premise of the shape-based inference: kwargs does not record the format.

        If this ever stops holding, the inference is still correct but no longer
        load-bearing, and these tests would pass for the wrong reason.
        """
        for result_format, evr in map_evrs.items():
            assert evr.expectation_config is not None
            assert "result_format" not in evr.expectation_config.kwargs, (
                f"{result_format.value} unexpectedly persisted result_format into kwargs"
            )

    @pytest.mark.unit
    def test_map_boolean_only(self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]):
        """A real BOOLEAN_ONLY map result is an empty dict -- genuinely ambiguous,
        since some map expectations also render an empty dict at other formats.
        result_format is not persisted into kwargs by validate(), so nothing here
        can recover BOOLEAN_ONLY specifically; the most permissive variant is
        returned instead, and it parses the empty dict without complaint.
        """
        typed = map_evrs[ResultFormat.BOOLEAN_ONLY].as_typed()
        assert isinstance(typed, MapCompleteResult)

    @pytest.mark.unit
    def test_map_boolean_only_explicit_format(
        self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """Passing the format the EVR was actually rendered at still recovers the
        narrow variant -- the ambiguity above is about *inference*, not parsing.
        """
        typed = map_evrs[ResultFormat.BOOLEAN_ONLY].as_typed(
            result_format=ResultFormat.BOOLEAN_ONLY
        )
        assert isinstance(typed, MapBooleanOnlyResult)

    @pytest.mark.unit
    def test_map_basic(self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]):
        typed = map_evrs[ResultFormat.BASIC].as_typed()
        assert isinstance(typed, MapBasicResult)
        assert not isinstance(typed, MapSummaryResult)

    @pytest.mark.unit
    def test_map_summary(self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]):
        typed = map_evrs[ResultFormat.SUMMARY].as_typed()
        assert isinstance(typed, MapSummaryResult)
        assert not isinstance(typed, MapCompleteResult)

    @pytest.mark.unit
    def test_map_complete(self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]):
        """The COMPLETE result of a validate() is the case a kwargs lookup rejects.

        kwargs carries no result_format, so a kwargs-only lookup defaults to
        SUMMARY, and MapSummaryResult forbids the unexpected_list and
        unexpected_index_list this dict carries.
        """
        typed = map_evrs[ResultFormat.COMPLETE].as_typed()
        assert isinstance(typed, MapCompleteResult)

    @pytest.mark.unit
    def test_map_complete_result_carries_a_pandas_index_query(
        self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """unexpected_index_query is emitted by pandas, so it cannot imply SQL."""
        typed = map_evrs[ResultFormat.COMPLETE].as_typed()
        assert isinstance(typed, MapCompleteResult)
        assert typed.unexpected_index_query is not None
        assert typed.unexpected_index_query.startswith("df.filter(")
        assert typed.engine_hint is None

    @pytest.mark.unit
    def test_map_values_round_trip_unchanged(
        self, map_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """Every value the typed view reports is the value the EVR holds."""
        evr = map_evrs[ResultFormat.COMPLETE]
        typed = evr.as_typed()
        for key, value in (evr.result or {}).items():
            typed_value = getattr(typed, key)
            assert type(typed_value) is type(value), f"{key}: {type(typed_value)} != {type(value)}"

    @pytest.mark.unit
    def test_aggregate_boolean_only(
        self, agg_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """As with the map family: a real, empty BOOLEAN_ONLY result cannot be
        told apart from any other format that also renders empty, so the widest
        variant is returned rather than a guess that happens to be narrowest.
        """
        typed = agg_evrs[ResultFormat.BOOLEAN_ONLY].as_typed()
        assert isinstance(typed, AggregateCompleteResult)

    @pytest.mark.unit
    def test_aggregate_boolean_only_explicit_format(
        self, agg_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        typed = agg_evrs[ResultFormat.BOOLEAN_ONLY].as_typed(
            result_format=ResultFormat.BOOLEAN_ONLY
        )
        assert isinstance(typed, AggregateBooleanOnlyResult)

    @pytest.mark.unit
    def test_empty_result_at_complete_is_not_boolean_only(
        self, column_exist_complete_evr: ExpectationValidationResult
    ):
        """expect_column_to_exist renders {} at COMPLETE just as it does at
        BOOLEAN_ONLY. Without an empty-dict-implies-BOOLEAN_ONLY special case,
        as_typed() with no arguments falls through to the widest aggregate
        variant instead of mislabeling this a boolean-only result.
        """
        typed = column_exist_complete_evr.as_typed()
        assert isinstance(typed, AggregateCompleteResult)
        assert not isinstance(typed, AggregateBooleanOnlyResult)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "result_format",
        [ResultFormat.BASIC, ResultFormat.SUMMARY, ResultFormat.COMPLETE],
    )
    def test_aggregate_non_boolean_formats(
        self,
        agg_evrs: Dict[ResultFormat, ExpectationValidationResult],
        result_format: ResultFormat,
    ):
        """Aggregate BASIC, SUMMARY, and COMPLETE payloads are indistinguishable.

        All three are ``{"observed_value": ...}``, so the shape names no format and
        the most permissive variant of the family is used — it accepts all three.
        """
        typed = agg_evrs[result_format].as_typed()
        assert isinstance(typed, AggregateBasicResult)

    @pytest.mark.unit
    def test_aggregate_observed_value_is_not_coerced(
        self, agg_evrs: Dict[ResultFormat, ExpectationValidationResult]
    ):
        """expect_column_mean_to_be_between observes a numpy float; it must survive."""
        evr = agg_evrs[ResultFormat.SUMMARY]
        typed = evr.as_typed()
        assert isinstance(typed, AggregateBasicResult)
        raw = (evr.result or {})["observed_value"]
        assert type(typed.observed_value) is type(raw)
        assert typed.observed_value == raw


# ---------------------------------------------------------------------------
# Return type checks — map family
# ---------------------------------------------------------------------------


class TestMapFamilyReturnTypes:
    """as_typed returns the correct map-family model class for each ResultFormat."""

    @pytest.mark.unit
    def test_map_boolean_only(self):
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "col_a", "result_format": "BOOLEAN_ONLY"},
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result={},
        )
        typed = evr.as_typed()
        assert isinstance(typed, MapBooleanOnlyResult)

    @pytest.mark.unit
    def test_map_basic(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        typed = evr.as_typed()
        assert isinstance(typed, MapBasicResult)

    @pytest.mark.unit
    def test_map_summary(self):
        evr = build_map_evr(result_format="SUMMARY", result=dict(MAP_SUMMARY_RESULT))
        typed = evr.as_typed()
        assert isinstance(typed, MapSummaryResult)

    @pytest.mark.unit
    def test_map_complete(self):
        evr = build_map_evr(result_format="COMPLETE", result=dict(MAP_COMPLETE_RESULT))
        typed = evr.as_typed()
        assert isinstance(typed, MapCompleteResult)


# ---------------------------------------------------------------------------
# Return type checks — aggregate family
# ---------------------------------------------------------------------------


class TestAggregateFamilyReturnTypes:
    """as_typed returns the correct aggregate-family model class for each ResultFormat."""

    @pytest.mark.unit
    def test_aggregate_boolean_only(self):
        config = ExpectationConfiguration(
            type="expect_column_mean_to_be_between",
            kwargs={"column": "col_a", "result_format": "BOOLEAN_ONLY"},
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result={},
        )
        typed = evr.as_typed()
        assert isinstance(typed, AggregateBooleanOnlyResult)

    @pytest.mark.unit
    def test_aggregate_basic(self):
        evr = build_agg_evr(result_format="BASIC")
        typed = evr.as_typed()
        assert isinstance(typed, AggregateBasicResult)
        assert not isinstance(typed, AggregateSummaryResult)

    @pytest.mark.unit
    def test_aggregate_summary(self):
        evr = build_agg_evr(result_format="SUMMARY")
        typed = evr.as_typed()
        assert isinstance(typed, AggregateSummaryResult)

    @pytest.mark.unit
    def test_aggregate_complete(self):
        config = ExpectationConfiguration(
            type="expect_column_mean_to_be_between",
            kwargs={"column": "col_a", "result_format": "COMPLETE"},
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result=dict(AGG_COMPLETE_RESULT),
        )
        typed = evr.as_typed()
        assert isinstance(typed, AggregateCompleteResult)


# ---------------------------------------------------------------------------
# No mutation
# ---------------------------------------------------------------------------


class TestNoMutation:
    """as_typed must not mutate self in any way."""

    @pytest.mark.unit
    def test_result_dict_not_mutated(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        before_result = dict(evr.result)
        _ = evr.as_typed()
        assert dict(evr.result) == before_result

    @pytest.mark.unit
    def test_to_json_dict_identical_after_call(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        before_dict = json.dumps(evr.to_json_dict(), sort_keys=True)
        _ = evr.as_typed()
        assert json.dumps(evr.to_json_dict(), sort_keys=True) == before_dict

    @pytest.mark.unit
    def test_no_new_attributes(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        before_vars = set(vars(evr).keys())
        _ = evr.as_typed()
        assert set(vars(evr).keys()) == before_vars


# ---------------------------------------------------------------------------
# EVR equality preserved
# ---------------------------------------------------------------------------


class TestEqualityPreserved:
    """as_typed must not affect EVR equality."""

    @pytest.mark.unit
    def test_equality_before_and_after_as_typed(self):
        evr1 = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        evr2 = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        assert evr1 == evr2
        _ = evr1.as_typed()
        assert evr1 == evr2

    @pytest.mark.unit
    def test_to_json_dict_byte_identical_pair(self):
        evr1 = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        evr2 = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        assert json.dumps(evr1.to_json_dict(), sort_keys=True) == json.dumps(
            evr2.to_json_dict(), sort_keys=True
        )
        _ = evr1.as_typed()
        assert json.dumps(evr1.to_json_dict(), sort_keys=True) == json.dumps(
            evr2.to_json_dict(), sort_keys=True
        )


# ---------------------------------------------------------------------------
# Missing expectation_config
# ---------------------------------------------------------------------------


class TestMissingConfig:
    """Without an expectation_config there is no type, and so no derivable family."""

    @pytest.mark.unit
    def test_none_config_raises_parse_error_naming_the_missing_config(self):
        """The message names the absent config rather than a registry miss.

        Forwarding a synthetic expectation type to the dispatcher instead surfaced as "no such
        expectation is registered.  Register the expectation", which sends the caller to the
        registry to add an expectation they never named.  The cause is the missing config, and
        the message has to say so for the caller to act on it.
        """
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=None,
            result={},
        )
        with pytest.raises(ParseError) as exc_info:
            evr.as_typed()
        message = str(exc_info.value)
        assert "expectation_config" in message
        assert "registered" not in message

    @pytest.mark.unit
    def test_none_config_no_mutation(self):
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=None,
            result={},
        )
        before_vars = set(vars(evr).keys())
        with pytest.raises(ParseError):
            evr.as_typed()
        assert set(vars(evr).keys()) == before_vars


# ---------------------------------------------------------------------------
# result_format resolution: explicit argument, result shape, configured value
# ---------------------------------------------------------------------------


class TestResultFormatResolution:
    """The explicit argument wins, then the result shape, then expectation kwargs."""

    @pytest.mark.unit
    def test_explicit_result_format_argument(self):
        """An explicit request is authoritative even when the shape disagrees."""
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_COMPLETE_RESULT))
        with pytest.raises(ParseError):
            evr.as_typed(result_format=ResultFormat.BASIC)

    @pytest.mark.unit
    def test_explicit_result_format_accepts_a_string(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_COMPLETE_RESULT))
        typed = evr.as_typed(result_format="COMPLETE")
        assert isinstance(typed, MapCompleteResult)

    @pytest.mark.unit
    def test_shape_wins_over_configured_kwargs(self):
        """kwargs says SUMMARY; the dict is COMPLETE-shaped and must still parse."""
        evr = build_map_evr(result_format="SUMMARY", result=dict(MAP_COMPLETE_RESULT))
        typed = evr.as_typed()
        assert isinstance(typed, MapCompleteResult)

    @pytest.mark.unit
    def test_string_result_format_in_kwargs(self):
        """result_format stored as plain string in kwargs."""
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        typed = evr.as_typed()
        assert isinstance(typed, MapBasicResult)

    @pytest.mark.unit
    def test_enum_result_format_in_kwargs(self):
        """result_format stored as ResultFormat enum in kwargs."""
        config = ExpectationConfiguration(
            type="expect_column_mean_to_be_between",
            kwargs={"column": "col_a", "result_format": ResultFormat.BASIC},
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result=dict(AGG_BASIC_RESULT),
        )
        typed = evr.as_typed()
        assert isinstance(typed, AggregateBasicResult)
        assert not isinstance(typed, AggregateSummaryResult)

    @pytest.mark.unit
    def test_dict_result_format_in_kwargs(self):
        """result_format stored as a parsed config dict in kwargs."""
        config = ExpectationConfiguration(
            type="expect_column_mean_to_be_between",
            kwargs={
                "column": "col_a",
                "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20},
            },
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result=dict(AGG_BASIC_RESULT),
        )
        typed = evr.as_typed()
        assert isinstance(typed, AggregateBasicResult)
        assert not isinstance(typed, AggregateSummaryResult)

    @pytest.mark.unit
    def test_no_result_format_anywhere_uses_the_shape(self):
        """kwargs carries no result_format, so the result dict decides."""
        config = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "col_a"},  # no result_format
        )
        evr = ExpectationValidationResult(
            success=True,
            expectation_config=config,
            result=dict(MAP_SUMMARY_RESULT),
        )
        typed = evr.as_typed()
        assert isinstance(typed, MapSummaryResult)
        assert not isinstance(typed, MapCompleteResult)


# ---------------------------------------------------------------------------
# engine_hint passthrough
# ---------------------------------------------------------------------------


class TestEngineHintPassthrough:
    """engine_hint is forwarded to the dispatcher without mutating the EVR."""

    @pytest.mark.unit
    def test_engine_hint_pandas_map_basic(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        typed = evr.as_typed(engine_hint="pandas")
        assert isinstance(typed, MapBasicResult)
        assert typed.engine_hint == "pandas"

    @pytest.mark.unit
    def test_engine_hint_does_not_mutate_evr(self):
        evr = build_map_evr(result_format="BASIC", result=dict(MAP_BASIC_RESULT))
        before_vars = set(vars(evr).keys())
        before_result = dict(evr.result)
        _ = evr.as_typed(engine_hint="pandas")
        assert set(vars(evr).keys()) == before_vars
        assert dict(evr.result) == before_result
