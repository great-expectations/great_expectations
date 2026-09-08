"""The populated gold-tier case table: `GOLD_CASES` and its derived key set.

This module holds the one thing `gold_expectation_cases.py` cannot: real, constructed
`Expectation` instances. Building those requires importing `great_expectations`, and
`gold_expectation_cases.py` must stay importable with no data-source dependency installed at all
(see that module's docstring). This module is the layer in between -- it imports
`great_expectations`, imports the pure data and record shape from `gold_expectation_cases.py`, and
publishes the populated table. `test_gold_expectation_suite.py` consumes `GOLD_CASES` from here; it
does not define cases itself.

Keeping the case table in its own module, rather than inline in the test module, matters at this
size: with one case per gallery expectation, collapsing that growth into the same file as the
collection-time builder and the case-consuming test functions would make the test module
unreadably long.
"""

from __future__ import annotations

from datetime import date
from typing import Final, FrozenSet, List, Sequence, Set, Tuple

import great_expectations.expectations as gxe
from tests.integration.data_sources_and_expectations.gold_expectation_cases import (
    EXTRA_TABLE_SELF_REFERENCE,
    GOLD_EXTRA_TABLE_NAME,
    GOLD_FIXTURE_DATA,
    CaseFixtureShape,
    GoldCase,
)
from tests.integration.test_utils.execution_engine_kind import ExecutionEngineKind

_SQL_ONLY: Final[FrozenSet[ExecutionEngineKind]] = frozenset({ExecutionEngineKind.SQL})
_SQL_ONLY_REASON: Final[str] = (
    "the extra-table and comparison-source harness fixtures both assert a SQL batch setup"
)

_PANDAS_ONLY: Final[FrozenSet[ExecutionEngineKind]] = frozenset({ExecutionEngineKind.PANDAS})

_PANDAS_AND_SPARK: Final[FrozenSet[ExecutionEngineKind]] = frozenset(
    {ExecutionEngineKind.PANDAS, ExecutionEngineKind.SPARK}
)
_NO_SQL_PROVIDER_REASON: Final[str] = (
    "the shipped package registers no SQL metric provider for this expectation"
)
_PANDAS_ONLY_PROVIDER_REASON: Final[str] = (
    "the shipped package registers a metric provider for this expectation on the pandas engine "
    "only, with neither a SQL nor a Spark provider"
)

_SQL_AND_SPARK: Final[FrozenSet[ExecutionEngineKind]] = frozenset(
    {ExecutionEngineKind.SQL, ExecutionEngineKind.SPARK}
)
_NO_PANDAS_PROVIDER_REASON: Final[str] = (
    "the shipped package registers a metric provider for this expectation on the SQL and Spark "
    "engines only; it runs raw SQL/Spark-SQL queries as its own validation logic and has no "
    "pandas provider"
)

_FIXTURE_COLUMNS: Final[Tuple[str, ...]] = tuple(GOLD_FIXTURE_DATA.columns)
"""Derived from the shared frame itself rather than hand-listed, so the table-shape cases below
cannot drift from the frame they describe."""

GOLD_CASES: Final[Tuple[GoldCase, ...]] = (
    GoldCase(
        key=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key").expectation_type,
        passing=gxe.ExpectColumnValuesToNotBeNull(column="increasing_key"),
        failing=gxe.ExpectColumnValuesToNotBeNull(column="nullable_value"),
        fixture_shape=CaseFixtureShape.STANDARD,
    ),
    GoldCase(
        key=gxe.ExpectTableRowCountToEqualOtherTable(
            other_table_name=EXTRA_TABLE_SELF_REFERENCE
        ).expectation_type,
        # Passing: the primary table compared against itself, resolved at test time -- always
        # equal, and never mistaken for "no comparison happened" since the table is real.
        passing=gxe.ExpectTableRowCountToEqualOtherTable(
            other_table_name=EXTRA_TABLE_SELF_REFERENCE
        ),
        # Failing: the primary table (six rows) compared against the shared extra table (seven
        # rows) -- a real, known mismatch.
        failing=gxe.ExpectTableRowCountToEqualOtherTable(other_table_name=GOLD_EXTRA_TABLE_NAME),
        fixture_shape=CaseFixtureShape.EXTRA_TABLE,
        engines=_SQL_ONLY,
        engine_restriction_reason=_SQL_ONLY_REASON,
    ),
    GoldCase(
        key=gxe.ExpectQueryResultsToMatchComparison(
            base_query="SELECT increasing_key FROM {batch} ORDER BY increasing_key",
            comparison_data_source_name=EXTRA_TABLE_SELF_REFERENCE,
            comparison_query="SELECT increasing_key FROM {source_table} ORDER BY increasing_key",
        ).expectation_type,
        # Passing: base and comparison queries select the identical column from the identical
        # (self-paired) data, ordered the same way -- every row matches.
        passing=gxe.ExpectQueryResultsToMatchComparison(
            base_query="SELECT increasing_key FROM {batch} ORDER BY increasing_key",
            comparison_data_source_name=EXTRA_TABLE_SELF_REFERENCE,
            comparison_query="SELECT increasing_key FROM {source_table} ORDER BY increasing_key",
        ),
        # Failing: the comparison query selects a strict subset of the base query's rows, so the
        # match percentage falls below the (default) mostly threshold.
        failing=gxe.ExpectQueryResultsToMatchComparison(
            base_query="SELECT increasing_key FROM {batch} ORDER BY increasing_key",
            comparison_data_source_name=EXTRA_TABLE_SELF_REFERENCE,
            comparison_query=(
                "SELECT increasing_key FROM {source_table} WHERE increasing_key < 3 "
                "ORDER BY increasing_key"
            ),
        ),
        fixture_shape=CaseFixtureShape.COMPARISON,
        engines=_SQL_ONLY,
        engine_restriction_reason=_SQL_ONLY_REASON,
    ),
    # ----------------------------------------------------------------------------------------
    # Column-map expectations
    # ----------------------------------------------------------------------------------------
    GoldCase(
        key=gxe.ExpectColumnValueLengthsToBeBetween(
            column="pattern_code", min_value=4, max_value=4
        ).expectation_type,
        # Passing: every `pattern_code` value is exactly four characters ("A100" .. "A600").
        passing=gxe.ExpectColumnValueLengthsToBeBetween(
            column="pattern_code", min_value=4, max_value=4
        ),
        # Failing: no value is five characters long.
        failing=gxe.ExpectColumnValueLengthsToBeBetween(
            column="pattern_code", min_value=5, max_value=10
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValueLengthsToEqual(column="pattern_code", value=4).expectation_type,
        passing=gxe.ExpectColumnValueLengthsToEqual(column="pattern_code", value=4),
        failing=gxe.ExpectColumnValueLengthsToEqual(column="pattern_code", value=5),
    ),
    GoldCase(
        key=gxe.ExpectColumnValueZScoresToBeLessThan(
            column="float_value", threshold=2.0, double_sided=True
        ).expectation_type,
        # `float_value` is an evenly-spaced sequence, so every z-score sits well under 2.
        passing=gxe.ExpectColumnValueZScoresToBeLessThan(
            column="float_value", threshold=2.0, double_sided=True
        ),
        # A threshold this small excludes every non-zero z-score.
        failing=gxe.ExpectColumnValueZScoresToBeLessThan(
            column="float_value", threshold=0.01, double_sided=True
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeBetween(
            column="float_value", min_value=10.0, max_value=22.5
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToBeBetween(
            column="float_value", min_value=10.0, max_value=22.5
        ),
        # The column's max (22.5) exceeds this upper bound.
        failing=gxe.ExpectColumnValuesToBeBetween(
            column="float_value", min_value=10.0, max_value=20.0
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeDateutilParseable(column="strftime_code").expectation_type,
        # `strftime_code` values ("2024-01-01" ...) are dateutil-parseable dates.
        passing=gxe.ExpectColumnValuesToBeDateutilParseable(column="strftime_code"),
        # `category` values ("red", "green", "blue") are not parseable as dates.
        failing=gxe.ExpectColumnValuesToBeDateutilParseable(column="category"),
        engines=_PANDAS_ONLY,
        engine_restriction_reason=_PANDAS_ONLY_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeDecreasing(column="decreasing_value").expectation_type,
        passing=gxe.ExpectColumnValuesToBeDecreasing(column="decreasing_value"),
        # `increasing_key` is strictly increasing, not decreasing.
        failing=gxe.ExpectColumnValuesToBeDecreasing(column="increasing_key"),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeInSet(
            column="category", value_set=["red", "green", "blue"]
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToBeInSet(
            column="category", value_set=["red", "green", "blue"]
        ),
        # `category` also holds "green" and "blue", outside this narrower set.
        failing=gxe.ExpectColumnValuesToBeInSet(column="category", value_set=["red"]),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeInTypeList(
            column="increasing_key", type_list=["INTEGER", "BIGINT", "SMALLINT"]
        ).expectation_type,
        # `increasing_key` is an integer column under every SQL backend this table runs against.
        passing=gxe.ExpectColumnValuesToBeInTypeList(
            column="increasing_key", type_list=["INTEGER", "BIGINT", "SMALLINT"]
        ),
        # No backend registers an integer column under a character type name.
        failing=gxe.ExpectColumnValuesToBeInTypeList(
            column="increasing_key", type_list=["VARCHAR"]
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason=(
            "the type name vocabulary this case checks against is SQL dialect type names, which "
            "have no meaning on a non-SQL engine"
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeIncreasing(column="increasing_key").expectation_type,
        passing=gxe.ExpectColumnValuesToBeIncreasing(column="increasing_key"),
        # `decreasing_value` is strictly decreasing, not increasing.
        failing=gxe.ExpectColumnValuesToBeIncreasing(column="decreasing_value"),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeJsonParseable(column="json_payload").expectation_type,
        passing=gxe.ExpectColumnValuesToBeJsonParseable(column="json_payload"),
        # `category` values are plain strings, not JSON documents.
        failing=gxe.ExpectColumnValuesToBeJsonParseable(column="category"),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeNull(column="nullable_value", mostly=0.3).expectation_type,
        # Two of six `nullable_value` rows are null (0.333...), clearing a 0.3 `mostly` floor.
        passing=gxe.ExpectColumnValuesToBeNull(column="nullable_value", mostly=0.3),
        # The default `mostly` (1.0) demands every row be null; four of six are not.
        failing=gxe.ExpectColumnValuesToBeNull(column="nullable_value"),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeOfType(
            column="increasing_key", type_="INTEGER"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToBeOfType(column="increasing_key", type_="INTEGER"),
        failing=gxe.ExpectColumnValuesToBeOfType(column="increasing_key", type_="VARCHAR"),
        engines=_SQL_ONLY,
        engine_restriction_reason=(
            "the type name this case checks against is a SQL dialect type name, which has no "
            "meaning on a non-SQL engine"
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToBeUnique(column="increasing_key").expectation_type,
        passing=gxe.ExpectColumnValuesToBeUnique(column="increasing_key"),
        # `category` repeats "red", "green", and "blue" across six rows.
        failing=gxe.ExpectColumnValuesToBeUnique(column="category"),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchJsonSchema(
            column="json_payload",
            json_schema={
                "type": "object",
                "properties": {"a": {"type": "number"}},
                "required": ["a"],
            },
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToMatchJsonSchema(
            column="json_payload",
            json_schema={
                "type": "object",
                "properties": {"a": {"type": "number"}},
                "required": ["a"],
            },
        ),
        # `a` is always a number, never a string.
        failing=gxe.ExpectColumnValuesToMatchJsonSchema(
            column="json_payload",
            json_schema={
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": ["a"],
            },
        ),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchLikePattern(
            column="pattern_code", like_pattern="A%"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToMatchLikePattern(column="pattern_code", like_pattern="A%"),
        # No `pattern_code` value starts with "B".
        failing=gxe.ExpectColumnValuesToMatchLikePattern(column="pattern_code", like_pattern="B%"),
        engines=_SQL_ONLY,
        engine_restriction_reason="a SQL `LIKE` pattern has no meaning on a non-SQL engine",
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchLikePatternList(
            column="pattern_code",
            like_pattern_list=["A1%", "A2%", "A3%", "A4%", "A5%", "A6%"],
            match_on="any",
        ).expectation_type,
        # Each row matches exactly one of these six patterns.
        passing=gxe.ExpectColumnValuesToMatchLikePatternList(
            column="pattern_code",
            like_pattern_list=["A1%", "A2%", "A3%", "A4%", "A5%", "A6%"],
            match_on="any",
        ),
        # No row starts with "B".
        failing=gxe.ExpectColumnValuesToMatchLikePatternList(
            column="pattern_code", like_pattern_list=["B%"], match_on="any"
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason="a SQL `LIKE` pattern has no meaning on a non-SQL engine",
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchRegex(
            column="pattern_code", regex=r"^A[0-9]{3}$"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToMatchRegex(column="pattern_code", regex=r"^A[0-9]{3}$"),
        # No `pattern_code` value starts with "B".
        failing=gxe.ExpectColumnValuesToMatchRegex(column="pattern_code", regex=r"^B"),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchRegexList(
            column="pattern_code", regex_list=[r"^A", r"[0-9]{3}$"], match_on="all"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToMatchRegexList(
            column="pattern_code", regex_list=[r"^A", r"[0-9]{3}$"], match_on="all"
        ),
        # No value matches a leading "B".
        failing=gxe.ExpectColumnValuesToMatchRegexList(
            column="pattern_code", regex_list=[r"^B"], match_on="all"
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToMatchStrftimeFormat(
            column="strftime_code", strftime_format="%Y-%m-%d"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToMatchStrftimeFormat(
            column="strftime_code", strftime_format="%Y-%m-%d"
        ),
        # `strftime_code` values are not slash-delimited.
        failing=gxe.ExpectColumnValuesToMatchStrftimeFormat(
            column="strftime_code", strftime_format="%m/%d/%Y"
        ),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotBeInSet(
            column="category", value_set=["purple"]
        ).expectation_type,
        # `category` never holds "purple".
        passing=gxe.ExpectColumnValuesToNotBeInSet(column="category", value_set=["purple"]),
        # `category` does hold "red".
        failing=gxe.ExpectColumnValuesToNotBeInSet(column="category", value_set=["red"]),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotBeOutliers(
            column="float_value", method="std", multiplier=2.0
        ).expectation_type,
        # `float_value` is an evenly-spaced sequence with no point more than two standard
        # deviations from the mean.
        passing=gxe.ExpectColumnValuesToNotBeOutliers(
            column="float_value", method="std", multiplier=2.0
        ),
        # A near-zero multiplier admits only values equal to the mean, flagging the rest.
        failing=gxe.ExpectColumnValuesToNotBeOutliers(
            column="float_value", method="std", multiplier=0.01
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotMatchLikePattern(
            column="pattern_code", like_pattern="B%"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToNotMatchLikePattern(
            column="pattern_code", like_pattern="B%"
        ),
        # Every `pattern_code` value starts with "A".
        failing=gxe.ExpectColumnValuesToNotMatchLikePattern(
            column="pattern_code", like_pattern="A%"
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason="a SQL `LIKE` pattern has no meaning on a non-SQL engine",
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotMatchLikePatternList(
            column="pattern_code", like_pattern_list=["B%", "C%"]
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToNotMatchLikePatternList(
            column="pattern_code", like_pattern_list=["B%", "C%"]
        ),
        # Every value matches one of these six "A"-prefixed patterns.
        failing=gxe.ExpectColumnValuesToNotMatchLikePatternList(
            column="pattern_code",
            like_pattern_list=["A1%", "A2%", "A3%", "A4%", "A5%", "A6%"],
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason="a SQL `LIKE` pattern has no meaning on a non-SQL engine",
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotMatchRegex(
            column="pattern_code", regex=r"^B"
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToNotMatchRegex(column="pattern_code", regex=r"^B"),
        # Every `pattern_code` value starts with "A".
        failing=gxe.ExpectColumnValuesToNotMatchRegex(column="pattern_code", regex=r"^A"),
    ),
    GoldCase(
        key=gxe.ExpectColumnValuesToNotMatchRegexList(
            column="pattern_code", regex_list=[r"^B", r"^C"]
        ).expectation_type,
        passing=gxe.ExpectColumnValuesToNotMatchRegexList(
            column="pattern_code", regex_list=[r"^B", r"^C"]
        ),
        # Every `pattern_code` value starts with "A".
        failing=gxe.ExpectColumnValuesToNotMatchRegexList(
            column="pattern_code", regex_list=[r"^A"]
        ),
    ),
    # ----------------------------------------------------------------------------------------
    # Column-aggregate expectations
    #
    # Every metric these fifteen cases lean on (`column.min`, `column.max`, `column.mean`,
    # `column.median`, `column.standard_deviation`, `column.sum`, `column.unique_proportion`,
    # `column.value_counts`, `column.distinct_values`, `column.partition`) registers a provider on
    # every execution engine this table runs against, verified against
    # `great_expectations.expectations.registry._registered_metrics`. None of these cases needs an
    # engine restriction.
    # ----------------------------------------------------------------------------------------
    GoldCase(
        key=gxe.ExpectColumnMinToBeBetween(
            column="record_date", min_value=date(2024, 1, 1), max_value=date(2024, 1, 1)
        ).expectation_type,
        # `record_date`'s true minimum is 2024-01-01, exactly.
        passing=gxe.ExpectColumnMinToBeBetween(
            column="record_date", min_value=date(2024, 1, 1), max_value=date(2024, 1, 1)
        ),
        # A near miss: the lower bound sits one day past the true minimum.
        failing=gxe.ExpectColumnMinToBeBetween(
            column="record_date", min_value=date(2024, 1, 2), max_value=date(2024, 1, 10)
        ),
    ),
    GoldCase(
        # `record_timestamp` (tz-aware) is deliberately NOT used here.
        # `_validate_metric_value_between` (great_expectations/expectations/expectation.py) does a
        # raw Python comparison between the observed metric value and the declared bounds with no
        # tz-normalization: SQLite round-trips a tz-aware timestamp as tz-naive while pandas
        # preserves it as tz-aware, so a tz-aware bound crashes on SQLite (comparing naive against
        # aware) and a tz-naive bound crashes on pandas (comparing aware against naive) -- there is
        # no bound tz-awareness that survives both locally runnable engines for the same source
        # column. Recorded as an upstream defect rather than worked around by injecting a
        # differently-shaped fixture column into the shared frame every other STANDARD case also
        # uses.
        key=gxe.ExpectColumnMaxToBeBetween(
            column="float_value", min_value=22.4, max_value=22.6
        ).expectation_type,
        # `float_value`'s true maximum is exactly 22.5.
        passing=gxe.ExpectColumnMaxToBeBetween(
            column="float_value", min_value=22.4, max_value=22.6
        ),
        # A near miss: the lower bound sits just past the true maximum.
        failing=gxe.ExpectColumnMaxToBeBetween(
            column="float_value", min_value=22.51, max_value=23.0
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnMeanToBeBetween(
            column="float_value", min_value=16.2, max_value=16.3
        ).expectation_type,
        # `float_value`'s true mean is exactly 16.25.
        passing=gxe.ExpectColumnMeanToBeBetween(
            column="float_value", min_value=16.2, max_value=16.3
        ),
        # A near miss: the lower bound sits just past the true mean.
        failing=gxe.ExpectColumnMeanToBeBetween(
            column="float_value", min_value=16.26, max_value=16.5
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnMedianToBeBetween(
            column="float_value", min_value=16.2, max_value=16.3
        ).expectation_type,
        # `float_value`'s true median is exactly 16.25 (the mean of the two middle values).
        passing=gxe.ExpectColumnMedianToBeBetween(
            column="float_value", min_value=16.2, max_value=16.3
        ),
        # A near miss: the lower bound sits just past the true median.
        failing=gxe.ExpectColumnMedianToBeBetween(
            column="float_value", min_value=16.26, max_value=17.0
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnStdevToBeBetween(
            column="float_value", min_value=4.6, max_value=4.8
        ).expectation_type,
        # `float_value`'s true sample stdev is ~4.6771.
        passing=gxe.ExpectColumnStdevToBeBetween(
            column="float_value", min_value=4.6, max_value=4.8
        ),
        # A near miss: the lower bound sits just past the true stdev.
        failing=gxe.ExpectColumnStdevToBeBetween(
            column="float_value", min_value=4.68, max_value=5.0
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnSumToBeBetween(
            column="float_value", min_value=97.4, max_value=97.6
        ).expectation_type,
        # `float_value`'s true sum is exactly 97.5.
        passing=gxe.ExpectColumnSumToBeBetween(
            column="float_value", min_value=97.4, max_value=97.6
        ),
        # A near miss: the lower bound sits just past the true sum.
        failing=gxe.ExpectColumnSumToBeBetween(
            column="float_value", min_value=97.51, max_value=98.0
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="increasing_key", min_value=6, max_value=6
        ).expectation_type,
        # `increasing_key` has exactly six distinct values, one per row.
        passing=gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="increasing_key", min_value=6, max_value=6
        ),
        # A near miss: the lower bound sits one past the true unique count.
        failing=gxe.ExpectColumnUniqueValueCountToBeBetween(
            column="increasing_key", min_value=7, max_value=10
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column="increasing_key", min_value=0.99, max_value=1.0
        ).expectation_type,
        # `increasing_key`'s proportion of unique values is exactly 1.0 (every row distinct).
        passing=gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column="increasing_key", min_value=0.99, max_value=1.0
        ),
        # A near miss: the upper bound sits just short of the true proportion.
        failing=gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column="increasing_key", min_value=0.0, max_value=0.999
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column="nullable_value", min_value=0.66, max_value=0.67
        ).expectation_type,
        # `nullable_value` has four non-null rows out of six (0.6667).
        passing=gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column="nullable_value", min_value=0.66, max_value=0.67
        ),
        # A near miss: the lower bound sits just past the true proportion.
        failing=gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column="nullable_value", min_value=0.67, max_value=0.7
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnQuantileValuesToBeBetween(
            column="float_value",
            quantile_ranges={"quantiles": [0.5], "value_ranges": [[14.9, 17.6]]},
        ).expectation_type,
        # The median-quantile computation is not pinned to one interpolation method across
        # engines -- pandas resolves the 0.5 quantile of this six-row column to 15.0 (a
        # lower-value method) rather than to the 16.25 the plain median metric reports, and a
        # different backend's own interpolation choice could land anywhere between those two
        # real values. The range is widened to cover both rather than narrowed to a near miss,
        # so this case does not become a false failure on a backend whose quantile algorithm
        # differs from pandas's, while still excluding a badly wrong value.
        passing=gxe.ExpectColumnQuantileValuesToBeBetween(
            column="float_value",
            quantile_ranges={"quantiles": [0.5], "value_ranges": [[14.9, 17.6]]},
        ),
        # No admissible interpolation of the 0.5 quantile falls below the column's own minimum.
        failing=gxe.ExpectColumnQuantileValuesToBeBetween(
            column="float_value",
            quantile_ranges={"quantiles": [0.5], "value_ranges": [[0.0, 14.9]]},
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnDistinctValuesToBeInSet(
            column="category", value_set=["red", "green", "blue", "purple"]
        ).expectation_type,
        # `category`'s distinct values ({red, green, blue}) are a subset of this wider set.
        passing=gxe.ExpectColumnDistinctValuesToBeInSet(
            column="category", value_set=["red", "green", "blue", "purple"]
        ),
        # `category` also holds "green" and "blue", outside this narrower set.
        failing=gxe.ExpectColumnDistinctValuesToBeInSet(column="category", value_set=["red"]),
    ),
    GoldCase(
        key=gxe.ExpectColumnDistinctValuesToContainSet(
            column="category", value_set=["red", "green"]
        ).expectation_type,
        # `category`'s distinct values include both "red" and "green".
        passing=gxe.ExpectColumnDistinctValuesToContainSet(
            column="category", value_set=["red", "green"]
        ),
        # `category` never holds "purple".
        failing=gxe.ExpectColumnDistinctValuesToContainSet(column="category", value_set=["purple"]),
    ),
    GoldCase(
        key=gxe.ExpectColumnDistinctValuesToEqualSet(
            column="category", value_set=["red", "green", "blue"]
        ).expectation_type,
        # `category`'s distinct values are exactly {red, green, blue}.
        passing=gxe.ExpectColumnDistinctValuesToEqualSet(
            column="category", value_set=["red", "green", "blue"]
        ),
        # Missing "blue" makes this an unequal set.
        failing=gxe.ExpectColumnDistinctValuesToEqualSet(
            column="category", value_set=["red", "green"]
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnMostCommonValueToBeInSet(
            column="category", value_set=["red", "green", "blue"], ties_okay=True
        ).expectation_type,
        # `category`'s three values are three-way tied for most common (two rows each);
        # `ties_okay=True` requires every tied mode to be a member of the set, and all three are.
        passing=gxe.ExpectColumnMostCommonValueToBeInSet(
            column="category", value_set=["red", "green", "blue"], ties_okay=True
        ),
        # None of the tied modes is "purple".
        failing=gxe.ExpectColumnMostCommonValueToBeInSet(
            column="category", value_set=["purple"], ties_okay=True
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnKLDivergenceToBeLessThan(
            column="category",
            partition_object={"values": ["red", "green", "blue"], "weights": [1 / 3, 1 / 3, 1 / 3]},
            threshold=0.01,
        ).expectation_type,
        # `category` is uniformly distributed across its three values (two rows each); the
        # expected partition matches that distribution exactly, so the observed divergence is 0.
        passing=gxe.ExpectColumnKLDivergenceToBeLessThan(
            column="category",
            partition_object={
                "values": ["red", "green", "blue"],
                "weights": [1 / 3, 1 / 3, 1 / 3],
            },
            threshold=0.01,
        ),
        # A partition heavily skewed toward "red" diverges far past this small threshold -- not a
        # near miss, because KL divergence has no natural "just past the true value" reading the
        # way a between-bounds aggregate does; a small skew and a small threshold both risk
        # floating-point noise flipping the verdict, so the skew is chosen distinctly large
        # instead.
        failing=gxe.ExpectColumnKLDivergenceToBeLessThan(
            column="category",
            partition_object={"values": ["red", "green", "blue"], "weights": [0.9, 0.05, 0.05]},
            threshold=0.01,
        ),
    ),
    # ----------------------------------------------------------------------------------------
    # Column-pair, multicolumn, and select-column-values expectations
    #
    # `pair_low` and `increasing_key` are numerically identical in every row of the shared frame
    # (both simply count 1..6) -- a real coincidence of the frame's own data, not a fixture change,
    # and it is what lets the "equal" cases below be expressed without adding a column. Every
    # metric these seven cases lean on (`column_pair_values.*`, `compound_columns.unique`,
    # `multicolumn_sum.equal`, `multicolumn_values.equal`, `select_column_values.unique.
    # within_record`) registers a provider on every execution engine this table runs against,
    # verified against `great_expectations.expectations.registry._registered_metrics`. None of
    # these cases needs an engine restriction.
    # ----------------------------------------------------------------------------------------
    GoldCase(
        key=gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="pair_high", column_B="pair_low"
        ).expectation_type,
        # `pair_high` exceeds `pair_low` in every row by construction.
        passing=gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="pair_high", column_B="pair_low"
        ),
        # Swapping the columns reverses the ordering the shared frame guarantees.
        failing=gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="pair_low", column_B="pair_high"
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnPairValuesToBeEqual(
            column_A="increasing_key", column_B="pair_low"
        ).expectation_type,
        # `pair_low` equals `increasing_key` in every row.
        passing=gxe.ExpectColumnPairValuesToBeEqual(column_A="increasing_key", column_B="pair_low"),
        # `pair_high` is ten times `increasing_key`, never equal.
        failing=gxe.ExpectColumnPairValuesToBeEqual(
            column_A="increasing_key", column_B="pair_high"
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnPairValuesToBeInSet(
            column_A="pair_low",
            column_B="pair_high",
            value_pairs_set=[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)],
        ).expectation_type,
        # Every `(pair_low, pair_high)` row is exactly one of these six declared pairs.
        passing=gxe.ExpectColumnPairValuesToBeInSet(
            column_A="pair_low",
            column_B="pair_high",
            value_pairs_set=[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)],
        ),
        # Only the first row's pair is in this narrower set; the other five are not.
        failing=gxe.ExpectColumnPairValuesToBeInSet(
            column_A="pair_low",
            column_B="pair_high",
            value_pairs_set=[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)],
        ),
    ),
    GoldCase(
        key=gxe.ExpectColumnToExist(column="increasing_key").expectation_type,
        passing=gxe.ExpectColumnToExist(column="increasing_key"),
        # No column of this name exists in the shared frame.
        failing=gxe.ExpectColumnToExist(column="does_not_exist_column"),
    ),
    GoldCase(
        key=gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["multicolumn_a", "multicolumn_b", "multicolumn_c"]
        ).expectation_type,
        # This triple has no duplicate row, by the shared frame's own invariant.
        passing=gxe.ExpectCompoundColumnsToBeUnique(
            column_list=["multicolumn_a", "multicolumn_b", "multicolumn_c"]
        ),
        # `multicolumn_c` is constant (10 in every row); paired with `category` (three values,
        # two rows each), each combination repeats exactly twice -- a real duplicate.
        failing=gxe.ExpectCompoundColumnsToBeUnique(column_list=["multicolumn_c", "category"]),
    ),
    GoldCase(
        key=gxe.ExpectMulticolumnSumToEqual(
            column_list=["multicolumn_a", "multicolumn_b", "multicolumn_c"], sum_total=30
        ).expectation_type,
        # Every row's three multicolumn values sum to exactly 30, by the shared frame's own
        # invariant.
        passing=gxe.ExpectMulticolumnSumToEqual(
            column_list=["multicolumn_a", "multicolumn_b", "multicolumn_c"], sum_total=30
        ),
        # A near miss: one more than the true row sum.
        failing=gxe.ExpectMulticolumnSumToEqual(
            column_list=["multicolumn_a", "multicolumn_b", "multicolumn_c"], sum_total=31
        ),
    ),
    GoldCase(
        key=gxe.ExpectMulticolumnValuesToBeEqual(
            column_list=["increasing_key", "pair_low"]
        ).expectation_type,
        # `pair_low` equals `increasing_key` in every row.
        passing=gxe.ExpectMulticolumnValuesToBeEqual(column_list=["increasing_key", "pair_low"]),
        # `pair_high` is never equal to `increasing_key`.
        failing=gxe.ExpectMulticolumnValuesToBeEqual(column_list=["increasing_key", "pair_high"]),
    ),
    GoldCase(
        key=gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=["pair_low", "pair_high"]
        ).expectation_type,
        # `pair_low` is strictly less than `pair_high` in every row, so the two selected values
        # are never equal within a record.
        passing=gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=["pair_low", "pair_high"]
        ),
        # `pair_low` equals `increasing_key` in every row, so the two selected values collide on
        # every record.
        failing=gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=["increasing_key", "pair_low"]
        ),
    ),
    # ----------------------------------------------------------------------------------------
    # Table-shape expectations
    #
    # Every field here is derived from `_FIXTURE_COLUMNS` (itself derived from
    # `GOLD_FIXTURE_DATA.columns`) rather than hand-listed, so these cases cannot silently drift
    # from the frame they describe.
    # ----------------------------------------------------------------------------------------
    GoldCase(
        key=gxe.ExpectTableColumnCountToBeBetween(
            min_value=len(_FIXTURE_COLUMNS), max_value=len(_FIXTURE_COLUMNS)
        ).expectation_type,
        passing=gxe.ExpectTableColumnCountToBeBetween(
            min_value=len(_FIXTURE_COLUMNS), max_value=len(_FIXTURE_COLUMNS)
        ),
        # A near miss: the lower bound sits one past the true column count.
        failing=gxe.ExpectTableColumnCountToBeBetween(
            min_value=len(_FIXTURE_COLUMNS) + 1, max_value=len(_FIXTURE_COLUMNS) + 5
        ),
    ),
    GoldCase(
        key=gxe.ExpectTableColumnCountToEqual(value=len(_FIXTURE_COLUMNS)).expectation_type,
        passing=gxe.ExpectTableColumnCountToEqual(value=len(_FIXTURE_COLUMNS)),
        # A near miss: one more than the true column count.
        failing=gxe.ExpectTableColumnCountToEqual(value=len(_FIXTURE_COLUMNS) + 1),
    ),
    GoldCase(
        key=gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=list(_FIXTURE_COLUMNS)
        ).expectation_type,
        passing=gxe.ExpectTableColumnsToMatchOrderedList(column_list=list(_FIXTURE_COLUMNS)),
        # A near miss: the first two columns swapped, everything else identical and in place.
        failing=gxe.ExpectTableColumnsToMatchOrderedList(
            column_list=[_FIXTURE_COLUMNS[1], _FIXTURE_COLUMNS[0], *_FIXTURE_COLUMNS[2:]]
        ),
    ),
    GoldCase(
        key=gxe.ExpectTableColumnsToMatchSet(
            column_set=set(_FIXTURE_COLUMNS), exact_match=True
        ).expectation_type,
        passing=gxe.ExpectTableColumnsToMatchSet(
            column_set=set(_FIXTURE_COLUMNS), exact_match=True
        ),
        # A near miss: one real column dropped from the declared set, which an exact match rejects.
        failing=gxe.ExpectTableColumnsToMatchSet(
            column_set=set(_FIXTURE_COLUMNS[1:]), exact_match=True
        ),
    ),
    GoldCase(
        key=gxe.ExpectTableRowCountToBeBetween(min_value=6, max_value=6).expectation_type,
        # The shared frame has exactly six rows.
        passing=gxe.ExpectTableRowCountToBeBetween(min_value=6, max_value=6),
        # A near miss: the lower bound sits one past the true row count.
        failing=gxe.ExpectTableRowCountToBeBetween(min_value=7, max_value=10),
    ),
    GoldCase(
        key=gxe.ExpectTableRowCountToEqual(value=6).expectation_type,
        passing=gxe.ExpectTableRowCountToEqual(value=6),
        # A near miss: one more than the true row count.
        failing=gxe.ExpectTableRowCountToEqual(value=7),
    ),
    GoldCase(
        key=gxe.UnexpectedRowsExpectation(
            unexpected_rows_query=("SELECT * FROM {batch} WHERE increasing_key > 6")
        ).expectation_type,
        # No row has `increasing_key` greater than 6 -- the query returns no rows, which is what
        # this expectation calls success.
        passing=gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE increasing_key > 6"
        ),
        # A near miss: lowering the bound by one returns exactly one row (`increasing_key` 6),
        # one row more than the empty result the passing query gets.
        failing=gxe.UnexpectedRowsExpectation(
            unexpected_rows_query="SELECT * FROM {batch} WHERE increasing_key > 5"
        ),
        # This expectation executes its own SQL/Spark-SQL query as its validation logic; the
        # shipped package registers no pandas provider for it.
        engines=_SQL_AND_SPARK,
        engine_restriction_reason=_NO_PANDAS_PROVIDER_REASON,
    ),
)
"""Every declared case: one per gallery expectation. One seed case per `CaseFixtureShape` member
proves the wiring end to end; the column-map, column-aggregate, column-pair, multicolumn,
select-column-values, and table-shape families fill out the rest of the gallery."""


def _derive_case_keys(cases: Sequence[GoldCase]) -> FrozenSet[str]:
    """The key set `GOLD_CASES` publishes, checked against the two things that make it a faithful
    index of the table rather than merely a set of strings.

    The gallery completeness check (`test_gold_expectation_suite.py`) compares this key set against
    the live registry and nothing else. That comparison is only worth what the key set is worth, so
    both ways the set can misrepresent the table are rejected here rather than collapsed silently:

    - **A repeated key.** Building the set with `frozenset(case.key for case in cases)` on its own
      absorbs a duplicate without a trace: two cases for one expectation, one key, a completeness
      check still green, and the table's stated one-case-per-expectation contract quietly broken.
    - **A key that names an expectation its own configurations do not execute.** The suite runs
      `case.passing` and `case.failing`; only `case.key` reaches the completeness check. A case
      copied and re-keyed without its configurations being changed with it would report coverage of
      the newly named expectation while running the old one -- the named expectation never executed
      and never missed. Requiring `key` to equal both configurations' `expectation_type` is what
      ties the claim to what runs.

    Raises `ValueError` rather than asserting: a bare `assert` at module scope is stripped entirely
    under `python -O`, which would drop the guarantee for every consumer of this module in an
    optimized run (the same reasoning as `_validate_gold_fixture_data` in
    `gold_expectation_cases.py`).
    """
    seen: Set[str] = set()
    duplicates: List[str] = []
    mismatched: List[str] = []
    for case in cases:
        if case.key in seen:
            duplicates.append(case.key)
        seen.add(case.key)
        for attribute, configuration in (("passing", case.passing), ("failing", case.failing)):
            if configuration.expectation_type != case.key:
                mismatched.append(
                    f"{case.key!r} declares a {attribute} configuration of "
                    f"{configuration.expectation_type!r}"
                )
    if duplicates:
        raise ValueError(
            f"GOLD_CASES declares more than one case for {sorted(set(duplicates))}. The table is "
            "one case per gallery expectation, and the published key set is what the gallery "
            "completeness check reads -- a repeated key collapses into one entry there, leaving "
            "the check green while the table says something else. Merge the duplicates or give "
            "each the expectation it is actually accountable for."
        )
    if mismatched:
        raise ValueError(
            "Every gold case's key must equal the expectation type of both its passing and its "
            "failing configuration, because the key is all the gallery completeness check sees "
            "while the configurations are what the suite executes. Mismatched: "
            f"{sorted(mismatched)}."
        )
    return frozenset(seen)


GOLD_CASE_KEYS: Final[FrozenSet[str]] = _derive_case_keys(GOLD_CASES)
"""The published case keys, derived from `GOLD_CASES` rather than hand-kept in sync with it."""
