"""The declarative case table for the validation-result-schema matrix, and its shared fixture.

Underscore-prefixed so pytest does not collect this module.

One case per registered core expectation. Each case names an expectation configured against the
one shared fixture frame published here, so every cell of the matrix produces a *real* result
dict: the matrix exists to record what each engine actually puts in that dict, and a cell whose
metric could not be computed records nothing but the absence of a metric. A configuration that
cannot resolve a metric is therefore a defect in this table, not an acceptable outcome -- the
runner fails such a cell rather than filing it as coverage.

Success and failure are both fine. The runner never asserts on ``success``; it asserts on the
shape of the result dict. A case is written to whichever verdict makes the result dict richest,
which is usually failure for map expectations (an all-passing map expectation returns empty
unexpected lists) and success for aggregates (the observed value is present either way).

Cases are collected under one shared frame. A shared frame means every case resolves to a single
cached batch setup per data source per session rather than one setup per case, so expressing a
new case with an existing column is preferred to adding a column, and adding a column changes
every case's batch.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Final, FrozenSet, List, Mapping, Optional, Tuple

import pandas as pd

import great_expectations.expectations as gxe

if TYPE_CHECKING:
    from great_expectations.expectations.expectation import Expectation

# ---------------------------------------------------------------------------
# Execution-engine vocabulary
# ---------------------------------------------------------------------------
#
# Spelled as plain strings rather than imported from the harness's engine enum on purpose: the
# enum lives in `tests.integration.test_utils.data_source_config`, whose package `__init__`
# eagerly imports every data source module and every dialect driver those modules soft-import.
# This module is imported by unit tests that must not pull that world in. The strings are the
# enum's own `.value`s, and the runner compares against `…execution_engine.value`, so the two
# vocabularies cannot drift apart silently -- a value that stopped matching would restrict every
# case to nothing and fail loudly.

PANDAS_ENGINE: Final[str] = "pandas"
SPARK_ENGINE: Final[str] = "spark"
SQL_ENGINE: Final[str] = "sql"

ALL_ENGINES: Final[FrozenSet[str]] = frozenset({PANDAS_ENGINE, SPARK_ENGINE, SQL_ENGINE})

_PANDAS_ONLY: Final[FrozenSet[str]] = frozenset({PANDAS_ENGINE})
_SQL_ONLY: Final[FrozenSet[str]] = frozenset({SQL_ENGINE})
_PANDAS_AND_SPARK: Final[FrozenSet[str]] = frozenset({PANDAS_ENGINE, SPARK_ENGINE})
_SQL_AND_SPARK: Final[FrozenSet[str]] = frozenset({SQL_ENGINE, SPARK_ENGINE})

_NO_SQL_PROVIDER_REASON: Final[str] = (
    "the shipped package registers no SQL metric provider for this expectation, so a SQL cell "
    "would record a missing metric rather than a result dict"
)
_PANDAS_ONLY_PROVIDER_REASON: Final[str] = (
    "the shipped package registers a metric provider for this expectation on the pandas engine "
    "only, with neither a SQL nor a Spark provider"
)
_NO_PANDAS_PROVIDER_REASON: Final[str] = (
    "this expectation runs a raw SQL/Spark-SQL query as its own validation logic; the shipped "
    "package registers no pandas provider for it"
)
_LIKE_PATTERN_REASON: Final[str] = "a SQL `LIKE` pattern has no meaning on a non-SQL engine"
_SECOND_SOURCE_REASON: Final[str] = (
    "this expectation reads a second table or a second data source, which only the SQL batch "
    "setups in this harness expose"
)

# ---------------------------------------------------------------------------
# Self-reference sentinels
# ---------------------------------------------------------------------------
#
# Two expectations name something that does not exist until the batch is set up at test time --
# the batch's own physical table, and its own data source -- because both carry a randomly
# generated suffix. A case cannot hold either value, so it holds a sentinel and the runner
# substitutes the real name immediately before validating. That keeps every published case a
# plain constructed instance rather than a factory, which is what makes this table readable as a
# specification of what each expectation is asked.
#
# Pointing both at the batch's own table/source is deliberate: this harness parameterizes one
# frame across every data source and so has no second table to offer, and a name that resolves to
# nothing would produce exactly the empty result dict this table exists to rule out.

SELF_TABLE_SENTINEL: Final[str] = "__matrix_self_table__"
SELF_DATA_SOURCE_SENTINEL: Final[str] = "__matrix_self_data_source__"


# ---------------------------------------------------------------------------
# ExpectationCase
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExpectationCase:
    """One declarative case: an expectation configured against the shared fixture frame.

    Holding a constructed instance rather than a factory keeps the table readable as a
    specification, since construction is a side-effect-free model instantiation.
    """

    id: str
    """The expectation type name, as the shipped package registers it.

    One case per registered core expectation is what makes this matrix complete, so this id is
    how a case is matched to the expectation it is accountable for; the completeness guard in
    `tests/unit/core/validation_result_schemas/test_cases_table.py` compares the id set against
    the live registry in both directions.
    """

    expectation: Expectation
    """The configuration validated in every applicable cell."""

    engines: FrozenSet[str] = ALL_ENGINES
    """The execution engines this case applies to.

    Applies to every engine when left at its default. A case that means to restrict itself
    declares a proper, non-empty subset and must also state why in `engine_restriction_reason`.
    """

    engine_restriction_reason: Optional[str] = None
    """Why the expectation cannot be exercised on the engines `engines` excludes.

    Required whenever `engines` is a proper subset, and forbidden otherwise: a reason left behind
    on a case that no longer restricts anything reads as a live constraint while constraining
    nothing.
    """

    unsupported_data_sources: Mapping[str, str] = field(default_factory=dict)
    """Data sources, by harness test id, that cannot evaluate this expectation, each with a reason.

    This is a property of one data source, not of an engine: the SQL Server dialect has no regex
    operator, so the regex metrics raise there while every other SQL dialect evaluates them. The
    runner records such a cell as `unsupported` and skips it, so the gap stays visible in the
    findings without being mistaken for a schema failure. Every entry needs a reason; an entry
    without one is rejected at construction.
    """

    empty_result_reason: Optional[str] = None
    """Why this expectation returns no result payload at all, if it returns none.

    Left `None` by all but a handful of cases. The runner requires a non-empty result dict at
    every format but `BOOLEAN_ONLY` -- an empty one is what a cell looks like when nothing was
    computed, and a schema parses it happily -- so an expectation that genuinely emits no payload
    has to say so here rather than being quietly tolerated. Declaring it turns an unexplained
    empty cell into a stated property of the expectation, and leaves the check armed for every
    other case.
    """

    def __post_init__(self) -> None:
        unknown = self.engines - ALL_ENGINES
        if unknown:
            raise ValueError(
                f"Case {self.id!r} names unknown execution engine(s) {sorted(unknown)}. The "
                f"vocabulary is {sorted(ALL_ENGINES)}, matching the harness's own engine values."
            )
        if not self.engines:
            raise ValueError(
                f"Case {self.id!r} declares an empty engine set. A case that applies to no "
                "execution engine proves nothing; restrict it to a non-empty subset, with a "
                "reason, or leave `engines` at its default to apply to all of them."
            )
        if self.engines != ALL_ENGINES and not self.engine_restriction_reason:
            raise ValueError(
                f"Case {self.id!r} restricts itself to {sorted(self.engines)} without an "
                "`engine_restriction_reason`. State what does not apply on the excluded engines, "
                "so a later reader can tell a real limitation from an unexplained gap."
            )
        if self.engines == ALL_ENGINES and self.engine_restriction_reason:
            raise ValueError(
                f"Case {self.id!r} carries an `engine_restriction_reason` while applying to every "
                "engine. Drop the reason, or restrict `engines` to the subset it describes."
            )
        for test_id, reason in self.unsupported_data_sources.items():
            if not test_id or not test_id.strip() or not reason or not reason.strip():
                raise ValueError(
                    f"Case {self.id!r} declares an unsupported data source without both a test id "
                    "and a reason. Name the data source by its harness test id and state why it "
                    "cannot evaluate this expectation."
                )


# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------
#
# One frame serves every case. Adding a column here changes every case's batch setup, so a new
# case should first ask whether an existing column already expresses what it needs.
#
# Every column is a type each of the eleven parameterized data sources can carry: plain integers,
# floats, short strings, and pandas datetimes. Two of those data sources round-trip the frame
# through CSV without a declared schema, so a column's *dtype* is not preserved everywhere even
# though its values are -- see the note on the date/timestamp columns below.

INCREASING_KEY_COL: Final[str] = "increasing_key"
"""A unique, strictly increasing integer key: uniqueness, increasing, min/max, and any case
needing a well-ordered numeric column."""

DECREASING_COL: Final[str] = "decreasing_value"
"""A strictly decreasing integer column: the decreasing expectation, which no other column
serves."""

FLOAT_COL: Final[str] = "float_value"
"""A float column with a known distribution: mean, median, stdev, quantiles, sum, z-scores, and
between-value checks."""

CATEGORY_COL: Final[str] = "category"
"""A short string column drawn from a small closed value set: value-set, distinct-value, and
most-common-value checks."""

PATTERN_COL: Final[str] = "pattern_code"
"""A string column matching an anchored pattern: regex, like-pattern, match-list, and value-length
checks."""

NULLABLE_COL: Final[str] = "nullable_value"
"""A nullable column with a known null ratio (two of eight rows null): null, not-null, `mostly`,
and non-null-proportion checks."""

DATE_COL: Final[str] = "record_date"
"""A date column, carried as a pandas datetime."""

JSON_COL: Final[str] = "json_payload"
"""A string column holding valid JSON: JSON-parseable and JSON-schema checks."""

STRFTIME_COL: Final[str] = "strftime_code"
"""A string column in a fixed strftime format (``%m/%d/%Y``): strftime-format and
dateutil-parseable checks."""

PAIR_LOW_COL: Final[str] = "pair_low"
PAIR_HIGH_COL: Final[str] = "pair_high"
"""A pair of numeric columns with a known ordering (`pair_low` < `pair_high` in every row):
column-pair comparisons. `pair_low` also equals `increasing_key` in every row, which is what lets
the equality cases be expressed without spending another column on them."""

MULTICOLUMN_A_COL: Final[str] = "multicolumn_a"
MULTICOLUMN_B_COL: Final[str] = "multicolumn_b"
MULTICOLUMN_C_COL: Final[str] = "multicolumn_c"
"""Three numeric columns with a known constant row sum and no duplicate row: multicolumn sum and
compound-column uniqueness."""

MULTICOLUMN_ROW_SUM: Final[int] = 30
"""The constant sum of the three multicolumn columns in every row of the shared frame."""

ROW_COUNT: Final[int] = 8
"""The shared frame's row count, referenced by the row-count cases so they cannot drift from it."""

MATRIX_FIXTURE_DATA: pd.DataFrame = pd.DataFrame(
    {
        INCREASING_KEY_COL: [1, 2, 3, 4, 5, 6, 7, 8],
        DECREASING_COL: [80, 70, 60, 50, 40, 30, 20, 10],
        FLOAT_COL: [10.0, 12.5, 15.0, 17.5, 20.0, 22.5, 25.0, 27.5],
        CATEGORY_COL: ["red", "green", "blue", "red", "green", "blue", "red", "red"],
        PATTERN_COL: ["A100", "A200", "A300", "A400", "A500", "A600", "A700", "A800"],
        NULLABLE_COL: [1.0, None, 3.0, None, 5.0, 6.0, 7.0, 8.0],
        # Plain `datetime.date` objects held in an object-dtype column, not a pandas
        # `datetime64` column. The distinction is load-bearing rather than stylistic:
        # the Spark batch setup passes each value through untouched when the config declares no
        # column types (as it does here, since every data source is instantiated with no
        # arguments), and Spark infers a `pandas.Timestamp` as an empty nested struct, which the
        # CSV writer then refuses to write at all -- failing every Spark cell in the matrix
        # during setup, before a single expectation runs. A `datetime64` column yields
        # `pandas.Timestamp` values, so the dtype must be pinned to object for the real Python
        # objects to survive. `date` is in the SQL setup's inference map, so the SQL backends
        # get a real DATE column. There is deliberately no timestamp column: the shared SQL
        # setup infers `datetime` as the generic DATETIME type, which PostgreSQL does not have,
        # and the configs here are default-constructed so no per-dialect column type can be
        # supplied to correct it.
        DATE_COL: pd.Series(
            [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 4),
                date(2024, 1, 5),
                date(2024, 1, 6),
                date(2024, 1, 7),
                date(2024, 1, 8),
            ],
            dtype=object,
        ),
        JSON_COL: [
            '{"a": 1}',
            '{"a": 2}',
            '{"a": 3}',
            '{"a": 4}',
            '{"a": 5}',
            '{"a": 6}',
            '{"a": 7}',
            '{"a": 8}',
        ],
        # Deliberately month-first and slash-delimited rather than ISO. This column must stay a
        # *string* on every data source, and one of them reads the frame back from CSV with
        # schema inference on, where an ISO-shaped value is inferred as a date -- which makes the
        # strftime-format expectation reject the column outright for not being text, in the one
        # place this column exists to be tested.
        STRFTIME_COL: [
            "01/01/2024",
            "01/02/2024",
            "01/03/2024",
            "01/04/2024",
            "01/05/2024",
            "01/06/2024",
            "01/07/2024",
            "01/08/2024",
        ],
        PAIR_LOW_COL: [1, 2, 3, 4, 5, 6, 7, 8],
        PAIR_HIGH_COL: [10, 20, 30, 40, 50, 60, 70, 80],
        MULTICOLUMN_A_COL: [10, 9, 8, 7, 6, 5, 4, 3],
        MULTICOLUMN_B_COL: [10, 11, 12, 13, 14, 15, 16, 17],
        MULTICOLUMN_C_COL: [10, 10, 10, 10, 10, 10, 10, 10],
    }
)
"""The one shared frame every case validates against.

No case compares a *bound* against the date or timestamp column. Two of the eleven data sources
write the frame to CSV and read it back with no declared schema, so those two see both columns as
strings while the rest see real dates -- and the aggregate between-bounds path compares an
observed value against a declared bound with no type normalization at all, so no single bound
spelling survives both readings. A bound that crashed on one backend would record a
missing metric there, which is precisely the vacuous cell this table exists to rule out. The two
columns are still real, loaded, queryable columns: the table-shape cases below derive their
expected column list and count from the frame, and `expect_column_to_exist` names the date column
directly.
"""

FIXTURE_COLUMNS: Final[Tuple[str, ...]] = tuple(MATRIX_FIXTURE_DATA.columns)
"""Derived from the frame itself rather than hand-listed, so the table-shape cases cannot drift
from the frame they describe."""


def _validate_fixture_data() -> None:
    """Check the invariants the frame's docstrings promise.

    A real check rather than a module-level ``assert``: a bare assert is removed entirely under
    ``python -O``, silently dropping the guarantee for every consumer of this module.
    """
    if len(MATRIX_FIXTURE_DATA) != ROW_COUNT:
        raise ValueError(
            f"MATRIX_FIXTURE_DATA must have exactly {ROW_COUNT} rows, "
            f"got {len(MATRIX_FIXTURE_DATA)}."
        )
    row_sums = (
        MATRIX_FIXTURE_DATA[MULTICOLUMN_A_COL]
        + MATRIX_FIXTURE_DATA[MULTICOLUMN_B_COL]
        + MATRIX_FIXTURE_DATA[MULTICOLUMN_C_COL]
    )
    if not (row_sums == MULTICOLUMN_ROW_SUM).all():
        raise ValueError(
            f"Every row of the three multicolumn columns must sum to {MULTICOLUMN_ROW_SUM}; "
            f"got {row_sums.tolist()}."
        )
    multicolumn_frame = MATRIX_FIXTURE_DATA[
        [MULTICOLUMN_A_COL, MULTICOLUMN_B_COL, MULTICOLUMN_C_COL]
    ]
    if multicolumn_frame.duplicated().any():
        raise ValueError(
            "The three multicolumn columns must have no duplicate row, for the compound-column "
            "uniqueness case."
        )
    if not (MATRIX_FIXTURE_DATA[PAIR_LOW_COL] < MATRIX_FIXTURE_DATA[PAIR_HIGH_COL]).all():
        raise ValueError(
            f"Every row's {PAIR_LOW_COL} must be less than its {PAIR_HIGH_COL}, for the "
            "column-pair ordering cases."
        )
    if not (MATRIX_FIXTURE_DATA[PAIR_LOW_COL] == MATRIX_FIXTURE_DATA[INCREASING_KEY_COL]).all():
        raise ValueError(
            f"Every row's {PAIR_LOW_COL} must equal its {INCREASING_KEY_COL}. The equality cases "
            "pair those two columns to get a relationship that holds on every row without "
            "spending another column on it, so this is a dependency rather than a coincidence of "
            "the values. Change both columns together, or give those cases a dedicated pair."
        )
    null_count = int(MATRIX_FIXTURE_DATA[NULLABLE_COL].isna().sum())
    if null_count != _NULL_COUNT:
        raise ValueError(
            f"{NULLABLE_COL} must hold exactly {_NULL_COUNT} nulls, for the null-ratio cases; "
            f"got {null_count}."
        )


_NULL_COUNT: Final[int] = 2
"""The known null count in `nullable_value`: two of eight rows, a 0.25 null ratio."""

_NON_NULL_PROPORTION: Final[float] = (ROW_COUNT - _NULL_COUNT) / ROW_COUNT

_validate_fixture_data()


# ---------------------------------------------------------------------------
# Type-name arguments
# ---------------------------------------------------------------------------
#
# The two type-checking expectations take engine-specific type-name vocabularies -- numpy dtype
# names on pandas, dialect type names on SQL, PySpark class names on Spark -- and no single name
# is correct on all three. They are still exercised on all three, because being *wrong* is not
# being *unresolvable*: an unrecognized name resolves to "no candidate types" and reports a plain
# failure on the pandas and SQL paths, which is a complete result dict either way. The Spark path
# is the exception -- it raises when *nothing* in the list resolves -- so both arguments below
# include at least one real PySpark type name, which is what keeps every cell resolvable.

_SPARK_RESOLVABLE_TYPE_NAME: Final[str] = "LongType"
_MULTI_ENGINE_TYPE_LIST: Final[List[str]] = [
    "INTEGER",
    "BIGINT",
    "int64",
    "LongType",
    "IntegerType",
]


# ---------------------------------------------------------------------------
# EXPECTATION_CASES
# ---------------------------------------------------------------------------

EXPECTATION_CASES: List[ExpectationCase] = [
    # ------------------------------------------------------------------
    # Column-map expectations
    # ------------------------------------------------------------------
    ExpectationCase(
        id="expect_column_value_lengths_to_be_between",
        # Every `pattern_code` value is exactly four characters ("A100" .. "A800").
        expectation=gxe.ExpectColumnValueLengthsToBeBetween(
            column=PATTERN_COL, min_value=4, max_value=4
        ),
    ),
    ExpectationCase(
        id="expect_column_value_lengths_to_equal",
        # Failing on purpose: the real length is four, so every row is unexpected and the
        # unexpected lists this matrix records are populated rather than empty.
        expectation=gxe.ExpectColumnValueLengthsToEqual(column=PATTERN_COL, value=3),
    ),
    ExpectationCase(
        id="expect_column_value_z_scores_to_be_less_than",
        expectation=gxe.ExpectColumnValueZScoresToBeLessThan(
            column=FLOAT_COL, threshold=3.0, double_sided=True
        ),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_be_between",
        # The column's maximum (27.5) exceeds this upper bound, so the last row is unexpected.
        expectation=gxe.ExpectColumnValuesToBeBetween(
            column=FLOAT_COL, min_value=10.0, max_value=25.0
        ),
    ),
    ExpectationCase(
        id="expect_column_values_to_be_dateutil_parseable",
        expectation=gxe.ExpectColumnValuesToBeDateutilParseable(column=STRFTIME_COL),
        engines=_PANDAS_ONLY,
        engine_restriction_reason=_PANDAS_ONLY_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_be_decreasing",
        expectation=gxe.ExpectColumnValuesToBeDecreasing(column=DECREASING_COL),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_be_in_set",
        # Failing on purpose: `category` also holds "green" and "blue", outside this set.
        expectation=gxe.ExpectColumnValuesToBeInSet(column=CATEGORY_COL, value_set=["red"]),
    ),
    ExpectationCase(
        id="expect_column_values_to_be_in_type_list",
        expectation=gxe.ExpectColumnValuesToBeInTypeList(
            column=INCREASING_KEY_COL, type_list=_MULTI_ENGINE_TYPE_LIST
        ),
    ),
    ExpectationCase(
        id="expect_column_values_to_be_increasing",
        expectation=gxe.ExpectColumnValuesToBeIncreasing(column=INCREASING_KEY_COL),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_be_json_parseable",
        expectation=gxe.ExpectColumnValuesToBeJsonParseable(column=JSON_COL),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_be_null",
        # Failing on purpose: the default `mostly` demands every row be null; six of eight are not,
        # so the unexpected lists are populated.
        expectation=gxe.ExpectColumnValuesToBeNull(column=NULLABLE_COL),
    ),
    ExpectationCase(
        id="expect_column_values_to_be_of_type",
        # A PySpark type name, so the Spark path resolves it rather than raising; the pandas and
        # SQL paths report a plain mismatch, which is the complete result dict this matrix wants.
        # This is also the one expectation with a per-engine schema override, so it must reach
        # all three engines.
        expectation=gxe.ExpectColumnValuesToBeOfType(
            column=INCREASING_KEY_COL, type_=_SPARK_RESOLVABLE_TYPE_NAME
        ),
    ),
    ExpectationCase(
        id="expect_column_values_to_be_unique",
        # Failing on purpose: `category` repeats its three values across eight rows.
        expectation=gxe.ExpectColumnValuesToBeUnique(column=CATEGORY_COL),
    ),
    ExpectationCase(
        id="expect_column_values_to_match_json_schema",
        # Failing on purpose: `a` is always a number, never a string.
        expectation=gxe.ExpectColumnValuesToMatchJsonSchema(
            column=JSON_COL,
            json_schema={
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": ["a"],
            },
        ),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_match_like_pattern",
        # Failing on purpose: no `pattern_code` value starts with "B".
        expectation=gxe.ExpectColumnValuesToMatchLikePattern(column=PATTERN_COL, like_pattern="B%"),
        engines=_SQL_ONLY,
        engine_restriction_reason=_LIKE_PATTERN_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_match_like_pattern_list",
        # Failing on purpose: no value starts with "B" or "C".
        expectation=gxe.ExpectColumnValuesToMatchLikePatternList(
            column=PATTERN_COL, like_pattern_list=["B%", "C%"], match_on="any"
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason=_LIKE_PATTERN_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_match_regex",
        # Failing on purpose: no `pattern_code` value starts with "B".
        expectation=gxe.ExpectColumnValuesToMatchRegex(column=PATTERN_COL, regex="^B"),
        unsupported_data_sources={
            "mssql": "the SQL Server dialect has no regex operator, so the regex metrics raise "
            "before producing a result",
        },
    ),
    ExpectationCase(
        id="expect_column_values_to_match_regex_list",
        # Failing on purpose: no value matches a leading "B" or "C".
        expectation=gxe.ExpectColumnValuesToMatchRegexList(
            column=PATTERN_COL, regex_list=["^B", "^C"], match_on="any"
        ),
        unsupported_data_sources={
            "mssql": "the SQL Server dialect has no regex operator, so the regex metrics raise "
            "before producing a result",
        },
    ),
    ExpectationCase(
        id="expect_column_values_to_match_strftime_format",
        # Failing on purpose: `strftime_code` values are month-first and slash-delimited.
        expectation=gxe.ExpectColumnValuesToMatchStrftimeFormat(
            column=STRFTIME_COL, strftime_format="%Y-%m-%d"
        ),
        engines=_PANDAS_AND_SPARK,
        engine_restriction_reason=_NO_SQL_PROVIDER_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_not_be_in_set",
        # Failing on purpose: `category` does hold "red".
        expectation=gxe.ExpectColumnValuesToNotBeInSet(column=CATEGORY_COL, value_set=["red"]),
    ),
    ExpectationCase(
        id="expect_column_values_to_not_be_null",
        # Failing on purpose: two of eight `nullable_value` rows are null.
        expectation=gxe.ExpectColumnValuesToNotBeNull(column=NULLABLE_COL),
    ),
    ExpectationCase(
        id="expect_column_values_to_not_be_outliers",
        # Failing on purpose: a near-zero multiplier admits only values at the mean.
        expectation=gxe.ExpectColumnValuesToNotBeOutliers(
            column=FLOAT_COL, method="std", multiplier=0.01
        ),
    ),
    ExpectationCase(
        id="expect_column_values_to_not_match_like_pattern",
        # Failing on purpose: every `pattern_code` value starts with "A".
        expectation=gxe.ExpectColumnValuesToNotMatchLikePattern(
            column=PATTERN_COL, like_pattern="A%"
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason=_LIKE_PATTERN_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_not_match_like_pattern_list",
        # Failing on purpose: every value matches the "A" pattern.
        expectation=gxe.ExpectColumnValuesToNotMatchLikePatternList(
            column=PATTERN_COL, like_pattern_list=["A%"]
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason=_LIKE_PATTERN_REASON,
    ),
    ExpectationCase(
        id="expect_column_values_to_not_match_regex",
        # Failing on purpose: every `pattern_code` value starts with "A".
        expectation=gxe.ExpectColumnValuesToNotMatchRegex(column=PATTERN_COL, regex="^A"),
        unsupported_data_sources={
            "mssql": "the SQL Server dialect has no regex operator, so the regex metrics raise "
            "before producing a result",
        },
    ),
    ExpectationCase(
        id="expect_column_values_to_not_match_regex_list",
        # Failing on purpose: every value matches the "A" pattern.
        expectation=gxe.ExpectColumnValuesToNotMatchRegexList(
            column=PATTERN_COL, regex_list=["^A"]
        ),
        unsupported_data_sources={
            "mssql": "the SQL Server dialect has no regex operator, so the regex metrics raise "
            "before producing a result",
        },
    ),
    # ------------------------------------------------------------------
    # Column-pair map expectations
    # ------------------------------------------------------------------
    ExpectationCase(
        id="expect_column_pair_values_a_to_be_greater_than_b",
        # Failing on purpose: `pair_low` is below `pair_high` in every row, so reversing the
        # arguments makes every row unexpected.
        expectation=gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=PAIR_LOW_COL, column_B=PAIR_HIGH_COL
        ),
    ),
    ExpectationCase(
        id="expect_column_pair_values_to_be_equal",
        # `pair_low` equals `increasing_key` in every row.
        expectation=gxe.ExpectColumnPairValuesToBeEqual(
            column_A=INCREASING_KEY_COL, column_B=PAIR_LOW_COL
        ),
    ),
    ExpectationCase(
        id="expect_column_pair_values_to_be_in_set",
        # Failing on purpose: only the first two rows' pairs are in this narrower set.
        expectation=gxe.ExpectColumnPairValuesToBeInSet(
            column_A=PAIR_LOW_COL,
            column_B=PAIR_HIGH_COL,
            value_pairs_set=[(1, 10), (2, 20)],
        ),
    ),
    # ------------------------------------------------------------------
    # Multicolumn map expectations
    # ------------------------------------------------------------------
    ExpectationCase(
        id="expect_compound_columns_to_be_unique",
        # The multicolumn triple has no duplicate row, by the shared frame's own invariant.
        expectation=gxe.ExpectCompoundColumnsToBeUnique(
            column_list=[MULTICOLUMN_A_COL, MULTICOLUMN_B_COL, MULTICOLUMN_C_COL]
        ),
    ),
    ExpectationCase(
        id="expect_multicolumn_sum_to_equal",
        # Failing on purpose: one past the frame's own constant row sum, so every row is
        # unexpected.
        expectation=gxe.ExpectMulticolumnSumToEqual(
            column_list=[MULTICOLUMN_A_COL, MULTICOLUMN_B_COL, MULTICOLUMN_C_COL],
            sum_total=MULTICOLUMN_ROW_SUM + 1,
        ),
    ),
    ExpectationCase(
        id="expect_multicolumn_values_to_be_equal",
        # Failing on purpose: `pair_high` is never equal to `increasing_key`.
        expectation=gxe.ExpectMulticolumnValuesToBeEqual(
            column_list=[INCREASING_KEY_COL, PAIR_HIGH_COL]
        ),
    ),
    ExpectationCase(
        id="expect_select_column_values_to_be_unique_within_record",
        # Failing on purpose: `pair_low` equals `increasing_key` in every row, so the two selected
        # values collide on every record.
        expectation=gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=[INCREASING_KEY_COL, PAIR_LOW_COL]
        ),
    ),
    # ------------------------------------------------------------------
    # Column-aggregate expectations
    # ------------------------------------------------------------------
    ExpectationCase(
        id="expect_column_distinct_values_to_be_in_set",
        # `category`'s distinct values are a subset of this wider set.
        expectation=gxe.ExpectColumnDistinctValuesToBeInSet(
            column=CATEGORY_COL, value_set=["red", "green", "blue", "purple"]
        ),
    ),
    ExpectationCase(
        id="expect_column_distinct_values_to_contain_set",
        expectation=gxe.ExpectColumnDistinctValuesToContainSet(
            column=CATEGORY_COL, value_set=["red", "green"]
        ),
    ),
    ExpectationCase(
        id="expect_column_distinct_values_to_equal_set",
        expectation=gxe.ExpectColumnDistinctValuesToEqualSet(
            column=CATEGORY_COL, value_set=["red", "green", "blue"]
        ),
    ),
    ExpectationCase(
        id="expect_column_kl_divergence_to_be_less_than",
        # The expected partition matches `category`'s real distribution (four red, two green, two
        # blue), so the observed divergence is zero and comfortably under the threshold.
        expectation=gxe.ExpectColumnKLDivergenceToBeLessThan(
            column=CATEGORY_COL,
            partition_object={
                "values": ["red", "green", "blue"],
                "weights": [0.5, 0.25, 0.25],
            },
            threshold=0.01,
        ),
    ),
    ExpectationCase(
        id="expect_column_max_to_be_between",
        # `float_value`'s true maximum is exactly 27.5.
        expectation=gxe.ExpectColumnMaxToBeBetween(
            column=FLOAT_COL, min_value=27.4, max_value=27.6
        ),
    ),
    ExpectationCase(
        id="expect_column_mean_to_be_between",
        # `float_value`'s true mean is exactly 18.75.
        expectation=gxe.ExpectColumnMeanToBeBetween(
            column=FLOAT_COL, min_value=18.7, max_value=18.8
        ),
    ),
    ExpectationCase(
        id="expect_column_median_to_be_between",
        # Deliberately wide: the median of an even-length column is not pinned to one
        # interpolation across engines, so this range covers every admissible answer between the
        # two middle values rather than a near miss one backend's algorithm would fail.
        expectation=gxe.ExpectColumnMedianToBeBetween(
            column=FLOAT_COL, min_value=17.5, max_value=20.0
        ),
    ),
    ExpectationCase(
        id="expect_column_min_to_be_between",
        # `float_value`'s true minimum is exactly 10.0.
        expectation=gxe.ExpectColumnMinToBeBetween(column=FLOAT_COL, min_value=9.9, max_value=10.1),
    ),
    ExpectationCase(
        id="expect_column_most_common_value_to_be_in_set",
        # "red" is the sole mode of `category` (four of eight rows). `ties_okay` is set anyway so
        # the case does not depend on a backend's tie-breaking behavior.
        expectation=gxe.ExpectColumnMostCommonValueToBeInSet(
            column=CATEGORY_COL, value_set=["red"], ties_okay=True
        ),
    ),
    ExpectationCase(
        id="expect_column_proportion_of_non_null_values_to_be_between",
        # Six of eight `nullable_value` rows are non-null.
        expectation=gxe.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column=NULLABLE_COL,
            min_value=_NON_NULL_PROPORTION - 0.01,
            max_value=_NON_NULL_PROPORTION + 0.01,
        ),
    ),
    ExpectationCase(
        id="expect_column_proportion_of_unique_values_to_be_between",
        # `increasing_key`'s proportion of unique values is exactly 1.0.
        expectation=gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column=INCREASING_KEY_COL, min_value=0.99, max_value=1.0
        ),
    ),
    ExpectationCase(
        id="expect_column_quantile_values_to_be_between",
        # Deliberately wide, for the same interpolation reason as the median case: the range spans
        # the column's own minimum and maximum, which no admissible 0.5-quantile can fall outside.
        expectation=gxe.ExpectColumnQuantileValuesToBeBetween(
            column=FLOAT_COL,
            quantile_ranges={"quantiles": [0.5], "value_ranges": [[10.0, 27.5]]},
        ),
    ),
    ExpectationCase(
        id="expect_column_stdev_to_be_between",
        # `float_value`'s sample standard deviation is ~5.61; the range is loose enough to survive
        # a backend that reports the population figure instead.
        expectation=gxe.ExpectColumnStdevToBeBetween(
            column=FLOAT_COL, min_value=1.0, max_value=10.0
        ),
    ),
    ExpectationCase(
        id="expect_column_sum_to_be_between",
        # `float_value`'s true sum is exactly 150.0.
        expectation=gxe.ExpectColumnSumToBeBetween(
            column=FLOAT_COL, min_value=149.9, max_value=150.1
        ),
    ),
    ExpectationCase(
        id="expect_column_to_exist",
        # Names the date column: this expectation reads no values, so it is the one case that can
        # name a column whose dtype differs between the CSV-backed data sources and the rest
        # without that difference meaning anything.
        expectation=gxe.ExpectColumnToExist(column=DATE_COL),
        empty_result_reason=(
            "this expectation's validation returns only a success verdict and no `result` key at "
            "all, on every engine and at every result format, so there is no payload for the "
            "matrix to record"
        ),
    ),
    ExpectationCase(
        id="expect_column_unique_value_count_to_be_between",
        expectation=gxe.ExpectColumnUniqueValueCountToBeBetween(
            column=INCREASING_KEY_COL, min_value=ROW_COUNT, max_value=ROW_COUNT
        ),
    ),
    # ------------------------------------------------------------------
    # Table- and batch-shape expectations
    # ------------------------------------------------------------------
    ExpectationCase(
        id="expect_query_results_to_match_comparison",
        # Both queries return a single one-column row holding the frame's row count, so the
        # comparison matches. The comparison data source is the batch's own, resolved at test
        # time; the comparison query is executed verbatim against it and so must be a complete
        # statement rather than a `{batch}` template.
        expectation=gxe.ExpectQueryResultsToMatchComparison(
            base_query="SELECT COUNT(*) AS row_total FROM {batch}",
            comparison_data_source_name=SELF_DATA_SOURCE_SENTINEL,
            comparison_query=f"SELECT {ROW_COUNT} AS row_total",
        ),
        engines=_SQL_ONLY,
        engine_restriction_reason=_SECOND_SOURCE_REASON,
    ),
    ExpectationCase(
        id="expect_table_column_count_to_be_between",
        expectation=gxe.ExpectTableColumnCountToBeBetween(
            min_value=len(FIXTURE_COLUMNS), max_value=len(FIXTURE_COLUMNS)
        ),
    ),
    ExpectationCase(
        id="expect_table_column_count_to_equal",
        expectation=gxe.ExpectTableColumnCountToEqual(value=len(FIXTURE_COLUMNS)),
    ),
    ExpectationCase(
        id="expect_table_columns_to_match_ordered_list",
        expectation=gxe.ExpectTableColumnsToMatchOrderedList(column_list=list(FIXTURE_COLUMNS)),
    ),
    ExpectationCase(
        id="expect_table_columns_to_match_set",
        expectation=gxe.ExpectTableColumnsToMatchSet(
            column_set=set(FIXTURE_COLUMNS), exact_match=True
        ),
    ),
    ExpectationCase(
        id="expect_table_row_count_to_be_between",
        expectation=gxe.ExpectTableRowCountToBeBetween(min_value=ROW_COUNT, max_value=ROW_COUNT),
    ),
    ExpectationCase(
        id="expect_table_row_count_to_equal",
        expectation=gxe.ExpectTableRowCountToEqual(value=ROW_COUNT),
    ),
    ExpectationCase(
        id="expect_table_row_count_to_equal_other_table",
        # The batch's own table, resolved at test time: it is a real table with a known row count,
        # so the comparison is a real one that always holds.
        expectation=gxe.ExpectTableRowCountToEqualOtherTable(other_table_name=SELF_TABLE_SENTINEL),
        engines=_SQL_ONLY,
        engine_restriction_reason=_SECOND_SOURCE_REASON,
    ),
    ExpectationCase(
        id="unexpected_rows_expectation",
        # Failing on purpose: rows with `increasing_key` above 5 are returned as unexpected.
        expectation=gxe.UnexpectedRowsExpectation(
            unexpected_rows_query=(f"SELECT * FROM {{batch}} WHERE {INCREASING_KEY_COL} > 5")
        ),
        engines=_SQL_AND_SPARK,
        engine_restriction_reason=_NO_PANDAS_PROVIDER_REASON,
    ),
]
"""One case per registered core expectation.

The completeness guard in `tests/unit/core/validation_result_schemas/test_cases_table.py` compares
this table's ids against the live registry in both directions, so an expectation added upstream
fails that guard until a case is written for it.
"""
