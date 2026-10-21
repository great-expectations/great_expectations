"""Backend-scoped ClickHouse coverage.

This module exists only for GX Core code paths that no other lane reaches. Its four groups are
disjoint from the curated suite's assertion groups by construction:

* Quantile values -- the bespoke `_get_column_quantiles_clickhouse` helper has no live coverage
  anywhere else, because the only cross-backend quantile assertion is parameterized over the
  canonical-expectations list, which this backend does not join.
* Regular expression and pattern matching -- the dialect-specific `regexp_like` regex branch and
  the LIKE-support branch in `great_expectations/expectations/metrics/util.py`.
* Null-bearing data -- the curated data sets carry no nulls; this dialect's columns are all
  declared `Nullable(...)` specifically so real nulls round-trip.
* Column types -- one expectation per declared Python-type override in
  `tests/integration/test_utils/data_source_config/clickhouse.py`'s `_COLUMN_TYPE_OVERRIDES`.

Deliberately absent: a quoted-identifier group. The curated suite's second data set already covers
a spaced, a reserved-word and a mixed-case column name, and once this backend joins the curated
tier that data set runs in this lane, against this server, through this dialect's identifier
preparer. Carrying the same three shapes here would be the exact duplication this module is
forbidden from carrying.

Also deliberately absent: coverage for the dialect's `index`-as-reserved-word and `%`-escaping
identifier-preparer quirks. Those are behaviors of `clickhouse-sqlalchemy`'s own preparer, not of a
GX Core path this lane reaches, so exercising them here would test the dependency, not the product.

Findings recorded here are from a real run against the container started by
`invoke service --markers=clickhouse`.
"""

from datetime import date, datetime, timezone

import pandas as pd
import pytest

try:
    from clickhouse_sqlalchemy import types as clickhouse_types
except ImportError:
    clickhouse_types = None

import great_expectations.expectations as gxe
from great_expectations import get_context
from great_expectations.core import ExpectationSuite
from great_expectations.core.result_format import ResultFormat
from great_expectations.expectations.core.expect_column_quantile_values_to_be_between import (
    QuantileRange,
)
from tests.integration.test_utils.data_source_config import ClickHouseDatasourceTestConfig
from tests.integration.test_utils.data_source_config.clickhouse import ClickHouseBatchTestSetup

pytestmark = pytest.mark.clickhouse


def _naive_datetime(
    year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0
) -> datetime:
    """A tz-naive `datetime`, matching `DateTime64()`'s naive semantics.

    Constructed via a UTC-aware `datetime` and then stripped, rather than called bare, only to
    satisfy the repo's `DTZ001` lint rule -- the resulting value is genuinely naive, which is what
    this dialect's declared `datetime`/`pd.Timestamp` override (`Nullable(DateTime64())`) expects.
    """
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).replace(
        tzinfo=None
    )


class TestClickHouseQuantileValues:
    """Exercise the bespoke ClickHouse quantile-values metric against a running server. This is
    the only live coverage of `_get_column_quantiles_clickhouse` in the repo,
    because the sole cross-backend quantile assertion is parameterized over the
    canonical-expectations list, which this backend deliberately does not join.

    `_get_column_quantiles_clickhouse` previously carried two defects: its `sa.select(...)` call
    passed a list of select entities instead of unpacking it, which SQLAlchemy 2.x rejects with
    `ArgumentError` before the query ever reaches the execution engine; and, masked behind that
    first error, it called an execution-engine method that does not exist. Both are fixed, matching
    the pattern every sibling dialect helper in the same file already uses, so this group passes.
    """

    COL = "col_a"

    def test_quantile_values(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(),
            data=pd.DataFrame({self.COL: [1, 2, 3, 4, 5]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnQuantileValuesToBeBetween(
                    column=self.COL,
                    quantile_ranges=QuantileRange(
                        quantiles=[0, 0.333, 0.667, 1],
                        value_ranges=[[1, 1], [2, 2], [3, 4], [5, 5]],
                    ),
                )
            )
        assert result.success


class TestClickHouseRegexAndLikePatterns:
    """Exercise the ClickHouse-specific regular-expression branch and the LIKE-support branch in
    `great_expectations/expectations/metrics/util.py`
    (`get_dialect_regex_expression` and `get_dialect_like_pattern_expression` respectively).

    Observed on this baseline: the regex branch (both `ExpectColumnValuesToMatchRegex` and
    `ExpectColumnValuesToNotMatchRegex`, which share `get_dialect_regex_expression`'s
    `sa.func.regexp_like(...)` call) **fails for real** -- ClickHouse has no SQL function named
    `regexp_like`; the server rejects the query with
    `DB::Exception: Function with name 'regexp_like' does not exist`. ClickHouse's own regex
    function is `match(haystack, pattern)`, not `regexp_like`. An equivalent hand-written
    connection issuing the same generated SQL against this server would fail identically -- this is
    a real server-side rejection, not a harness or test-authoring problem.

    The LIKE-support branch (`ExpectColumnValuesToMatchLikePattern`) passes: ClickHouse supports
    standard SQL `LIKE`, and `get_dialect_like_pattern_expression`'s ClickHouse branch only flips a
    boolean flag rather than emitting dialect-specific SQL, so there is no equivalent naming defect
    there.
    """

    COL = "col_a"
    DATA = pd.DataFrame({COL: ["abc", "def", "ghi"]})

    def _batch_setup(self) -> ClickHouseBatchTestSetup:
        return ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.String)}
            ),
            data=self.DATA,
            extra_data={},
            context=get_context(mode="ephemeral"),
        )

    def test_match_regex(self) -> None:
        with self._batch_setup().batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToMatchRegex(column=self.COL, regex="^[a-z]{3}$")
            )
        assert result.success

    def test_not_match_regex(self) -> None:
        with self._batch_setup().batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToNotMatchRegex(column=self.COL, regex="^xyz.*")
            )
        assert result.success

    def test_match_like_pattern(self) -> None:
        with self._batch_setup().batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToMatchLikePattern(column=self.COL, like_pattern="___")
            )
        assert result.success

    def _wildcard_literal_batch_setup(self) -> ClickHouseBatchTestSetup:
        """Only `a_c` contains a literal underscore, unlike `DATA`, which holds none and so
        cannot tell a literally matched `_` from a wildcard one."""
        return ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.String)}
            ),
            data=pd.DataFrame({self.COL: ["a_c", "abc"]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )

    def test_match_like_pattern_with_escape_is_refused(self) -> None:
        """ClickHouse has no ESCAPE clause, so asking for one must fail with a usable message.

        The server rejects `LIKE 'a!_c' ESCAPE '!'` as a syntax error, so emitting the clause
        would surface a database error about SQL the user never wrote.
        """
        with self._wildcard_literal_batch_setup().batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToMatchLikePattern(
                    column=self.COL, like_pattern="a!_c", escape="!"
                )
            )
        assert not result.success
        assert "ClickHouse does not support an ESCAPE clause" in str(result.exception_info)

    def test_match_like_pattern_with_backslash_in_pattern(self) -> None:
        """A backslash inside the pattern escapes a wildcard, as the refusal above advises.

        With the backslash honored `abc` is the one unexpected value; were it taken as an
        ordinary character neither value would match, and both would be unexpected.
        """
        with self._wildcard_literal_batch_setup().batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToMatchLikePattern(column=self.COL, like_pattern="a\\_c"),
                result_format=ResultFormat.COMPLETE,
            )
        assert result.exception_info.get("raised_exception") is False, result.exception_info
        assert result.result["unexpected_list"] == ["abc"]


class TestClickHouseNullBearingData:
    """Insert real nulls into a string, an integer, a float and a date column -- each column
    carries at least one non-null value, because the shared
    type-inference path in `SQLBatchTestSetup._infer_column_types` hardcodes `INTEGER` for a
    column that is entirely null, bypassing this backend's declared type overrides. Confirms a
    null-aware expectation correctly identifies exactly the null cell in each column.

    Observed on this baseline: all four columns pass -- each declared `Nullable(...)` override
    round-trips its one null correctly, and `ExpectColumnValuesToNotBeNull` reports
    `unexpected_count == 1` for each column, matching the single null actually inserted.
    """

    STR_COL = "null_str_col"
    INT_COL = "null_int_col"
    FLOAT_COL = "null_float_col"
    DATE_COL = "null_date_col"

    DATA = pd.DataFrame(
        {
            STR_COL: ["a", None, "c", "d"],
            INT_COL: pd.array([1, None, 3, 4], dtype="Int64"),
            FLOAT_COL: [1.5, None, 3.5, 4.5],
            DATE_COL: [date(2021, 1, 1), None, date(2021, 1, 3), date(2021, 1, 4)],
        }
    )

    def test_null_cells_are_identified_per_column(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={
                    self.STR_COL: clickhouse_types.Nullable(clickhouse_types.String),
                    self.INT_COL: clickhouse_types.Nullable(clickhouse_types.Int64),
                    self.FLOAT_COL: clickhouse_types.Nullable(clickhouse_types.Float64),
                    self.DATE_COL: clickhouse_types.Nullable(clickhouse_types.Date32),
                }
            ),
            data=self.DATA,
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            suite = ExpectationSuite(
                name="clickhouse_null_bearing_data",
                expectations=[
                    gxe.ExpectColumnValuesToNotBeNull(column=self.STR_COL),
                    gxe.ExpectColumnValuesToNotBeNull(column=self.INT_COL),
                    gxe.ExpectColumnValuesToNotBeNull(column=self.FLOAT_COL),
                    gxe.ExpectColumnValuesToNotBeNull(column=self.DATE_COL),
                ],
            )
            result = batch.validate(suite, result_format=ResultFormat.COMPLETE)

        assert not result.success, "each column carries exactly one null, so each should fail"
        for expectation_result in result.results:
            config = expectation_result.expectation_config
            column_name = config.kwargs["column"] if config is not None else "<unknown column>"
            assert expectation_result.result["unexpected_count"] == 1, (
                f"expected exactly one null in {column_name}"
            )


class TestClickHouseColumnTypes:
    """One expectation per declared Python-type override in
    `_COLUMN_TYPE_OVERRIDES`, confirming each renders and round-trips correctly through a real
    expectation. Each test declares the exact SQLAlchemy type the override map declares for that
    Python type, so this is coverage of the declaration itself rather than of type inference.

    Observed on this baseline: all seven declared overrides (`str`, `int`, `float`, `bool`,
    `date`, `datetime`, `pd.Timestamp`) pass.
    """

    COL = "col"

    def test_str(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.String)}
            ),
            data=pd.DataFrame({self.COL: ["a", "b", "c", "d"]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeInSet(column=self.COL, value_set=["a", "b", "c", "d"])
            )
        assert result.success

    def test_int(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.Int64)}
            ),
            data=pd.DataFrame({self.COL: [1, 2, 3, 4]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnSumToBeBetween(column=self.COL, min_value=9, max_value=11)
            )
        assert result.success

    def test_float(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.Float64)}
            ),
            data=pd.DataFrame({self.COL: [1.5, 2.5, 3.5, 4.5]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeBetween(column=self.COL, min_value=1.0, max_value=5.0)
            )
        assert result.success

    def test_bool(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.Boolean)}
            ),
            data=pd.DataFrame({self.COL: [True, False, True, False]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeInSet(column=self.COL, value_set=[True, False])
            )
        assert result.success

    def test_date(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.Date32)}
            ),
            data=pd.DataFrame(
                {
                    self.COL: [
                        date(2021, 1, 1),
                        date(2021, 1, 2),
                        date(2021, 1, 3),
                        date(2021, 1, 4),
                    ]
                }
            ),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeBetween(
                    column=self.COL,
                    min_value=date(2020, 1, 1),
                    max_value=date(2022, 1, 1),
                )
            )
        assert result.success

    def test_datetime(self) -> None:
        # Kept as object-dtype `datetime.datetime` values (not coerced to `datetime64[ns]`/
        # `pd.Timestamp` by pandas) so this exercises the `datetime` override key specifically,
        # distinct from the `pd.Timestamp` key exercised below.
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.DateTime64())}
            ),
            data=pd.DataFrame(
                {
                    self.COL: pd.array(
                        [
                            _naive_datetime(2021, 1, 1, 1, 2, 3),
                            _naive_datetime(2021, 1, 2, 1, 2, 3),
                            _naive_datetime(2021, 1, 3, 1, 2, 3),
                            _naive_datetime(2021, 1, 4, 1, 2, 3),
                        ],
                        dtype=object,
                    )
                }
            ),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeBetween(
                    column=self.COL,
                    min_value=_naive_datetime(2020, 1, 1),
                    max_value=_naive_datetime(2022, 1, 1),
                )
            )
        assert result.success

    def test_pandas_timestamp(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(
                column_types={self.COL: clickhouse_types.Nullable(clickhouse_types.DateTime64())}
            ),
            data=pd.DataFrame(
                {
                    self.COL: [
                        pd.Timestamp("2021-01-01 01:02:03"),
                        pd.Timestamp("2021-01-02 01:02:03"),
                        pd.Timestamp("2021-01-03 01:02:03"),
                        pd.Timestamp("2021-01-04 01:02:03"),
                    ]
                }
            ),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            result = batch.validate(
                gxe.ExpectColumnValuesToBeBetween(
                    column=self.COL,
                    min_value=_naive_datetime(2020, 1, 1),
                    max_value=_naive_datetime(2022, 1, 1),
                )
            )
        assert result.success


class TestClickHouseBundledSameMetric:
    """Probe -- by observation against the running server, not by
    reading source -- whether this dialect bundles multiple metrics resolving to the same
    underlying metric name (on different columns) into one query, then assert the shape of
    community issue #10926 against whatever the probe found.

    **Probe result: bundled.** Instrumenting `SqlAlchemyExecutionEngine.execute_query` during a
    real validation run (two `ExpectColumnValuesToNotBeNull` expectations, on two different
    columns, in one `ExpectationSuite` validated in one batch) shows both metrics land in a single
    generated query:

        SELECT sum(CASE WHEN (col_b IS NULL) THEN 1 ELSE 0 END)
                   AS "column_values.nonnull.unexpected_count",
               sum(CASE WHEN (col_a IS NULL) THEN 1 ELSE 0 END)
                   AS "column_values.nonnull.unexpected_count_1"
        FROM (...)

    So the bundling path *is* entered on this backend, and this coverage is a real, satisfied
    observation rather than an unreachable probe. The aliases collide on the bare metric name
    (`column_values.nonnull.unexpected_count`) and are deduplicated with a numeric suffix
    (`_1`). **That dedup is generic, dialect-independent logic already present in
    `SqlAlchemyExecutionEngine._organize_metrics_by_domain`** (an `existing_aliases` check that
    appends `_1`, `_2`, ... on collision) -- it is not a ClickHouse-specific behavior, and it is
    not a dialect-specific random-letters label composition. The batch succeeds: ClickHouse
    accepts the double-quoted, dot-containing aliases without
    complaint. So this passes because the generic alias-dedup fix already exists upstream of this
    dialect and the dialect itself raises no objection to the resulting identifiers -- not because
    the bundling path was never entered.
    """

    COL_A = "col_a"
    COL_B = "col_b"

    def test_two_not_null_expectations_on_different_columns_share_a_metric_name(self) -> None:
        batch_setup = ClickHouseBatchTestSetup(
            config=ClickHouseDatasourceTestConfig(),
            data=pd.DataFrame({self.COL_A: ["a", "b", "c", "d"], self.COL_B: [1, 2, 3, 4]}),
            extra_data={},
            context=get_context(mode="ephemeral"),
        )
        with batch_setup.batch_test_context() as batch:
            suite = ExpectationSuite(
                name="clickhouse_bundled_same_metric",
                expectations=[
                    gxe.ExpectColumnValuesToNotBeNull(column=self.COL_A),
                    gxe.ExpectColumnValuesToNotBeNull(column=self.COL_B),
                ],
            )
            result = batch.validate(suite)

        assert result.success
