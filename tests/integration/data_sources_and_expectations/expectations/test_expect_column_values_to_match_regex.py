from typing import Sequence, cast
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.data_sources_and_expectations.data_source_lists import (
    JUST_PANDAS_DATA_SOURCES,
)
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    ExasolDatasourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SparkFilesystemCsvDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)
from tests.integration.test_utils.data_source_config.base import DataSourceTestConfig
from tests.integration.test_utils.data_source_config.sqlite import SqliteDatasourceTestConfig

SUPPORTED_SQL_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    ExasolDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]
SUPPORTED_NON_SQL_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    SparkFilesystemCsvDatasourceTestConfig()
]
ALL_SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    *SUPPORTED_SQL_DATA_SOURCES,
    *SUPPORTED_NON_SQL_DATA_SOURCES,
]

BASIC_STRINGS = "basic_strings"
COMPLEX_STRINGS = "complex_strings"
WITH_NULL = "with_null"

DATA = pd.DataFrame(
    {
        BASIC_STRINGS: ["abc", "def", "ghi"],
        COMPLEX_STRINGS: ["a1b2", "cccc", "123"],
        WITH_NULL: ["abc", None, "ghi"],
    }
)

# Kept out of DATA on purpose. `test_basic_success` and the other shared cases run over DATA for
# six other backends, and the two cases below assert an answer that is specific to Exasol's regex
# engine -- newline handling in a regex predicate is legitimately dialect-specific (POSIX regex on
# PostgreSQL and Redshift lets `.` cross a newline by default, MySQL 8's ICU engine does not), so
# these values and cases stay on their own.
NEWLINE_STRINGS = "newline_strings"

NEWLINE_DATA = pd.DataFrame({NEWLINE_STRINGS: ["a\nb", "a\rb", "axb"]})


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_SQL_DATA_SOURCES, data=DATA)
def test_basic_success(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegex(
        column=BASIC_STRINGS,
        regex="^[a-z]{3}$",
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_SQL_DATA_SOURCES, data=DATA)
def test_basic_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegex(
        column=BASIC_STRINGS,
        regex="^xyz.*",
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_postgresql_complete_results_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchRegex(
        column=BASIC_STRINGS,
        regex="^xyz.*",
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    json_dict = result.to_json_dict()
    result_dict = json_dict.get("result")

    assert isinstance(result_dict, dict)
    assert not result.success
    assert "WHERE basic_strings IS NOT NULL AND NOT (basic_strings ~ '^xyz.*')" in cast(
        "str", result_dict.get("unexpected_index_query")
    )
    assert result_dict == {
        "element_count": 3,
        "unexpected_count": 3,
        "unexpected_percent": 100.0,
        "partial_unexpected_list": ["abc", "def", "ghi"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": 100.0,
        "unexpected_percent_nonmissing": 100.0,
        "partial_unexpected_counts": [
            {"value": "abc", "count": 1},
            {"value": "def", "count": 1},
            {"value": "ghi", "count": 1},
        ],
        "unexpected_list": ["abc", "def", "ghi"],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=BASIC_STRINGS,
                regex="[a-z]*",
            ),
            id="match_any_strings",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=BASIC_STRINGS,
                regex="^[a-z]{3}$",
            ),
            id="basic_regex",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=COMPLEX_STRINGS,
                regex="^[a-z0-9]+$",
            ),
            id="alphanumeric_regex",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=WITH_NULL,
                regex="^abc$",
                mostly=0.3,
            ),
            id="mostly_with_null",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchRegex,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=BASIC_STRINGS,
                regex="^xyz.*",
            ),
            id="no_matches",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=COMPLEX_STRINGS,
                regex="^[a-z]+$",
            ),
            id="no_numbers_allowed",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchRegex(
                column=WITH_NULL,
                regex="^abc$",
                mostly=0.9,
            ),
            id="mostly_threshold_not_met",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_failure(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchRegex,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(data_source_configs=JUST_PANDAS_DATA_SOURCES, data=DATA)
def test_include_unexpected_rows_pandas(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToMatchRegex with pandas data sources."""
    expectation = gxe.ExpectColumnValuesToMatchRegex(column=COMPLEX_STRINGS, regex="^[a-z]+$")
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    # For pandas data sources, unexpected_rows should be directly usable
    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, pd.DataFrame)

    # Convert directly to DataFrame for pandas data sources
    unexpected_rows_df = unexpected_rows_data

    # Should contain 2 rows where COMPLEX_STRINGS doesn't match regex ^[a-z]+$ ("a1b2" and "123")
    assert len(unexpected_rows_df) == 2

    # The unexpected rows should have values "a1b2" and "123" in COMPLEX_STRINGS
    unexpected_values = sorted(unexpected_rows_df[COMPLEX_STRINGS].tolist())
    assert unexpected_values == ["123", "a1b2"]


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToMatchRegex with SQL data sources."""
    expectation = gxe.ExpectColumnValuesToMatchRegex(column=COMPLEX_STRINGS, regex="^[a-z]+$")
    result = batch_for_datasource.validate(
        expectation, result_format={"result_format": "BASIC", "include_unexpected_rows": True}
    )

    assert not result.success
    result_dict = result["result"]

    # Verify that unexpected_rows is present and contains the expected data
    assert "unexpected_rows" in result_dict
    assert result_dict["unexpected_rows"] is not None

    unexpected_rows_data = result_dict["unexpected_rows"]
    assert isinstance(unexpected_rows_data, list)

    # Should contain 2 rows where COMPLEX_STRINGS doesn't match regex ^[a-z]+$ ("a1b2" and "123")
    assert len(unexpected_rows_data) == 2

    # Check that both non-matching values appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "123" in unexpected_rows_str
    assert "a1b2" in unexpected_rows_str


@parameterize_batch_for_data_sources(
    data_source_configs=[SQLServerDatasourceTestConfig()],
    data=DATA,
)
def test_unsupported_dialect_states_the_reason(batch_for_datasource: Batch) -> None:
    """A dialect with no regex support must say so in the result, not fail silently.

    SQL Server has no regex predicate to compile to, so the metric raises. The reason
    reaches the user only through exception_info; a bare raise leaves it empty, which is
    indistinguishable from a regex that matched nothing or a column that does not exist.
    """
    result = batch_for_datasource.validate(
        gxe.ExpectColumnValuesToMatchRegex(column=BASIC_STRINGS, regex="^[a-z]{3}$")
    )

    assert result.success is False
    messages = [info["exception_message"] for info in result.exception_info.values()]
    assert messages, "expected the result to record an exception"
    assert all("Regex is not supported for dialect mssql" in message for message in messages), (
        f"exception_message does not state the cause: {messages!r}"
    )


@parameterize_batch_for_data_sources(
    data_source_configs=[ExasolDatasourceTestConfig()],
    data=NEWLINE_DATA,
)
def test_exasol_caller_dot_does_not_cross_a_newline(batch_for_datasource: Batch) -> None:
    """The caller's own `.` must mean on Exasol what it means to `re.search`.

    Exasol's `REGEXP_LIKE` is a whole-string predicate, so the Exasol branch of
    `get_dialect_regex_expression` wraps the caller's pattern in `.*` on both sides to make
    it the substring search the metric contracts for. Those added wildcards need DOTALL --
    and the modifier that grants it must reach *only* them, which is why the branch emits the
    scoped `(?s:.*)` behind a `(*LF)` newline verb rather than a leading global `(?s)`.

    Asserted against `ResultFormat.COMPLETE` rather than `result.success` because the exact
    `unexpected_list` separates all three candidate wrappings at once, over the values
    `["a\\nb", "a\\rb", "axb"]` with `regex="a.b"`:

    - `(*LF)(?s:.*)(?:a.b)(?s:.*)` -- correct, `["a\\nb"]`, which is what `re.search` says.
    - `(?s).*(?:a.b).*` -- a *global* `(?s)`, which runs to the end of the pattern and so
      redefines the caller's `.` too. Gives `[]`: every value matches and the expectation
      wrongly succeeds. This is the defect the review comment on the branch caught.
    - `(?s:.*)(?:a.b)(?s:.*)` -- scoped but without `(*LF)`. Gives `["a\\nb", "a\\rb"]`:
      Exasol defaults to PCRE's `ANY` newline convention, so the caller's `.` also refuses to
      cross `\\r`, which `re.search` accepts. Scoping alone is a net regression, not a partial
      fix, which is why `(*LF)` is part of the same change.

    `test_exasol_added_wildcards_still_cross_a_newline` is the other half: this case alone
    would also be satisfied by deleting the modifier altogether, which is the wrong fix.
    """
    expectation = gxe.ExpectColumnValuesToMatchRegex(column=NEWLINE_STRINGS, regex="a.b")

    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    assert not result.success
    result_dict = result["result"]
    assert result_dict["unexpected_list"] == ["a\nb"], (
        "Exasol's answer for the caller's `.` diverged from re.search -- `[]` means the DOTALL "
        "modifier is global and reached the caller's pattern, and an extra 'a\\rb' means the "
        "modifier is scoped but the `(*LF)` newline verb is missing"
    )


@parameterize_batch_for_data_sources(
    data_source_configs=[ExasolDatasourceTestConfig()],
    data=NEWLINE_DATA,
)
def test_exasol_added_wildcards_still_cross_a_newline(batch_for_datasource: Batch) -> None:
    """The wildcards the Exasol branch adds must keep crossing a newline.

    This is the guard against answering the scoping problem by deleting the modifier instead
    of scoping it. With the branch emitting `.*(?:b).*` -- no modifier at all -- the added
    wildcards stop crossing a newline, so `a\\nb` and `a\\rb` are judged unmatched and this
    case fails, even though `re.search("b")` matches all three values.

    Without this case, `test_exasol_caller_dot_does_not_cross_a_newline` would be satisfied by
    that wrong fix too: dropping the modifier also stops the caller's `.` crossing `\\n`. The
    two cases together admit only the scoped form.
    """
    expectation = gxe.ExpectColumnValuesToMatchRegex(column=NEWLINE_STRINGS, regex="b")

    result = batch_for_datasource.validate(expectation)

    assert result.success, (
        "a plain substring pattern failed to match a value containing a newline -- the `.*` "
        "wildcards the Exasol branch adds have stopped crossing one, which is what removing "
        "the DOTALL modifier does instead of scoping it"
    )
