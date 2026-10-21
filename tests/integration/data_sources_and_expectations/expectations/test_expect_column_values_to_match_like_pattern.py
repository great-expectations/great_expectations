from typing import Sequence, cast
from unittest.mock import ANY

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core.result_format import ResultFormat
from great_expectations.datasource.fluent.interfaces import Batch
from tests.integration.conftest import parameterize_batch_for_data_sources
from tests.integration.test_utils.data_source_config import (
    BigQueryDatasourceTestConfig,
    DataSourceTestConfig,
    GenericSQLDatasourceTestConfig,
    MySQLDatasourceTestConfig,
    PostgreSQLDatasourceTestConfig,
    RedshiftDatasourceTestConfig,
    SnowflakeDatasourceTestConfig,
    SqliteDatasourceTestConfig,
    SQLServerDatasourceTestConfig,
)

BASIC_PATTERNS = "basic_patterns"
PREFIXED_PATTERNS = "prefixed_patterns"
SUFFIXED_PATTERNS = "suffixed_patterns"
WITH_NULL = "with_null"
WILDCARD_LITERALS = "wildcard_literals"

DATA = pd.DataFrame(
    {
        BASIC_PATTERNS: ["abc", "def", "ghi"],
        PREFIXED_PATTERNS: ["foo_abc", "foo_def", "foo_ghi"],
        SUFFIXED_PATTERNS: ["abc_foo", "def_foo", "ghi_foo"],
        WITH_NULL: ["ba", None, "ab"],
        # Exactly one row contains a literal underscore and one a literal percent; the
        # third contains neither. Both are LIKE wildcards, so telling them apart is only
        # possible with an escape character.
        WILDCARD_LITERALS: ["a_b", "axb", "a%b"],
    }
)

SUPPORTED_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    BigQueryDatasourceTestConfig(),
    SQLServerDatasourceTestConfig(),
    MySQLDatasourceTestConfig(),
    PostgreSQLDatasourceTestConfig(),
    RedshiftDatasourceTestConfig(),
    GenericSQLDatasourceTestConfig(),
    SnowflakeDatasourceTestConfig(),
    SqliteDatasourceTestConfig(),
]


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_basic_success(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=PREFIXED_PATTERNS,
        like_pattern="foo%",
    )
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(data_source_configs=SUPPORTED_DATA_SOURCES, data=DATA)
def test_basic_failure(batch_for_datasource: Batch) -> None:
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=BASIC_PATTERNS,
        like_pattern="xyz%",
    )
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_complete_results_failure(batch_for_datasource: Batch) -> None:
    ABOUT_TWO_THIRDS = pytest.approx(2 / 3 * 100)
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=BASIC_PATTERNS,
        like_pattern="%b%",
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)
    json_dict = result.to_json_dict()
    result_dict = json_dict.get("result")

    assert isinstance(result_dict, dict)
    assert not result.success
    assert "IS NOT NULL AND basic_patterns NOT LIKE '%b%'" in cast(
        "str", result_dict.get("unexpected_index_query")
    )
    assert result.to_json_dict().get("result") == {
        "element_count": 3,
        "unexpected_count": 2,
        "unexpected_percent": ABOUT_TWO_THIRDS,
        "partial_unexpected_list": ["def", "ghi"],
        "missing_count": 0,
        "missing_percent": 0.0,
        "unexpected_percent_total": ABOUT_TWO_THIRDS,
        "unexpected_percent_nonmissing": ABOUT_TWO_THIRDS,
        "partial_unexpected_counts": [
            {"value": "def", "count": 1},
            {"value": "ghi", "count": 1},
        ],
        "unexpected_list": ["def", "ghi"],
        "unexpected_index_query": ANY,
    }


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=BASIC_PATTERNS,
                like_pattern="%",
            ),
            id="match_all",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=PREFIXED_PATTERNS,
                like_pattern="foo%",
            ),
            id="prefixed_pattern",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=SUFFIXED_PATTERNS,
                like_pattern="%foo",
            ),
            id="suffixed_pattern",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=BASIC_PATTERNS, like_pattern="%b%", mostly=0.3
            ),
            id="mostly",
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig(), RedshiftDatasourceTestConfig()],
    data=DATA,
)
def test_success(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchLikePattern,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=BASIC_PATTERNS,
                like_pattern="%xyz%",
            ),
            id="no_matches",
        ),
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=BASIC_PATTERNS,
                like_pattern="%b%",
                mostly=0.4,
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
    expectation: gxe.ExpectColumnValuesToMatchLikePattern,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert not result.success


@pytest.mark.parametrize(
    "expectation",
    [
        pytest.param(
            gxe.ExpectColumnValuesToMatchLikePattern(
                column=BASIC_PATTERNS,
                like_pattern="[adg]%",
            ),
        ),
    ],
)
@parameterize_batch_for_data_sources(
    data_source_configs=[SQLServerDatasourceTestConfig()], data=DATA
)
def test_msql_fancy_syntax(
    batch_for_datasource: Batch,
    expectation: gxe.ExpectColumnValuesToMatchLikePattern,
) -> None:
    result = batch_for_datasource.validate(expectation)
    assert result.success


@parameterize_batch_for_data_sources(
    data_source_configs=[PostgreSQLDatasourceTestConfig()], data=DATA
)
def test_include_unexpected_rows_sql(batch_for_datasource: Batch) -> None:
    """Test include_unexpected_rows for ExpectColumnValuesToMatchLikePattern with SQL."""
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=BASIC_PATTERNS, like_pattern="%b%"
    )
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

    # Should contain 2 rows where BASIC_PATTERNS doesn't match pattern %b%
    assert len(unexpected_rows_data) == 2

    # Check that "def" and "ghi" appear in the unexpected rows data
    unexpected_rows_str = str(unexpected_rows_data)
    assert "def" in unexpected_rows_str
    assert "ghi" in unexpected_rows_str


# BigQuery is excluded: GoogleSQL has no ESCAPE clause, which is asserted separately as a
# unit test over the dialect helper.
ESCAPE_DATA_SOURCES: Sequence[DataSourceTestConfig] = [
    config
    for config in SUPPORTED_DATA_SOURCES
    if not isinstance(config, BigQueryDatasourceTestConfig)
]


@parameterize_batch_for_data_sources(data_source_configs=ESCAPE_DATA_SOURCES, data=DATA)
def test_unescaped_wildcards_still_match_anything(batch_for_datasource: Batch) -> None:
    """Baseline: without an escape, '_' matches any character, so all three rows match.

    This is the behavior that makes a literal underscore impossible to express, and it is
    unchanged by adding the escape parameter.
    """
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=WILDCARD_LITERALS, like_pattern="a_b"
    )
    assert batch_for_datasource.validate(expectation).success


@pytest.mark.parametrize(
    ("like_pattern", "escape", "expected_unexpected"),
    [
        pytest.param("a!_b", "!", ["a%b", "axb"], id="literal_underscore"),
        pytest.param("a!%b", "!", ["a_b", "axb"], id="literal_percent"),
        pytest.param("a#_b", "#", ["a%b", "axb"], id="a_different_escape_character"),
    ],
)
@parameterize_batch_for_data_sources(data_source_configs=ESCAPE_DATA_SOURCES, data=DATA)
def test_escape_matches_wildcard_characters_literally(
    batch_for_datasource: Batch,
    like_pattern: str,
    escape: str,
    expected_unexpected: list[str],
) -> None:
    """An escape character makes '_' and '%' match themselves rather than any character.

    Only the single row holding the literal character may match; the other two must be
    reported as unexpected. Which character does the escaping is the caller's choice, so
    two different ones must produce the same result.

    Backslash is deliberately not one of them. Several dialects also treat it specially
    inside string literals, before LIKE ever sees the pattern -- Redshift rejects
    ``ESCAPE '\\'`` outright -- which is the reason this parameter lets callers pick a
    character that does not collide with their dialect or their data.
    """
    expectation = gxe.ExpectColumnValuesToMatchLikePattern(
        column=WILDCARD_LITERALS, like_pattern=like_pattern, escape=escape
    )
    result = batch_for_datasource.validate(expectation, result_format=ResultFormat.COMPLETE)

    # A metric that raised leaves `result` empty, so read the failure out of
    # exception_info rather than reporting a bare KeyError three frames later.
    assert "unexpected_list" in result.result, f"metric did not resolve: {result.exception_info}"
    assert not result.success
    assert sorted(result.result["unexpected_list"]) == expected_unexpected
